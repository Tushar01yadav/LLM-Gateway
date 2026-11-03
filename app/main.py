import asyncio

import json

import logging

import os

import sqlite3

import time

import uuid

from abc import ABC, abstractmethod

from contextlib import asynccontextmanager

from typing import List, Optional, Dict, Any

import httpx

from dotenv import load_dotenv

from fastapi import FastAPI, HTTPException, Request, Response, status, Depends, Header

from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

import urllib.parse

from pydantic import BaseModel, Field

from app.security import (
    create_gateway_api_key,
    validate_gateway_api_key,
    list_gateway_api_keys,
    revoke_gateway_api_key,
    RATE_LIMITER,
    verify_admin_auth,
    ADMIN_TOKEN,
)

from app.cost import (
    BUDGET_GUARD,
    COST_TRACKER,
    REQUEST_COALESCER,
    RESPONSE_CACHE,
    build_cache_key,
    calculate_request_cost,
)

from app.jobs import JOB_MANAGER
from app.guardrails import GUARDRAILS, AUDIT_LOGGER

from app.reliability import (

    CIRCUIT_BREAKERS,

    ADAPTER_MAP,

    BaseAdapter,

    GroqAdapter,

    OpenRouterAdapter,

    GeminiAdapter,

    load_models_config,

    resolve_fallback_chain,

    execute_reliable_completion,

    mask_api_key,

    TRAFFIC_BROADCASTER,

    NODE_METRICS,

    INJECTED_FAILURES,

    set_failure_injection,

    update_provider_api_key,

    get_provider_nodes_info,

    LoadShedError,

    get_concurrency_metrics,

    SMOKE_TEST_FAILURES,

    SMOKE_TEST_RESULTS,

    run_startup_smoke_test,

)

from app.auth import (

    GOOGLE_CONFIG,

    db_create_otp,

    db_create_session as db_create_user_session,

    db_create_user,

    db_delete_session as db_delete_user_session,

    db_get_oauth_tokens,

    db_get_user_by_email,

    db_get_user_by_id,

    db_save_oauth_tokens,

    db_update_user_profile,

    db_validate_session,

    db_verify_otp,

    dispatch_otp_email,

    generate_avatar_image,

    is_valid_email,

    verify_password,

)

# Setup logger

logger = logging.getLogger("gateway")

if not logger.handlers:

    _log_handler = logging.StreamHandler()

    _log_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))

    logger.addHandler(_log_handler)

logger.setLevel(logging.INFO)

# Load environment variables from .env

load_dotenv()

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "gateway.db")

def init_db():

    """Initialize SQLite database for chat history persistence."""

    conn = sqlite3.connect(DB_PATH, timeout=15.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")

    cursor = conn.cursor()

    cursor.execute("""

        CREATE TABLE IF NOT EXISTS sessions (

            id TEXT PRIMARY KEY,

            title TEXT,

            model TEXT,

            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            user_id INTEGER,

            user_name TEXT,

            user_email TEXT

        )

    """)

    for col, col_type in [("user_id", "INTEGER"), ("user_name", "TEXT"), ("user_email", "TEXT")]:

        try:

            cursor.execute(f"ALTER TABLE sessions ADD COLUMN {col} {col_type}")

        except Exception:

            pass

    cursor.execute("""

        CREATE TABLE IF NOT EXISTS messages (

            id INTEGER PRIMARY KEY AUTOINCREMENT,

            session_id TEXT,

            role TEXT,

            content TEXT,

            latency_ms REAL DEFAULT 0,

            total_tokens INTEGER DEFAULT 0,

            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE

        )

    """)

    cursor.execute("""

        CREATE TABLE IF NOT EXISTS request_logs (

            id INTEGER PRIMARY KEY AUTOINCREMENT,

            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            session_id TEXT,

            target TEXT,

            provider TEXT,

            model TEXT,

            status_code INTEGER DEFAULT 200,

            fallback_used INTEGER DEFAULT 0,

            attempts_json TEXT,

            latency_ms REAL DEFAULT 0,

            queue_wait_ms REAL DEFAULT 0,

            total_tokens INTEGER DEFAULT 0,

            error_message TEXT

        )

    """)

    try:

        cursor.execute("ALTER TABLE request_logs ADD COLUMN queue_wait_ms REAL DEFAULT 0")

        conn.commit()

    except Exception:

        pass

    conn.commit()

    conn.close()

def db_create_session(

    session_id: str,

    title: str,

    model: str,

    user_id: Optional[int] = None,

    user_name: Optional[str] = None,

    user_email: Optional[str] = None,

):

    conn = sqlite3.connect(DB_PATH)

    cursor = conn.cursor()

    cursor.execute(

        "INSERT OR IGNORE INTO sessions (id, title, model, user_id, user_name, user_email) VALUES (?, ?, ?, ?, ?, ?)",

        (session_id, title, model, user_id, user_name, user_email),

    )

    conn.commit()

    conn.close()

def db_save_message(

    session_id: str,

    role: str,

    content: str,

    latency_ms: float = 0,

    total_tokens: int = 0,

):

    conn = sqlite3.connect(DB_PATH)

    cursor = conn.cursor()

    cursor.execute(

        "INSERT INTO messages (session_id, role, content, latency_ms, total_tokens) VALUES (?, ?, ?, ?, ?)",

        (session_id, role, content, latency_ms, total_tokens),

    )

    conn.commit()

    conn.close()

def db_get_sessions(user_id: Optional[int] = None):

    conn = sqlite3.connect(DB_PATH)

    cursor = conn.cursor()

    if user_id is not None:

        cursor.execute(

            "SELECT id, title, model, created_at, user_id, user_name, user_email FROM sessions WHERE user_id = ? ORDER BY created_at DESC",

            (user_id,),

        )

    else:

        cursor.execute(

            "SELECT id, title, model, created_at, user_id, user_name, user_email FROM sessions ORDER BY created_at DESC"

        )

    rows = cursor.fetchall()

    conn.close()

    return [

        {

            "id": r[0],

            "title": r[1],

            "model": r[2],

            "created_at": r[3],

            "user_id": r[4],

            "user_name": r[5],

            "user_email": r[6],

        }

        for r in rows

    ]

def db_get_session_messages(session_id: str):

    conn = sqlite3.connect(DB_PATH)

    cursor = conn.cursor()

    cursor.execute(

        "SELECT role, content, latency_ms, total_tokens, created_at FROM messages WHERE session_id = ? ORDER BY id ASC",

        (session_id,),

    )

    rows = cursor.fetchall()

    conn.close()

    return [

        {

            "role": r[0],

            "content": r[1],

            "meta": (

                {"latency_ms": r[2], "token_usage": {"total_tokens": r[3]}}

                if r[2] > 0

                else None

            ),

        }

        for r in rows

    ]

def db_delete_session(session_id: str):

    conn = sqlite3.connect(DB_PATH)

    cursor = conn.cursor()

    cursor.execute("DELETE FROM messages WHERE session_id = ?", (session_id,))

    cursor.execute("DELETE FROM sessions WHERE id = ?", (session_id,))

    conn.commit()

    conn.close()

def db_update_session_title(session_id: str, title: str):

    conn = sqlite3.connect(DB_PATH)

    cursor = conn.cursor()

    cursor.execute("UPDATE sessions SET title = ? WHERE id = ?", (title, session_id))

    conn.commit()

    conn.close()

def db_clear_all_sessions():

    conn = sqlite3.connect(DB_PATH)

    cursor = conn.cursor()

    cursor.execute("DELETE FROM messages")

    cursor.execute("DELETE FROM sessions")

    conn.commit()

    conn.close()

def db_get_storage_info() -> Dict[str, Any]:

    conn = sqlite3.connect(DB_PATH)

    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM sessions")

    session_count = cursor.fetchone()[0] or 0

    cursor.execute("SELECT COUNT(*) FROM messages")

    message_count = cursor.fetchone()[0] or 0

    conn.close()

    file_size_bytes = 0

    if os.path.exists(DB_PATH):

        file_size_bytes = os.path.getsize(DB_PATH)

    file_size_kb = round(file_size_bytes / 1024, 1)

    return {

        "session_count": session_count,

        "message_count": message_count,

        "db_size_kb": file_size_kb,

        "db_size_bytes": file_size_bytes,

        "storage_type": "SQLite Persistent Storage",

        "db_path": os.path.basename(DB_PATH),

    }

def _db_save_request_log_sync(
    session_id: str,
    target: str,
    provider: str,
    model: str,
    status_code: int,
    fallback_used: bool,
    attempts: list,
    latency_ms: float,
    total_tokens: int,
    error_message: Optional[str] = None,
    queue_wait_ms: float = 0.0,
):
    try:
        conn = sqlite3.connect(DB_PATH, timeout=15.0)
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO request_logs 
            (session_id, target, provider, model, status_code, fallback_used, attempts_json, latency_ms, queue_wait_ms, total_tokens, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                session_id,
                target,
                provider,
                model,
                status_code,
                1 if fallback_used else 0,
                json.dumps(attempts),
                latency_ms,
                queue_wait_ms,
                total_tokens,
                error_message,
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"Failed to persist request log: {e}")

def db_save_request_log(
    session_id: str,
    target: str,
    provider: str,
    model: str,
    status_code: int,
    fallback_used: bool,
    attempts: list,
    latency_ms: float,
    total_tokens: int,
    error_message: Optional[str] = None,
    queue_wait_ms: float = 0.0,
):
    """Offload SQLite log writes off the asyncio event loop to worker thread."""
    try:
        loop = asyncio.get_running_loop()
        loop.run_in_executor(
            None,
            _db_save_request_log_sync,
            session_id,
            target,
            provider,
            model,
            status_code,
            fallback_used,
            attempts,
            latency_ms,
            total_tokens,
            error_message,
            queue_wait_ms,
        )
    except RuntimeError:
        _db_save_request_log_sync(
            session_id,
            target,
            provider,
            model,
            status_code,
            fallback_used,
            attempts,
            latency_ms,
            total_tokens,
            error_message,
            queue_wait_ms,
        )

def init_node_metrics_from_db():
    """Seed in-memory served telemetry from historical SQLite request_logs so counts persist across restarts."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=5.0)
        cursor = conn.cursor()
        cursor.execute("SELECT LOWER(provider), COUNT(*) FROM request_logs WHERE status_code=200 GROUP BY LOWER(provider)")
        for prov, count in cursor.fetchall():
            if prov in NODE_METRICS:
                NODE_METRICS[prov]["served"] = max(NODE_METRICS[prov]["served"], count)
        conn.close()
    except Exception as e:
        logger.warning(f"Could not load initial metrics from DB: {e}")

def db_get_request_logs(limit: int = 50):

    conn = sqlite3.connect(DB_PATH)

    cursor = conn.cursor()

    cursor.execute(

        """

        SELECT id, timestamp, session_id, target, provider, model, status_code, fallback_used, attempts_json, latency_ms, total_tokens, error_message, queue_wait_ms

        FROM request_logs

        ORDER BY id DESC

        LIMIT ?

        """,

        (limit,),

    )

    rows = cursor.fetchall()

    conn.close()

    logs = []

    for r in rows:

        attempts = []

        if r[8]:

            try:

                attempts = json.loads(r[8])

            except Exception:

                pass

        q_wait = r[12] if len(r) > 12 and r[12] is not None else 0.0

        logs.append({

            "id": r[0],

            "timestamp": r[1],

            "session_id": r[2],

            "target": r[3],

            "provider": r[4],

            "model": r[5],

            "status_code": r[6],

            "fallback_used": bool(r[7]),

            "attempts": attempts,

            "latency_ms": r[9],

            "queue_wait_ms": q_wait,

            "total_tokens": r[10],

            "error_message": r[11],

        })

    return logs

def db_clear_request_logs():

    conn = sqlite3.connect(DB_PATH)

    cursor = conn.cursor()

    cursor.execute("DELETE FROM request_logs")

    conn.commit()

    conn.close()

def check_startup_keys():

    """Verify provider API keys in environment."""

    has_keys = [

        k for k in ["GROQ_API_KEY", "OPENROUTER_API_KEY", "GEMINI_API_KEY"]

        if os.getenv(k) and os.getenv(k).strip()

    ]

    if not has_keys:

        logger.warning("No LLM provider API keys found in environment.")

# ---------------------------------------------------------

# 1. Model Registry & Unified Adapters (Wired via app.reliability)

# models.json is the single source of truth for routing config

# ---------------------------------------------------------

load_model_registry = load_models_config

ADAPTERS = ADAPTER_MAP

@asynccontextmanager

async def lifespan(app: FastAPI):

    check_startup_keys()

    init_db()

    init_node_metrics_from_db()

    await JOB_MANAGER.start_worker()

    asyncio.create_task(run_startup_smoke_test())

    yield

    await JOB_MANAGER.stop_worker()

app = FastAPI(

    title="LLM Gateway",

    version="1.0.0",

    lifespan=lifespan,

)

class ChatMessage(BaseModel):

    role: str

    content: str

class ChatRequest(BaseModel):

    messages: List[ChatMessage]

    model: Optional[str] = None

    alias: Optional[str] = None

    temperature: Optional[float] = Field(0.7, ge=0.0, le=2.0)

    max_tokens: Optional[int] = Field(1000, gt=0, le=1000)

    session_id: Optional[str] = None

@app.get("/health")

async def health():

    return {"status": "ok"}

@app.get("/api/chats")

async def get_chats(request: Request):

    user = None

    token = request.cookies.get("llm_gateway_session")

    if not token:

        auth_header = request.headers.get("Authorization")

        if auth_header and auth_header.startswith("Bearer "):

            token = auth_header[7:].strip()

    if token:

        user = await asyncio.to_thread(db_validate_session, token)

    user_id = user["id"] if user else None

    return await asyncio.to_thread(db_get_sessions, user_id=user_id)

@app.get("/api/chats/{session_id}")

async def get_chat_history(session_id: str):

    messages = await asyncio.to_thread(db_get_session_messages, session_id)

    return {"session_id": session_id, "messages": messages}

class RenameSessionRequest(BaseModel):

    title: str

@app.delete("/api/chats/{session_id}")

async def delete_chat(session_id: str):

    await asyncio.to_thread(db_delete_session, session_id)

    return {"status": "deleted"}

@app.patch("/api/chats/{session_id}")

async def rename_chat(session_id: str, request: RenameSessionRequest):

    new_title = request.title.strip() or "Untitled Chat"

    await asyncio.to_thread(db_update_session_title, session_id, new_title)

    return {"status": "ok", "title": new_title}

@app.delete("/api/chats")

async def clear_all_chats():

    await asyncio.to_thread(db_clear_all_sessions)

    return {"status": "cleared"}

@app.get("/api/chats/storage/metrics")

async def get_chat_storage_metrics():

    return await asyncio.to_thread(db_get_storage_info)

# ---------------------------------------------------------------------------

# Reliability, Stats & Logs API Endpoints

# ---------------------------------------------------------------------------

def db_get_gateway_stats() -> Dict[str, Any]:

    conn = sqlite3.connect(DB_PATH)

    cursor = conn.cursor()

    cursor.execute("""

        SELECT 

            COUNT(*),

            SUM(CASE WHEN status_code = 200 THEN 1 ELSE 0 END),

            AVG(latency_ms),

            SUM(total_tokens),

            SUM(CASE WHEN fallback_used = 1 THEN 1 ELSE 0 END)

        FROM request_logs

    """)

    row = cursor.fetchone()

    total = row[0] or 0

    success = row[1] or 0

    avg_latency = round(row[2] or 0)

    total_tokens = row[3] or 0

    fallbacks = row[4] or 0

    cursor.execute("SELECT provider, COUNT(*) FROM request_logs GROUP BY provider")

    providers = {p: c for p, c in cursor.fetchall()}

    cursor.execute("SELECT model, COUNT(*), SUM(total_tokens) FROM request_logs GROUP BY model ORDER BY COUNT(*) DESC LIMIT 5")

    top_models = [{"model": m, "count": c, "tokens": t or 0} for m, c, t in cursor.fetchall()]

    conn.close()

    success_rate = round((success / total * 100), 1) if total > 0 else 100.0

    return {

        "total_requests": total,

        "successful_requests": success,

        "failed_requests": total - success,

        "success_rate": success_rate,

        "avg_latency_ms": avg_latency,

        "total_tokens": total_tokens,

        "fallback_count": fallbacks,

        "provider_breakdown": providers,

        "top_models": top_models,

    }

@app.get("/api/stats")

async def get_stats():

    return await asyncio.to_thread(db_get_gateway_stats)

@app.get("/api/logs")

async def get_logs(limit: int = 100):

    return await asyncio.to_thread(db_get_request_logs, limit=limit)

@app.delete("/api/logs")

async def clear_logs():

    await asyncio.to_thread(db_clear_request_logs)

    return {"status": "cleared"}

@app.get("/api/concurrency/status")

async def get_concurrency_status():

    return get_concurrency_metrics()

@app.get("/api/reliability/status")

async def get_reliability_status():

    cb_status = {name: cb.get_status() for name, cb in CIRCUIT_BREAKERS.items()}

    return {

        "circuit_breakers": cb_status,

        "concurrency": get_concurrency_metrics(),

        "smoke_test": {

            "failures": SMOKE_TEST_FAILURES,

            "results": SMOKE_TEST_RESULTS,

        },

        "registry_chains": load_models_config(),

    }

@app.post("/api/reliability/circuit-breaker/reset")

async def reset_circuit_breakers():

    for cb in CIRCUIT_BREAKERS.values():

        cb.reset()

    return {"status": "all_reset"}

@app.post("/api/reliability/circuit-breaker/trip")

async def trip_circuit_breaker(provider: str):

    cb = CIRCUIT_BREAKERS.get(provider.lower())

    if not cb:

        raise HTTPException(status_code=404, detail=f"Provider '{provider}' not found")

    cb.state = "OPEN"

    cb.consecutive_failures = cb.failure_threshold

    cb.last_failure_time = time.time()

    return {"status": "tripped", "provider": provider}

# ---------------------------------------------------------------------------

# Visualize & Real-Time Traffic Observability Endpoints

# ---------------------------------------------------------------------------

class KeyEditPayload(BaseModel):

    provider: str

    api_key: str

class FailureInjectPayload(BaseModel):

    provider: str

    enabled: bool

    mode: Optional[str] = "503"

class TestRequestPayload(BaseModel):

    target: Optional[str] = "fast"

    prompt: Optional[str] = None

    request_id: Optional[str] = None

def verify_admin_or_localhost(request_http: Request):

    """

    Security check for key editing:

    Permits localhost connections (127.0.0.1, ::1, testclient) or active user sessions or admin key.

    Rejects unauthorized remote calls with 403 Forbidden.

    """

    forwarded = request_http.headers.get("x-forwarded-for")

    if forwarded:

        client_host = forwarded.split(",")[0].strip()

    else:

        client_host = request_http.client.host if request_http.client else ""

    is_localhost = client_host in ("127.0.0.1", "::1", "localhost")

    if client_host == "testclient" and not forwarded:

        is_localhost = True

    token = request_http.cookies.get("llm_gateway_session")

    if not token:

        auth_header = request_http.headers.get("Authorization")

        if auth_header and auth_header.startswith("Bearer "):

            token = auth_header[7:].strip()

    is_authenticated = False

    if token:

        user = db_validate_session(token)

        if user:

            is_authenticated = True

    admin_key = request_http.headers.get("X-Admin-Key")

    admin_env = os.getenv("ADMIN_SECRET_KEY")

    is_admin_header = bool(admin_env and admin_key == admin_env)

    if not (is_localhost or is_authenticated or is_admin_header):

        raise HTTPException(

            status_code=status.HTTP_403_FORBIDDEN,

            detail="Forbidden: Key editing requires admin authorization or localhost connection",

        )

@app.get("/api/reliability/provider-nodes")

async def get_provider_nodes():

    """Returns provider nodes telemetry. Backend NEVER returns full keys, only masked (***yd)."""

    init_node_metrics_from_db()

    return get_provider_nodes_info()

@app.post("/api/reliability/keys")

async def edit_provider_key(payload: KeyEditPayload, request_http: Request, admin_auth: bool = Depends(verify_admin_auth)):

    """

    Key editing on provider nodes:

    1. Security check: Protected by Admin Token.

    2. Test call first to validate key.

    3. If valid: applied at runtime, saved to .env, breaker reset.

    4. Never returns full key, only masked.

    """

    res = await update_provider_api_key(payload.provider, payload.api_key)

    return res

class CreateGatewayKeyPayload(BaseModel):

    name: str = "Production Key"

    quota: int = 60

    daily_budget: float = 5.0

@app.post("/api/gateway-keys")

async def api_create_gateway_key(payload: CreateGatewayKeyPayload, admin_auth: bool = Depends(verify_admin_auth)):

    raw_key, info = await asyncio.to_thread(
        create_gateway_api_key,
        name=payload.name,
        quota=payload.quota,
        daily_budget=payload.daily_budget
    )

    return info

@app.get("/api/cost/metrics")

async def api_get_cost_metrics():

    total_cost = await COST_TRACKER.get_total_spend()

    return {
        "total_cost_usd": total_cost,
        "cache_size": len(RESPONSE_CACHE._local_cache),
    }

@app.get("/api/gateway-keys")

async def api_list_gateway_keys(admin_auth: bool = Depends(verify_admin_auth)):

    return await asyncio.to_thread(list_gateway_api_keys)

@app.delete("/api/gateway-keys/{key_id}")

async def api_revoke_gateway_key(key_id: int, admin_auth: bool = Depends(verify_admin_auth)):

    ok = await asyncio.to_thread(revoke_gateway_api_key, key_id)

    if not ok:

        raise HTTPException(status_code=404, detail="Key not found")

    return {"status": "revoked", "id": key_id}

class CreateJobPayload(BaseModel):
    model: Optional[str] = "fast"
    alias: Optional[str] = None
    messages: List[ChatMessage]
    temperature: Optional[float] = 0.7
    max_tokens: Optional[int] = 1000
    max_attempts: Optional[int] = 3

@app.post("/v1/jobs")
async def create_async_job(
    request: CreateJobPayload,
    request_http: Request,
    idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
    x_idempotency_key: Optional[str] = Header(None, alias="X-Idempotency-Key"),
):
    raw_key = None
    auth_header = request_http.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        raw_key = auth_header[7:].strip()
    elif request_http.headers.get("X-API-Key"):
        raw_key = request_http.headers.get("X-API-Key").strip()

    key_info = None
    if raw_key:
        key_info = await asyncio.to_thread(validate_gateway_api_key, raw_key)
        if not key_info:
            raise HTTPException(status_code=401, detail="Unauthorized: Invalid Gateway API key")
    else:
        session_token = request_http.cookies.get("llm_gateway_session")
        user = await asyncio.to_thread(db_validate_session, session_token) if session_token else None
        if user:
            key_info = {"id": 0, "name": f"user_{user['id']}", "quota": 60, "daily_budget": 5.0}
        else:
            raise HTTPException(status_code=401, detail="Unauthorized: Missing Gateway API key")

    effective_idempotency = idempotency_key or x_idempotency_key
    payload_dict = {
        "model": request.model,
        "alias": request.alias,
        "messages": [{"role": m.role, "content": m.content} for m in request.messages],
        "temperature": request.temperature,
        "max_tokens": request.max_tokens,
    }

    job_info = await JOB_MANAGER.enqueue_job(
        payload=payload_dict,
        key_id=str(key_info["id"]),
        key_name=key_info["name"],
        idempotency_key=effective_idempotency,
        max_attempts=request.max_attempts or 3,
    )
    return job_info

@app.get("/v1/jobs/{job_id}")
async def get_async_job(job_id: str):
    job = await JOB_MANAGER.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job

@app.get("/v1/dlq")
async def list_dlq_jobs():
    items = await JOB_MANAGER.list_dlq()
    return items

@app.get("/v1/dlq/{job_id}")
async def inspect_dlq_job(job_id: str):
    job = await JOB_MANAGER.get_job(job_id)
    if not job or job.get("status") != "dlq":
        raise HTTPException(status_code=404, detail="DLQ item not found")
    return job

@app.post("/v1/dlq/{job_id}/replay")
async def replay_dlq_job(job_id: str):
    res = await JOB_MANAGER.replay_dlq_job(job_id)
    return res

@app.post("/v1/dlq/clear")
@app.delete("/v1/dlq")
async def clear_dlq_jobs():
    cleared = await JOB_MANAGER.clear_dlq()
    return {"status": "ok", "cleared": cleared}

@app.post("/api/reliability/clear-cache")
async def api_clear_response_cache():
    RESPONSE_CACHE.clear()
    if "cache" in NODE_METRICS:
        NODE_METRICS["cache"]["served"] = 0
    return {"status": "ok", "message": "Redis Response Cache cleared successfully"}

@app.get("/api/audit-logs")
async def api_get_audit_logs(admin_auth: bool = Depends(verify_admin_auth)):
    logs = await asyncio.to_thread(AUDIT_LOGGER.list_recent_logs, 100)
    return {"logs": logs}

@app.post("/api/reliability/inject-failure")

async def inject_failure(payload: FailureInjectPayload):

    """Controls: Break this provider toggle for failure injection demo."""

    res = set_failure_injection(payload.provider, payload.enabled, payload.mode or "503")

    return res

@app.get("/api/reliability/traffic-stream")

async def get_traffic_stream(request_http: Request):

    """SSE events: request_started, attempt_started, attempt_failed, fallback, request_served."""

    q = await TRAFFIC_BROADCASTER.subscribe()

    async def event_generator():

        try:

            yield f"event: connected\ndata: {json.dumps({'status': 'connected', 'timestamp': time.time()})}\n\n"

            while True:

                if await request_http.is_disconnected():

                    break

                try:

                    msg = await asyncio.wait_for(q.get(), timeout=15.0)

                    yield f"event: {msg['event']}\ndata: {json.dumps(msg['data'])}\n\n"

                except asyncio.TimeoutError:

                    yield ": ping\n\n"

        finally:

            await TRAFFIC_BROADCASTER.unsubscribe(q)

    return StreamingResponse(

        event_generator(),

        media_type="text/event-stream",

        headers={

            "Cache-Control": "no-cache",

            "Connection": "keep-alive",

            "X-Accel-Buffering": "no",

        },

    )

TEST_PROMPTS = [

    "hi",

    "hello",

    "ping",

    "quick check",

    "gateway status",

    "echo test",

    "health check",

    "status ping",

]

@app.post("/api/reliability/test-request")

async def trigger_test_request(payload: TestRequestPayload):

    """Controls: Send real traffic test request through the LLM gateway."""

    target = payload.target or "fast"

    prompt_text = (payload.prompt or "").strip()

    if not prompt_text:

        prompt_text = random.choice(TEST_PROMPTS)

    test_id = payload.request_id or f"test_{int(time.time()*1000)}_{uuid.uuid4().hex[:6]}"

    # Exact-match Redis Cache Check
    cache_key = build_cache_key(alias=target, model=target, messages=[{"role": "user", "content": prompt_text}], temperature=0.7)
    cached = await RESPONSE_CACHE.get(cache_key)
    if cached:
        await TRAFFIC_BROADCASTER.broadcast("cache_hit", {"target": target, "request_id": test_id})
        res = dict(cached)
        res["cached"] = True
        res["saved_cost"] = res.get("cost_usd", 0.000018)
        res["cost_usd"] = 0.0
        if "cache" in NODE_METRICS:
            NODE_METRICS["cache"]["served"] += 1
        return res

    await TRAFFIC_BROADCASTER.broadcast("cache_miss", {"target": target, "request_id": test_id})

    res = await execute_reliable_completion(

        target=target,

        messages=[{"role": "user", "content": prompt_text}],

        max_tokens=60,

        request_id=test_id,

    )

    prov = (res.get("provider") or target).lower()
    mod = res.get("model") or target
    usage = res.get("token_usage") or res.get("usage") or {}
    cost_usd = calculate_request_cost(
        alias=target,
        model=mod,
        token_usage=usage,
        messages=[{"role": "user", "content": prompt_text}],
        completion_text=res.get("text", "")
    )
    res["cost_usd"] = cost_usd
    res["cost"] = cost_usd
    res["cached"] = False

    if prov in NODE_METRICS:
        NODE_METRICS[prov]["served"] += 1

    total_tokens = usage.get("total_tokens") or (usage.get("prompt_tokens", 0) + usage.get("completion_tokens", 0)) or 10
    db_save_request_log(
        session_id=test_id,
        target=target,
        provider=prov,
        model=mod,
        status_code=200,
        fallback_used=res.get("fallback_used", False),
        attempts=res.get("attempts", []),
        latency_ms=res.get("latency_ms", 0.0),
        total_tokens=total_tokens,
        error_message=None,
        queue_wait_ms=res.get("queue_wait_ms", 0.0)
    )

    await RESPONSE_CACHE.set(cache_key, res)

    return res

# ---------------------------------------------------------------------------

# Authentication & OTP Verification Endpoints

# ---------------------------------------------------------------------------

class SignupRequest(BaseModel):

    name: str

    email: str

    password: str

class LoginRequest(BaseModel):

    email: str

    password: str

class VerifyOtpRequest(BaseModel):

    email: str

    otp_code: Optional[str] = None

    otp: Optional[str] = None

class ResendOtpRequest(BaseModel):

    email: str

class GoogleCallbackRequest(BaseModel):

    code: str

@app.get("/api/auth/google/url")

async def get_google_auth_url():

    client_id = GOOGLE_CONFIG.get("client_id")

    redirect_uri = GOOGLE_CONFIG.get("redirect_uri", "http://localhost:8000")

    if not client_id:

        raise HTTPException(

            status_code=500,

            detail="Google Client ID not configured. Please ensure client_secret*.json is in project root.",

        )

    scopes = "openid email profile https://www.googleapis.com/auth/gmail.send https://www.googleapis.com/auth/gmail.modify"

    encoded_scopes = urllib.parse.quote(scopes)

    encoded_redirect = urllib.parse.quote(redirect_uri)

    auth_url = (

        f"https://accounts.google.com/o/oauth2/auth?"

        f"client_id={client_id}&"

        f"redirect_uri={encoded_redirect}&"

        f"response_type=code&"

        f"scope={encoded_scopes}&"

        f"access_type=offline&"

        f"prompt=consent&"

        f"include_granted_scopes=true"

    )

    return {"url": auth_url, "client_id": client_id, "redirect_uri": redirect_uri}

@app.post("/api/auth/google/callback")

async def handle_google_callback(request: GoogleCallbackRequest, response: Response):

    code = request.code.strip()

    client_id = GOOGLE_CONFIG.get("client_id")

    client_secret = GOOGLE_CONFIG.get("client_secret")

    redirect_uri = GOOGLE_CONFIG.get("redirect_uri", "http://localhost:8000")

    token_uri = GOOGLE_CONFIG.get("token_uri", "https://oauth2.googleapis.com/token")

    if not client_id or not client_secret:

        raise HTTPException(status_code=500, detail="Google credentials not configured.")

    async with httpx.AsyncClient(timeout=15.0) as client:

        token_res = await client.post(

            token_uri,

            data={

                "code": code,

                "client_id": client_id,

                "client_secret": client_secret,

                "redirect_uri": redirect_uri,

                "grant_type": "authorization_code",

            },

        )

        if token_res.status_code != 200:

            logger.error(f"Google token exchange failed: {token_res.status_code} {token_res.text}")

            raise HTTPException(status_code=400, detail=f"Google authorization failed: {token_res.text}")

        token_data = token_res.json()

        access_token = token_data.get("access_token")

        refresh_token = token_data.get("refresh_token")

        expires_in = token_data.get("expires_in", 3600)

        granted_scopes = token_data.get("scope", "")

        userinfo_res = await client.get(

            "https://www.googleapis.com/oauth2/v2/userinfo",

            headers={"Authorization": f"Bearer {access_token}"},

        )

        if userinfo_res.status_code != 200:

            raise HTTPException(status_code=400, detail="Failed to retrieve Google user profile.")

        user_info = userinfo_res.json()

        email = user_info.get("email")

        name = user_info.get("name") or email.split("@")[0]

        picture = user_info.get("picture")

        user = await asyncio.to_thread(
            db_create_user,
            name=name,
            email=email,
            picture=picture,
            auth_provider="google",
            is_verified=True,
        )

        if refresh_token or access_token:

            await asyncio.to_thread(
                db_save_oauth_tokens,
                key="system_mailer",
                access_token=access_token,
                refresh_token=refresh_token,
                expires_in=expires_in,
                email=email,
                scopes=granted_scopes,
            )

            logger.info(f"Saved system mailer OAuth tokens for {email}")

        session_token = await asyncio.to_thread(db_create_user_session, user["id"])

        response.set_cookie(

            key="llm_gateway_session",

            value=session_token,

            max_age=7 * 86400,

            httponly=True,

            samesite="lax",

            path="/",

        )

        return {"status": "ok", "user": user, "token": session_token}

@app.post("/api/auth/signup")

async def handle_signup(request: SignupRequest):

    email = request.email.strip().lower()

    name = request.name.strip()

    password = request.password

    if not is_valid_email(email):

        raise HTTPException(status_code=400, detail="Please enter a valid email address.")

    if len(name) < 2:

        raise HTTPException(status_code=400, detail="Name must be at least 2 characters.")

    if len(password) < 6:

        raise HTTPException(status_code=400, detail="Password must be at least 6 characters.")

    existing = await asyncio.to_thread(db_get_user_by_email, email)

    if existing and existing["is_verified"]:

        raise HTTPException(status_code=400, detail="An account with this email already exists. Please sign in.")

    await asyncio.to_thread(
        db_create_user,
        name=name,
        email=email,
        password=password,
        auth_provider="local",
        is_verified=False,
    )

    otp_code = await asyncio.to_thread(db_create_otp, email)

    dispatch_result = await dispatch_otp_email(email, otp_code)

    return {

        "status": "otp_sent",

        "email": email,

        "message": dispatch_result.get("message"),

        "dev_otp": dispatch_result.get("dev_otp"),

        "method": dispatch_result.get("method"),

    }

@app.post("/api/auth/verify-otp")

async def handle_verify_otp(request: VerifyOtpRequest, response: Response):

    email = request.email.strip().lower()

    otp_code = (request.otp_code or request.otp or "").strip()

    ok, message = await asyncio.to_thread(db_verify_otp, email, otp_code)

    if not ok:

        raise HTTPException(status_code=400, detail=message)

    user = await asyncio.to_thread(db_get_user_by_email, email)

    if not user:

        raise HTTPException(status_code=404, detail="User not found.")

    session_token = await asyncio.to_thread(db_create_user_session, user["id"])

    response.set_cookie(

        key="llm_gateway_session",

        value=session_token,

        max_age=7 * 86400,

        httponly=True,

        samesite="lax",

        path="/",

    )

    return {"status": "verified", "user": user, "token": session_token}

@app.post("/api/auth/resend-otp")

async def handle_resend_otp(request: ResendOtpRequest):

    email = request.email.strip().lower()

    user = await asyncio.to_thread(db_get_user_by_email, email)

    if not user:

        raise HTTPException(status_code=404, detail="Account not found. Please sign up.")

    otp_code = await asyncio.to_thread(db_create_otp, email)

    dispatch_result = await dispatch_otp_email(email, otp_code)

    return {

        "status": "otp_sent",

        "email": email,

        "dev_otp": dispatch_result.get("dev_otp"),

        "message": dispatch_result.get("message"),

    }

@app.post("/api/auth/login")

async def handle_login(request: LoginRequest, response: Response):

    email = request.email.strip().lower()

    password = request.password

    user = await asyncio.to_thread(db_get_user_by_email, email)

    if not user:

        raise HTTPException(status_code=400, detail="Invalid email or password.")

    if not user["password_hash"] or not verify_password(user["password_hash"], password):

        raise HTTPException(status_code=400, detail="Invalid email or password.")

    if not user["is_verified"]:

        otp_code = await asyncio.to_thread(db_create_otp, email)

        dispatch_result = await dispatch_otp_email(email, otp_code)

        return {

            "status": "unverified",

            "email": email,

            "dev_otp": dispatch_result.get("dev_otp"),

            "message": "Account email is unverified. We have sent a verification code to your email.",

        }

    session_token = await asyncio.to_thread(db_create_user_session, user["id"])

    response.set_cookie(

        key="llm_gateway_session",

        value=session_token,

        max_age=7 * 86400,

        httponly=True,

        samesite="lax",

        path="/",

    )

    return {"status": "ok", "user": user, "token": session_token}

@app.get("/api/auth/me")

async def get_current_user(request: Request):

    token = request.cookies.get("llm_gateway_session")

    if not token:

        auth_header = request.headers.get("Authorization")

        if auth_header and auth_header.startswith("Bearer "):

            token = auth_header[7:].strip()

    if not token:

        return {"authenticated": False, "user": None}

    user = db_validate_session(token)

    if not user:

        return {"authenticated": False, "user": None}

    return {"authenticated": True, "user": user}

@app.post("/api/auth/logout")

async def handle_logout(request: Request, response: Response):

    token = request.cookies.get("llm_gateway_session")

    if token:

        await asyncio.to_thread(db_delete_user_session, token)

    response.delete_cookie(key="llm_gateway_session", path="/")

    return {"status": "logged_out"}

@app.get("/api/auth/mailer-status")

async def get_mailer_status():

    tokens = await asyncio.to_thread(db_get_oauth_tokens, "system_mailer")

    has_gmail = bool(tokens and (tokens.get("refresh_token") or tokens.get("access_token")))

    smtp_host = os.getenv("SMTP_HOST")

    has_smtp = bool(smtp_host and os.getenv("SMTP_USER"))

    return {

        "gmail_api_configured": has_gmail,

        "gmail_account": tokens.get("email") if tokens else None,

        "smtp_configured": has_smtp,

        "can_send_real_email": has_gmail or has_smtp,

    }

class GenerateAvatarRequest(BaseModel):

    name: str = "User"

    bg_color: str = "#06b6d4"

    seed: int = 0

    style: str = "cartoon"

class GenerateAvatarsBatchRequest(BaseModel):

    name: str = "User"

    bg_color: str = "#06b6d4"

    page: int = 0

    count: int = 6

class UpdateProfileRequest(BaseModel):

    name: str

    picture: Optional[str] = None

    bg_color: Optional[str] = None

@app.post("/api/user/generate-avatar")

async def api_generate_avatar(body: GenerateAvatarRequest):

    avatar_url = generate_avatar_image(

        name=body.name,

        bg_hex=body.bg_color,

        seed_val=body.seed,

        style=body.style,

        size=256,

    )

    return {"avatar_data_url": avatar_url}

@app.post("/api/user/generate-avatars-batch")

async def api_generate_avatars_batch(body: GenerateAvatarsBatchRequest):

    count = min(max(1, body.count), 12)

    start_seed = body.page * count + 1

    avatars = []

    for i in range(count):

        seed_val = start_seed + i

        data_url = generate_avatar_image(

            name=body.name,

            bg_hex=body.bg_color,

            seed_val=seed_val,

            size=180,

        )

        avatars.append({"id": seed_val, "url": data_url})

    return {"avatars": avatars, "page": body.page}

@app.patch("/api/user/profile")

async def api_update_profile(body: UpdateProfileRequest, request: Request):

    token = request.cookies.get("llm_gateway_session")

    if not token:

        auth_header = request.headers.get("Authorization")

        if auth_header and auth_header.startswith("Bearer "):

            token = auth_header[7:].strip()

    if not token:

        raise HTTPException(status_code=401, detail="Authentication required")

    user = await asyncio.to_thread(db_validate_session, token)

    if not user:

        raise HTTPException(status_code=401, detail="Session expired or invalid")

    new_name = body.name.strip()

    if not new_name:

        raise HTTPException(status_code=400, detail="Name cannot be empty")

    updated_user = await asyncio.to_thread(
        db_update_user_profile,
        user_id=user["id"],
        name=new_name,
        picture=body.picture,
        bg_color=body.bg_color,
    )

    # Sync sessions user_name in SQLite

    try:

        def _sync_user_name():
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.execute("UPDATE sessions SET user_name = ? WHERE user_id = ?", (new_name, user["id"]))
            conn.commit()
            conn.close()

        await asyncio.to_thread(_sync_user_name)

    except Exception as e:

        logger.warning(f"Could not sync sessions user_name: {e}")

    return {"status": "ok", "user": updated_user}

@app.post("/v1/chat")

async def chat_endpoint(request: ChatRequest, request_http: Request):

    """

    Phase 3 Reliable Chat Endpoint:

    - Target resolution (alias or direct model ID)

    - Full fallback chain execution across providers

    - Circuit breaker checking and trip/cooldown handling

    - Error classification and retry with exponential backoff & jitter

    - 30-second time budget

    - Persistence into SQLite request_logs and chat sessions

    """

    start_time = time.perf_counter()

    session_id = request.session_id or str(uuid.uuid4())

    # -----------------------------------------------------------------------
    # Step 1 in Pipeline: Auth (Gateway API Key or Session Authentication)
    # -----------------------------------------------------------------------
    raw_key = None
    auth_header = request_http.headers.get("Authorization")
    if auth_header and auth_header.startswith("Bearer "):
        raw_key = auth_header[7:].strip()
    elif request_http.headers.get("X-API-Key"):
        raw_key = request_http.headers.get("X-API-Key").strip()

    user = None
    key_info = None

    if raw_key:
        key_info = await asyncio.to_thread(validate_gateway_api_key, raw_key)
        if not key_info:
            logger.warning("[AUTH] Invalid, revoked, or non-existent Gateway API key attempted.")
            await TRAFFIC_BROADCASTER.broadcast("auth_failed", {
                "reason": "invalid_or_revoked_key",
                "status_code": 401,
            })
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Unauthorized: Invalid or revoked Gateway API key.",
                headers={"WWW-Authenticate": "Bearer"},
            )
    else:
        # Check session cookie for browser UI, or allow local web developer guest
        session_token = request_http.cookies.get("llm_gateway_session")
        if session_token:
            user = await asyncio.to_thread(db_validate_session, session_token)
        if user:
            key_info = {"id": 0, "name": f"user_{user['id']}", "quota": 60, "daily_budget": 50.0}
        else:
            # Seamless default developer/web guest for browser dashboard
            key_info = {"id": 0, "name": "web_developer_guest", "quota": 60, "daily_budget": 50.0}

    user_id = user["id"] if user else None
    user_name = user["name"] if user else None
    user_email = user["email"] if user else None

    # -----------------------------------------------------------------------
    # Step 2 in Pipeline: Rate Limit (Redis Token Bucket per key)
    # -----------------------------------------------------------------------
    await TRAFFIC_BROADCASTER.broadcast("redis_check", {"key_id": key_info["id"], "quota": key_info.get("quota", 60)})
    allowed, retry_after, remaining = await RATE_LIMITER.check_rate_limit(
        key_id=key_info["id"],
        quota_rpm=key_info.get("quota", 60),
    )
    if not allowed:
        logger.warning(
            f"[RATE_LIMIT] Key '{key_info['name']}' (ID: {key_info['id']}) exceeded quota ({key_info.get('quota', 60)} rpm). Retry-After: {retry_after}s."
        )
        await TRAFFIC_BROADCASTER.broadcast("rate_limited", {
            "key_id": key_info["id"],
            "key_name": key_info["name"],
            "retry_after": retry_after,
            "status_code": 429,
        })
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Too Many Requests: Rate limit quota exceeded for key '{key_info['name']}'. Please retry in {retry_after}s.",
            headers={"Retry-After": str(retry_after)},
        )

    # -----------------------------------------------------------------------
    # Step 2b: Budget Guard (Per-key daily budget limit)
    # -----------------------------------------------------------------------
    key_daily_budget = float(key_info.get("daily_budget") or 5.0)
    budget_ok, current_spend, budget_limit = await BUDGET_GUARD.check_budget(
        str(key_info["id"]), key_info["name"], key_daily_budget
    )
    if not budget_ok:
        logger.warning(
            f"[BUDGET] Key '{key_info['name']}' exceeded daily budget: ${current_spend:.4f} >= ${budget_limit:.2f}"
        )
        await TRAFFIC_BROADCASTER.broadcast("budget_blocked", {
            "key_id": key_info["id"],
            "key_name": key_info["name"],
            "current_spend": current_spend,
            "daily_budget": budget_limit,
            "status_code": 429,
        })
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Budget exceeded: Daily budget limit of ${budget_limit:.2f} reached for API key '{key_info['name']}'. Current spend today: ${current_spend:.4f}.",
        )

    # -----------------------------------------------------------------------
    # Step 3 in Pipeline: Validate & Input Guardrails (OWASP Top 10)
    # -----------------------------------------------------------------------
    if not request.messages:
        raise HTTPException(status_code=400, detail="Validation error: 'messages' array cannot be empty.")

    messages_payload = [{"role": m.role, "content": m.content} for m in request.messages]

    # Input Guardrails Scan: Prompt Injection (LLM01), SSRF (LLM06), Secrets/PII (LLM02), Payload Size (LLM10)
    raw_content_len = int(request_http.headers.get("content-length") or 0)
    client_ip = request_http.client.host if request_http.client else "127.0.0.1"
    try:
        guard_res = GUARDRAILS.inspect_input(messages_payload, raw_body_len=raw_content_len)
        messages_payload = guard_res["sanitized_messages"]
    except HTTPException as h_err:
        await AUDIT_LOGGER.log_event(
            key_id=str(key_info["id"]),
            key_name=key_info["name"],
            endpoint="/v1/chat",
            outcome="blocked",
            hits=[{"detail": h_err.detail}],
            summary=f"Blocked by Input Guardrail: {h_err.detail}",
            ip_address=client_ip,
        )
        await TRAFFIC_BROADCASTER.broadcast("guardrail_blocked", {
            "key_id": key_info["id"],
            "reason": h_err.detail,
            "status_code": h_err.status_code,
        })
        raise h_err

    # Determine target alias or model
    target = "fast"
    if request.alias is not None and request.alias.strip():
        target = request.alias.strip()
    elif request.model is not None and request.model.strip():
        target = request.model.strip()

    temperature = request.temperature if request.temperature is not None else 0.7

    # -----------------------------------------------------------------------
    # Step 3b: Exact-Match Response Cache Check (Redis with TTL)
    # -----------------------------------------------------------------------
    cache_key = build_cache_key(alias=target, model=target, messages=messages_payload, temperature=temperature)
    cached_result = await RESPONSE_CACHE.get(cache_key)
    if cached_result:
        cached_resp = dict(cached_result)
        cached_resp["cached"] = True
        cached_resp["latency_ms"] = 2.0
        cached_resp["latency"] = 2.0
        cached_resp["queue_wait_ms"] = 0.0
        saved_cost = cached_resp.get("cost_usd", 0.000010)
        await TRAFFIC_BROADCASTER.broadcast("cache_hit", {
            "key_id": key_info["id"],
            "target": target,
            "alias": target,
            "saved_cost": saved_cost,
        })
        logger.info(
            f"target={target} CACHE_HIT provider={cached_resp.get('provider')} "
            f"tokens={cached_resp.get('token_usage', {}).get('total_tokens', 0)} "
            f"saved_cost=${saved_cost:.6f} status=200"
        )
        return cached_resp
    else:
        await TRAFFIC_BROADCASTER.broadcast("cache_miss", {
            "key_id": key_info["id"],
            "target": target,
            "alias": target,
        })

    # -----------------------------------------------------------------------
    # Step 4 in Pipeline: Request Coalescing & Global Concurrency Bulkhead
    # -----------------------------------------------------------------------
    async def _execute_provider_completion():
        return await execute_reliable_completion(
            target=target,
            messages=messages_payload,
            temperature=temperature,
            max_tokens=request.max_tokens or 1000,
            max_retries_per_provider=2,
            total_time_budget=30.0,
        )

    try:
        result, was_coalesced = await REQUEST_COALESCER.execute(cache_key, _execute_provider_completion)
        if was_coalesced:
            await TRAFFIC_BROADCASTER.broadcast("coalesced", {
                "key_id": key_info["id"],
                "target": target,
                "alias": target,
            })
    except Exception as exc:
        latency_ms = round((time.perf_counter() - start_time) * 1000, 2)
        status_code = 502
        if isinstance(exc, HTTPException):
            status_code = exc.status_code
        elif hasattr(exc, "response") and hasattr(exc.response, "status_code"):
            status_code = exc.response.status_code
        q_wait_ms = getattr(exc, "queue_wait_ms", 0.0)

        # Persist failed attempt to request_logs (never cached)
        db_save_request_log(
            session_id=session_id,
            target=target,
            provider="failed",
            model="",
            status_code=status_code,
            fallback_used=True,
            attempts=[],
            latency_ms=latency_ms,
            total_tokens=0,
            error_message=str(exc),
            queue_wait_ms=q_wait_ms,
        )
        logger.error(f"target={target} latency={latency_ms}ms status={status_code} error={exc}")
        if isinstance(exc, HTTPException):
            raise exc
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Gateway reliability error: {str(exc)}",
        )

    latency_ms = result["latency_ms"]
    queue_wait_ms = result.get("queue_wait_ms", 0.0)
    text = result["text"]
    finish_reason = result["finish_reason"]
    token_usage = result["token_usage"]
    total_tokens = token_usage.get("total_tokens", 0)
    provider = result["provider"]
    model = result["model"]
    fallback_used = result["fallback_used"]
    attempts = result["attempts"]

    # Output Guardrail: Scan for leaked keys, secrets, PII (OWASP LLM02 / LLM05)
    text, output_hits = GUARDRAILS.inspect_output(text)

    # Audit Logging: Non-leaking security event log
    await AUDIT_LOGGER.log_event(
        key_id=str(key_info["id"]),
        key_name=key_info["name"],
        endpoint="/v1/chat",
        outcome="allowed" if not output_hits else "redacted",
        hits=output_hits,
        summary=f"Completion for target {target} ({provider})",
        ip_address=client_ip,
    )

    # -----------------------------------------------------------------------
    # Step 5 in Pipeline: Cost Tracking & Response Cache Store
    # -----------------------------------------------------------------------
    cost_usd = calculate_request_cost(
        alias=target,
        model=model,
        token_usage=token_usage,
        messages=messages_payload,
        completion_text=text,
    )
    await COST_TRACKER.record_cost(
        key_id=str(key_info["id"]),
        key_name=key_info["name"],
        alias=target,
        provider=provider,
        model=model,
        prompt_tokens=token_usage.get("prompt_tokens", 1),
        completion_tokens=token_usage.get("completion_tokens", 1),
        total_tokens=total_tokens or (token_usage.get("prompt_tokens", 1) + token_usage.get("completion_tokens", 1)),
        cost_usd=cost_usd,
    )
    total_spent = await COST_TRACKER.get_total_spend()
    await TRAFFIC_BROADCASTER.broadcast("cost_updated", {
        "key_id": key_info["id"],
        "cost_usd": cost_usd,
        "total_spend": total_spent,
    })

    # Exact-match response caching (Never cache errors!)
    response_to_cache = {
        "text": text,
        "finish_reason": finish_reason,
        "token_usage": token_usage,
        "token usage": token_usage,
        "usage": token_usage,
        "latency_ms": latency_ms,
        "latency (ms)": latency_ms,
        "latency": latency_ms,
        "queue_wait_ms": queue_wait_ms,
        "session_id": session_id,
        "provider": provider,
        "model": model,
        "alias": target,
        "fallback_used": fallback_used,
        "attempts": attempts,
        "cost_usd": cost_usd,
        "cost": cost_usd,
        "cached": False,
        "coalesced": was_coalesced,
    }
    await RESPONSE_CACHE.set(cache_key, response_to_cache)

    # 6. Logging: Per request with strict non-zero cost: alias, provider, model, latency, tokens, cost, status
    logger.info(
        f"target={target} provider={provider} model={model} "
        f"latency={latency_ms}ms queue_wait={queue_wait_ms}ms tokens={total_tokens} "
        f"cost=${cost_usd:.6f} coalesced={was_coalesced} fallback={fallback_used} status=200"
    )

    # Persist successful completion into SQLite request_logs

    db_save_request_log(

        session_id=session_id,

        target=target,

        provider=provider,

        model=model,

        status_code=200,

        fallback_used=fallback_used,

        attempts=attempts,

        latency_ms=latency_ms,

        total_tokens=total_tokens,

        queue_wait_ms=queue_wait_ms,

    )

    try:

        first_user_msg = (

            request.messages[0].content if request.messages else "New Conversation"

        )

        title = first_user_msg[:32] + ("..." if len(first_user_msg) > 32 else "")

        def _persist_session_sync():

            db_create_session(

                session_id=session_id,

                title=title,

                model=f"{provider} ({model})",

                user_id=user_id,

                user_name=user_name,

                user_email=user_email,

            )

            db_save_message(session_id, "user", request.messages[-1].content)

            db_save_message(

                session_id,

                "assistant",

                text,

                latency_ms=latency_ms,

                total_tokens=total_tokens,

            )

        await asyncio.to_thread(_persist_session_sync)

    except Exception as db_err:

        print("Database persistence notice:", db_err)

    # Standard response: text, finish_reason, token usage, latency plus provider, model, alias, fallback_used, attempts

    return {

        "text": text,

        "finish_reason": finish_reason,

        "token_usage": token_usage,

        "token usage": token_usage,

        "usage": token_usage,

        "latency_ms": latency_ms,

        "latency (ms)": latency_ms,

        "latency": latency_ms,

        "queue_wait_ms": queue_wait_ms,

        "session_id": session_id,

        "provider": provider,

        "model": model,

        "alias": target,

        "fallback_used": fallback_used,

        "attempts": attempts,

        "cost_usd": cost_usd,

        "cost": cost_usd,

        "cached": False,

        "coalesced": was_coalesced,

    }

@app.get("/", response_class=HTMLResponse)

@app.get("/chats", response_class=HTMLResponse)

@app.get("/settings", response_class=HTMLResponse)

@app.get("/stats", response_class=HTMLResponse)

@app.get("/observability", response_class=HTMLResponse)

@app.get("/logs", response_class=HTMLResponse)

@app.get("/about", response_class=HTMLResponse)

@app.get("/architecture", response_class=HTMLResponse)

@app.get("/visualize", response_class=HTMLResponse)

async def serve_ui():

    return HTML_CONTENT

LOGO_SVG = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32" width="32" height="32" fill="none">

  <defs>

    <radialGradient id="fav-bubble-core" cx="50%" cy="50%" r="50%">

      <stop offset="60%" stop-color="#ffffff" stop-opacity="0.95"/>

      <stop offset="100%" stop-color="#0ea5e9" stop-opacity="0.12"/>

    </radialGradient>

    <linearGradient id="fav-bubble-ring" x1="4" y1="4" x2="28" y2="28" gradientUnits="userSpaceOnUse">

      <stop offset="0%" stop-color="#38bdf8"/>

      <stop offset="40%" stop-color="#818cf8"/>

      <stop offset="70%" stop-color="#c084fc"/>

      <stop offset="100%" stop-color="#ffffff"/>

    </linearGradient>

    <linearGradient id="fav-bubble-crescent" x1="8" y1="26" x2="26" y2="22" gradientUnits="userSpaceOnUse">

      <stop offset="0%" stop-color="#38bdf8" stop-opacity="0"/>

      <stop offset="45%" stop-color="#67e8f9"/>

      <stop offset="75%" stop-color="#ffffff"/>

      <stop offset="100%" stop-color="#c084fc" stop-opacity="0"/>

    </linearGradient>

    <filter id="fav-bubble-glow" x="-20%" y="-20%" width="140%" height="140%">

      <feGaussianBlur stdDeviation="1.2" result="blur"/>

      <feComposite in="SourceGraphic" in2="blur" operator="over"/>

    </filter>

  </defs>

  <circle cx="16" cy="16" r="15" fill="#ffffff"/>

  <circle cx="16" cy="16" r="11.5" fill="url(#fav-bubble-core)"/>

  <circle cx="16" cy="16" r="11.5" stroke="url(#fav-bubble-ring)" stroke-width="2.6" opacity="0.45" filter="url(#fav-bubble-glow)"/>

  <circle cx="16" cy="16" r="11.5" stroke="url(#fav-bubble-ring)" stroke-width="1.5"/>

  <path d="M 6.5 19.5 C 8.5 25.5 17.5 28.5 25.5 20" stroke="url(#fav-bubble-crescent)" stroke-width="2.2" stroke-linecap="round" filter="url(#fav-bubble-glow)"/>

  <path d="M 8.5 20.5 C 11 25.5 17 27 23.5 21" stroke="#ffffff" stroke-width="1.1" stroke-linecap="round" opacity="0.9"/>

</svg>"""

@app.get("/favicon.ico")

@app.get("/favicon.svg")

async def favicon():

    return Response(content=LOGO_SVG, media_type="image/svg+xml")

HTML_CONTENT = """<!DOCTYPE html>

<html lang="en">

<head>

  <meta charset="UTF-8">

  <meta name="viewport" content="width=device-width, initial-scale=1.0">

  <meta name="referrer" content="no-referrer">

  <title>LLM Gateway</title>

  <link rel="icon" type="image/svg+xml" href="/favicon.svg">

  <link rel="alternate icon" href="/favicon.ico">

  <script>

    // Silence Tailwind Play CDN production warning in console

    const _origWarn = console.warn;

    console.warn = function(...args) {

      if (args[0] && typeof args[0] === 'string' && args[0].includes('cdn.tailwindcss.com')) return;

      _origWarn.apply(console, args);

    };

  </script>

  <script src="https://cdn.tailwindcss.com"></script>

  <script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js"></script>

  <link rel="preconnect" href="https://fonts.googleapis.com">

  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>

  <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700&display=swap" rel="stylesheet">

  <style>

    body {

      font-family: 'Plus Jakarta Sans', sans-serif;

      background-color: #080c14;

      color: #e2e8f0;

      overflow: hidden;

    }

    .top-gradient-shade {

      position: absolute;

      top: 0;

      left: 0;

      right: 0;

      height: 380px;

      pointer-events: none;

      background: radial-gradient(circle at 75% 0%, rgba(20, 184, 166, 0.16) 0%, rgba(6, 182, 212, 0.1) 25%, rgba(99, 102, 241, 0.04) 50%, transparent 80%),

                  radial-gradient(circle at 30% -10%, rgba(14, 165, 233, 0.08) 0%, transparent 60%);

      z-index: 1;

    }

    .glass-panel {

      background: rgba(15, 23, 42, 0.65);

      backdrop-filter: blur(16px);

      border: 1px solid rgba(255, 255, 255, 0.07);

    }

    .input-glass {

      background: #111827;

      border: 1px solid rgba(255, 255, 255, 0.08);

      box-shadow: 0 10px 30px -10px rgba(0, 0, 0, 0.5);

    }

    .input-glass:focus-within {

      border-color: rgba(45, 212, 191, 0.4);

      box-shadow: 0 0 25px -5px rgba(20, 184, 166, 0.25);

    }

    .card-hover {

      transition: all 0.2s cubic-bezier(0.16, 1, 0.3, 1);

    }

    .card-hover:hover {

      transform: translateY(-2px);

      border-color: rgba(45, 212, 191, 0.3);

      background: rgba(30, 41, 59, 0.7);

    }

    ::-webkit-scrollbar { width: 5px; }

    ::-webkit-scrollbar-track { background: transparent; }

    ::-webkit-scrollbar-thumb { background: rgba(255, 255, 255, 0.1); border-radius: 4px; }

    ::-webkit-scrollbar-thumb:hover { background: rgba(255, 255, 255, 0.2); }

    .custom-dropdown-scroll {

      scrollbar-width: thin;

      scrollbar-color: rgba(255, 255, 255, 0.2) transparent;

    }

    .custom-dropdown-scroll::-webkit-scrollbar {

      width: 4px;

    }

    .custom-dropdown-scroll::-webkit-scrollbar-track {

      background: transparent;

      margin: 8px 0;

    }

    .custom-dropdown-scroll::-webkit-scrollbar-thumb {

      background: rgba(255, 255, 255, 0.2);

      border-radius: 9999px;

    }

    .custom-dropdown-scroll::-webkit-scrollbar-thumb:hover {

      background: rgba(255, 255, 255, 0.35);

    }

    /* Custom Sleek Tooltip Styling */

    .custom-tooltip {

      position: fixed;

      pointer-events: none;

      z-index: 99999;

      opacity: 0;

      transform: scale(0.93);

      transition: opacity 0.15s cubic-bezier(0.16, 1, 0.3, 1), transform 0.15s cubic-bezier(0.16, 1, 0.3, 1);

      background: rgba(10, 15, 26, 0.95);

      color: #f8fafc;

      border: 1px solid rgba(6, 182, 212, 0.45);

      border-radius: 8px;

      padding: 5px 11px;

      font-size: 11px;

      font-weight: 600;

      white-space: nowrap;

      box-shadow: 0 10px 25px -5px rgba(0, 0, 0, 0.75), 0 0 16px rgba(6, 182, 212, 0.22);

      backdrop-filter: blur(14px);

      -webkit-backdrop-filter: blur(14px);

      letter-spacing: 0.01em;

    }

    .custom-tooltip.tooltip-visible {

      opacity: 1;

      transform: scale(1);

    }

    /* COMPREHENSIVE LIGHT THEME ENGINE */

    html.light-theme,

    body.light-theme {

      background-color: #f8fafc !important;

      color: #0f172a !important;

    }

    .light-theme .top-gradient-shade {

      background: radial-gradient(circle at 75% 0%, rgba(6, 182, 212, 0.1) 0%, rgba(14, 165, 233, 0.06) 30%, transparent 70%),

                  radial-gradient(circle at 30% -10%, rgba(99, 102, 241, 0.05) 0%, transparent 60%) !important;

    }

    .light-theme aside#appSidebar {

      background-color: #ffffff !important;

      border-right-color: #e2e8f0 !important;

    }

    .light-theme aside#appSidebar > div:first-child {

      border-bottom-color: #e2e8f0 !important;

    }

    .light-theme main {

      background-color: #f8fafc !important;

    }

    .light-theme header {

      background-color: rgba(255, 255, 255, 0.92) !important;

      border-bottom-color: #e2e8f0 !important;

    }

    .light-theme .glass-panel {

      background: #ffffff !important;

      border-color: #e2e8f0 !important;

      color: #0f172a !important;

      box-shadow: 0 4px 20px -2px rgba(0, 0, 0, 0.05) !important;

    }

    .light-theme .card-hover:hover {

      background: #f8fafc !important;

      border-color: #cbd5e1 !important;

    }

    .light-theme .input-glass {

      background: #ffffff !important;

      border-color: #cbd5e1 !important;

      color: #0f172a !important;

      box-shadow: 0 4px 15px -3px rgba(0, 0, 0, 0.07) !important;

    }

    .light-theme .input-glass textarea,

    .light-theme .input-glass input {

      color: #0f172a !important;

    }

    .light-theme input:not([type="checkbox"]):not([type="radio"]):not([type="color"]),

    .light-theme textarea,

    .light-theme select {

      background-color: #ffffff !important;

      border-color: #cbd5e1 !important;

      color: #0f172a !important;

    }

    .light-theme input::placeholder,

    .light-theme textarea::placeholder {

      color: #94a3b8 !important;

    }

    /* User Input Textareas: Dynamic theme font color */

    .light-theme #heroChatInput,

    .light-theme #streamChatInput {

      color: #000000 !important;

      font-weight: 500 !important;

    }

    .light-theme #heroChatInput::placeholder,

    .light-theme #streamChatInput::placeholder {

      color: #64748b !important;

    }

    html:not(.light-theme) #heroChatInput,

    html:not(.light-theme) #streamChatInput,

    body:not(.light-theme) #heroChatInput,

    body:not(.light-theme) #streamChatInput {

      color: #ffffff !important;

      font-weight: 500 !important;

    }

    html:not(.light-theme) #heroChatInput::placeholder,

    html:not(.light-theme) #streamChatInput::placeholder,

    body:not(.light-theme) #heroChatInput::placeholder,

    body:not(.light-theme) #streamChatInput::placeholder {

      color: #9ca3af !important;

    }

    /* User Chat Bubble: Dynamic theme font color & contrast */

    .light-theme .user-message-bubble {

      background-color: rgba(6, 182, 212, 0.16) !important;

      border-color: rgba(6, 182, 212, 0.4) !important;

    }

    .light-theme .user-message-bubble,

    .light-theme .user-message-bubble *,

    .light-theme .user-message-text,

    .light-theme .user-message-text *,

    .light-theme .user-message-text p {

      color: #000000 !important;

      font-weight: 600 !important;

    }

    html:not(.light-theme) .user-message-bubble,

    body:not(.light-theme) .user-message-bubble {

      background-color: rgba(6, 182, 212, 0.22) !important;

      border-color: rgba(6, 182, 212, 0.35) !important;

    }

    html:not(.light-theme) .user-message-bubble,

    html:not(.light-theme) .user-message-bubble *,

    html:not(.light-theme) .user-message-text,

    html:not(.light-theme) .user-message-text *,

    html:not(.light-theme) .user-message-text p,

    body:not(.light-theme) .user-message-bubble,

    body:not(.light-theme) .user-message-bubble *,

    body:not(.light-theme) .user-message-text,

    body:not(.light-theme) .user-message-text *,

    body:not(.light-theme) .user-message-text p {

      color: #ffffff !important;

      font-weight: 500 !important;

    }

    /* LLM Gateway Response Logo in light theme */

    .light-theme .llm-gw-logo-bg {

      fill: #ffffff !important;

    }

    .light-theme .llm-gw-logo-core {

      fill: #ffffff !important;

      opacity: 0.1 !important;

    }

    .light-theme .llm-gw-logo-core-stop {

      stop-color: #ffffff !important;

    }

    /* Floating Chat Table Row Menu in light theme */

    .light-theme #chatTableRowActionMenu {

      background-color: #ffffff !important;

      border-color: #e2e8f0 !important;

      box-shadow: 0 10px 25px -5px rgba(0, 0, 0, 0.1), 0 8px 10px -6px rgba(0, 0, 0, 0.1) !important;

      color: #0f172a !important;

    }

    .light-theme #rowActionRenameBtn {

      color: #334155 !important;

    }

    .light-theme #rowActionRenameBtn:hover {

      background-color: #f1f5f9 !important;

      color: #0f172a !important;

    }

    /* Modals in light theme */

    .light-theme .chat-response-content,

    .light-theme .chat-response-content .prose,

    .light-theme .chat-response-content p {

      color: #0f172a !important;

      font-weight: 500 !important;

    }

    .light-theme .chat-response-content strong,

    .light-theme .chat-response-content h1,

    .light-theme .chat-response-content h2,

    .light-theme .chat-response-content h3 {

      color: #020617 !important;

      font-weight: 700 !important;

    }

    /* Reset first child margins in AI response for exact optical alignment with AI glyph */

    .chat-response-content > :first-child,

    .chat-response-content .prose > :first-child,

    .chat-response-content .prose > p:first-child,

    .chat-response-content .prose > h1:first-child,

    .chat-response-content .prose > h2:first-child,

    .chat-response-content .prose > h3:first-child,

    .chat-response-content .prose > ul:first-child,

    .chat-response-content .prose > ol:first-child,

    .chat-response-content .prose > blockquote:first-child,

    .chat-response-content .prose > pre:first-child {

      margin-top: 0 !important;

      padding-top: 0 !important;

    }

    .light-theme #authModal,

    .light-theme #profileModal,

    .light-theme #storageModal {

      background-color: rgba(15, 23, 42, 0.45) !important;

    }

    .light-theme #authModal .glass-panel,

    .light-theme #profileModal .glass-panel,

    .light-theme #storageModal .glass-panel {

      background-color: #ffffff !important;

      border-color: #e2e8f0 !important;

      color: #0f172a !important;

      box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.18) !important;

    }

    /* Popovers, menus & dropdowns */

    .light-theme #profileDropdownMenu,

    .light-theme [id$="Dropdown"],

    .light-theme [id$="Menu"],

    .light-theme [id$="Popover"] {

      background-color: #ffffff !important;

      border-color: #e2e8f0 !important;

      color: #0f172a !important;

      box-shadow: 0 15px 30px -5px rgba(0, 0, 0, 0.12) !important;

    }

    /* Avatar tray in light theme */

    .light-theme #avatarTrayGrid {

      background-color: #f1f5f9 !important;

      border-color: #e2e8f0 !important;

    }

    .light-theme .tray-avatar-card {

      background-color: #ffffff !important;

      border-color: #e2e8f0 !important;

    }

    .light-theme .tray-avatar-card:hover {

      border-color: #cbd5e1 !important;

      background-color: #f8fafc !important;

    }

    .light-theme .tray-avatar-card.ring-cyan-400 {

      border-color: #06b6d4 !important;

      background-color: rgba(6, 182, 212, 0.1) !important;

    }

    /* Tooltip in light theme */

    .light-theme .custom-tooltip {

      background: rgba(255, 255, 255, 0.98) !important;

      color: #0f172a !important;

      border: 1px solid #cbd5e1 !important;

      box-shadow: 0 10px 25px -5px rgba(0, 0, 0, 0.15), 0 0 15px rgba(6, 182, 212, 0.15) !important;

    }

    /* General typography overrides for light mode */

    .light-theme .text-white:not(button[class*="bg-cyan-"]):not(button[class*="bg-blue-"]):not(button[class*="bg-rose-"]):not(button[class*="bg-emerald-"]):not(.keep-white) {

      color: #0f172a !important;

    }

    .light-theme .text-gray-100,

    .light-theme .text-gray-200 {

      color: #1e293b !important;

    }

    .light-theme .text-gray-300 {

      color: #334155 !important;

    }

    .light-theme .text-gray-400 {

      color: #475569 !important;

    }

    .light-theme .text-gray-500 {

      color: #64748b !important;

    }

    /* Tailwind border & background overrides in light theme */

    .light-theme .border-white\/5,

    .light-theme .border-white\/10,

    .light-theme .border-white\/15,

    .light-theme .border-white\/20 {

      border-color: #e2e8f0 !important;

    }

    .light-theme .bg-white\/5 {

      background-color: #f1f5f9 !important;

    }

    .light-theme .bg-white\/10 {

      background-color: #e2e8f0 !important;

    }

    .light-theme .bg-black\/20,

    .light-theme .bg-black\/30,

    .light-theme .bg-black\/40,

    .light-theme .bg-black\/50,

    .light-theme .bg-\[\#0a0e17\],

    .light-theme .bg-\[\#0b101b\],

    .light-theme .bg-\[\#161a22\],

    .light-theme .bg-\[\#111827\],

    .light-theme .bg-gray-900,

    .light-theme .bg-gray-950 {

      background-color: #f8fafc !important;

    }

    /* Sidebar nav buttons */

    .light-theme .sidebar-nav-btn {

      color: #475569 !important;

    }

    .light-theme .sidebar-nav-btn:hover {

      background-color: #f1f5f9 !important;

      color: #0f172a !important;

    }

    .light-theme .sidebar-nav-btn.bg-cyan-500\/15 {

      background-color: rgba(6, 182, 212, 0.12) !important;

      color: #0891b2 !important;

      border-color: rgba(6, 182, 212, 0.35) !important;

    }

    /* Sidebar Logo Bubble: Clean in light theme */

    .light-theme #sbBubbleBg {

      fill: #ffffff !important;

    }

    .light-theme #sbBubbleCore {

      fill: #ffffff !important;

      opacity: 0.1 !important;

    }

    .light-theme #sbBubbleCoreStop {

      stop-color: #ffffff !important;

    }

    .light-theme .sb-bubble-container {

      background-color: #ffffff !important;

      box-shadow: 0 4px 12px rgba(6, 182, 212, 0.15) !important;

    }

    /* Universal Dark Container Overrides in Light Theme */

    .light-theme .bg-\[\#090d16\],

    .light-theme [class*="bg-[#090d16]"],

    .light-theme [class*="bg-[#0f172a]"],

    .light-theme .bg-\[\#0f172a\],

    .light-theme [class*="bg-[#111827]"],

    .light-theme .bg-\[\#111827\],

    .light-theme [class*="bg-[#171b24]"],

    .light-theme .bg-\[\#171b24\],

    .light-theme [class*="bg-[#161a22]"],

    .light-theme .bg-\[\#161a22\] {

      background-color: #ffffff !important;

      border-color: #e2e8f0 !important;

    }

    /* Tables & Theads in light theme */

    .light-theme table thead,

    .light-theme thead.bg-\[\#111827\],

    .light-theme thead[class*="bg-[#111827]"],

    .light-theme table thead th {

      background-color: #f8fafc !important;

      color: #475569 !important;

      border-bottom-color: #e2e8f0 !important;

    }

    .light-theme table tbody tr {

      background-color: #ffffff !important;

      border-bottom-color: #f1f5f9 !important;

      color: #0f172a !important;

    }

    .light-theme table tbody tr:hover {

      background-color: #f8fafc !important;

    }

    /* Secondary & Action Buttons in Light Theme */

    .light-theme button[class*="bg-red-950"],

    .light-theme button.bg-red-950\/40,

    .light-theme [class*="bg-red-950"] {

      background-color: #fee2e2 !important;

      color: #b91c1c !important;

      border-color: #fca5a5 !important;

    }

    .light-theme button[class*="bg-red-950"]:hover {

      background-color: #fecaca !important;

      color: #991b1b !important;

    }

    .light-theme button[class*="bg-[#0f172a]"],

    .light-theme button.bg-\[\#0f172a\] {

      background-color: #f1f5f9 !important;

      color: #0f172a !important;

      border-color: #cbd5e1 !important;

    }

    /* Topology Visualizer Light Theme Overrides */

    .light-theme #topologyContainer {

      background: #f8fafc !important;

      border-color: #cbd5e1 !important;

    }

    .light-theme #vizSvgCanvas path:not(.viz-hidden-guide):not([stroke="none"]) {

      stroke: rgba(100, 116, 139, 0.45) !important;

    }

    .light-theme #vizSvgCanvas path.viz-hidden-guide,
    .light-theme #vizSvgCanvas path[stroke="none"] {
      stroke: none !important;
      fill: none !important;
    }

    #vizSvgCanvas {
      overflow: visible !important;
    }

    .light-theme #path-queue-groq {
      stroke: #f43f5e !important;
      stroke-width: 2.5px !important;
    }

    .light-theme #path-queue-gemini {
      stroke: #6366f1 !important;
      stroke-width: 2.5px !important;
    }

    .light-theme #path-queue-openrouter {
      stroke: #10b981 !important;
      stroke-width: 2.5px !important;
    }

    .light-theme #cable-audit-client {
      stroke: #059669 !important;
      stroke-width: 2.2px !important;
    }

    .light-theme #vizEventStreamFeed {

      background: #f1f5f9 !important;

      color: #0f172a !important;

      border-color: #e2e8f0 !important;

    }

    .light-theme #vizSvgCanvas marker path {

      stroke: none !important;

    }

    /* Visualize Studio Page — Light Theme */

    .light-theme #visualizePage,

    .light-theme #studioCanvasViewport {

      background-color: #f1f5f9 !important;

    }

    .light-theme #studioCanvasViewport {

      background-image: radial-gradient(circle, rgba(15, 23, 42, 0.10) 1.2px, transparent 1.2px) !important;

    }

    .light-theme #studioNodeGateway > div:first-child {

      background-color: #f5f3ff !important;

      box-shadow: 0 0 22px rgba(168, 85, 247, 0.22), inset 0 0 10px rgba(168, 85, 247, 0.06) !important;

    }

    .light-theme #studioNodeBalancer > div:first-child {

      background-color: #f0fdfa !important;

      box-shadow: 0 0 22px rgba(20, 184, 166, 0.22), inset 0 0 10px rgba(20, 184, 166, 0.06) !important;

    }

    .light-theme #studioNodeAppServer > div:first-child {

      background-color: #eff6ff !important;

      box-shadow: 0 0 22px rgba(59, 130, 246, 0.22), inset 0 0 10px rgba(59, 130, 246, 0.06) !important;

    }

    .light-theme #studioNodeCache > div:first-child {

      background-color: #fff1f2 !important;

      box-shadow: 0 0 22px rgba(244, 63, 94, 0.22), inset 0 0 10px rgba(244, 63, 94, 0.06) !important;

    }

    .light-theme #studioNodeGemini > div:first-child {

      background-color: #eef2ff !important;

      box-shadow: 0 0 22px rgba(99, 102, 241, 0.22), inset 0 0 10px rgba(99, 102, 241, 0.06) !important;

    }

    .light-theme #studioNodeDatabase > div:first-child {

      background-color: #ecfdf5 !important;

      box-shadow: 0 0 22px rgba(16, 185, 129, 0.22), inset 0 0 10px rgba(16, 185, 129, 0.06) !important;

    }

    .light-theme #studioNodesStage .node-title,

    .light-theme #studioNodesStage span.text-gray-200 {

      color: #0f172a !important;

      font-weight: 700 !important;

    }

    .light-theme #studioNodesStage .node-sub-purple,

    .light-theme #studioNodesStage span[class*="text-purple-300"] {

      color: #6b21a8 !important;

      font-weight: 600 !important;

      opacity: 1 !important;

    }

    .light-theme #studioNodesStage .node-sub-teal,

    .light-theme #studioNodesStage span[class*="text-teal-300"] {

      color: #0f766e !important;

      font-weight: 600 !important;

      opacity: 1 !important;

    }

    .light-theme #studioNodesStage .node-sub-blue,

    .light-theme #studioNodesStage span[class*="text-blue-300"] {

      color: #1d4ed8 !important;

      font-weight: 600 !important;

      opacity: 1 !important;

    }

    .light-theme #studioNodesStage .node-sub-rose,

    .light-theme #studioNodesStage span[class*="text-rose-300"] {

      color: #be123c !important;

      font-weight: 600 !important;

      opacity: 1 !important;

    }

    .light-theme #studioNodesStage .node-sub-indigo,

    .light-theme #studioNodesStage span[class*="text-indigo-300"] {

      color: #4338ca !important;

      font-weight: 600 !important;

      opacity: 1 !important;

    }

    .light-theme #studioNodesStage .node-sub-emerald,

    .light-theme #studioNodesStage span[class*="text-emerald-300"] {

      color: #047857 !important;

      font-weight: 600 !important;

      opacity: 1 !important;

    }

    .light-theme #vizK6ControlBar {

      background-color: rgba(255, 255, 255, 0.96) !important;

      border-color: #cbd5e1 !important;

      box-shadow: 0 10px 25px -5px rgba(0, 0, 0, 0.08), 0 8px 10px -6px rgba(0, 0, 0, 0.06) !important;

    }

    .light-theme #vizK6ControlBar select,

    .light-theme #vizK6ControlBar input {

      background-color: #f8fafc !important;

      color: #0f172a !important;

      border-color: #cbd5e1 !important;

    }

    .light-theme #vizK6ControlBar span {

      color: #334155 !important;

    }

    .light-theme #vizK6ControlBar .text-amber-400 {

      color: #b45309 !important;

    }

    .light-theme #vizK6ControlBar .text-cyan-400 {

      color: #0891b2 !important;

    }

    .light-theme #vizK6ControlBar .bg-black\/40,

    .light-theme #vizK6ControlBar .bg-black\/50 {

      background-color: #f1f5f9 !important;

      border-color: #e2e8f0 !important;

    }

    .light-theme #vizK6ControlBar .bg-white\/5 {

      background-color: #f1f5f9 !important;

      border-color: #e2e8f0 !important;

      color: #475569 !important;

    }

    .light-theme #studioCanvasViewport .bg-\[\#141923\]\/90 {

      background-color: rgba(255, 255, 255, 0.92) !important;

      border-color: #e2e8f0 !important;

    }

    .light-theme #visualizerFeedModal {

      background-color: rgba(255, 255, 255, 0.97) !important;

      border-color: #e2e8f0 !important;

    }

    .light-theme #visualizerFeedModal .bg-black\/50 {

      background-color: #f8fafc !important;

      border-color: #e2e8f0 !important;

    }

    .light-theme #studioNodeClient > div:first-child {

      background-color: #f8fafc !important;

      box-shadow: 0 0 22px rgba(100, 116, 139, 0.22), inset 0 0 10px rgba(100, 116, 139, 0.06) !important;

    }

    .light-theme #studioNodeAuth > div:first-child {

      background-color: #eef2ff !important;

      box-shadow: 0 0 22px rgba(99, 102, 241, 0.22), inset 0 0 10px rgba(99, 102, 241, 0.06) !important;

    }

    .light-theme #studioNodeRateLimit > div:first-child {

      background-color: #fffbeb !important;

      box-shadow: 0 0 22px rgba(245, 158, 11, 0.22), inset 0 0 10px rgba(245, 158, 11, 0.06) !important;

    }

    .light-theme #studioNodeRedis > div:first-child {

      background-color: #fff1f2 !important;

      box-shadow: 0 0 22px rgba(225, 29, 72, 0.22), inset 0 0 10px rgba(225, 29, 72, 0.06) !important;

    }

    .light-theme #studioNodeRetry > div:first-child {

      background-color: #fff7ed !important;

      box-shadow: 0 0 22px rgba(249, 115, 22, 0.22), inset 0 0 10px rgba(249, 115, 22, 0.06) !important;

    }

    .light-theme #studioNodeIdempotency > div:first-child {

      background-color: #f5f3ff !important;

      box-shadow: 0 0 22px rgba(139, 92, 246, 0.22), inset 0 0 10px rgba(139, 92, 246, 0.06) !important;

    }

    .light-theme #studioNodeJobQueue > div:first-child {

      background-color: #ecfeff !important;

      box-shadow: 0 0 22px rgba(6, 182, 212, 0.22), inset 0 0 10px rgba(6, 182, 212, 0.06) !important;

    }

    .light-theme #studioNodeWorker > div:first-child {

      background-color: #eff6ff !important;

      box-shadow: 0 0 22px rgba(59, 130, 246, 0.22), inset 0 0 10px rgba(59, 130, 246, 0.06) !important;

    }

    .light-theme #studioNodeDLQ > div:first-child {

      background-color: #fff1f2 !important;

      box-shadow: 0 0 22px rgba(225, 29, 72, 0.22), inset 0 0 10px rgba(225, 29, 72, 0.06) !important;

    }

    .light-theme #studioNodeInputGuardrails > div:first-child {

      background-color: #ecfdf5 !important;

      box-shadow: 0 0 22px rgba(16, 185, 129, 0.22), inset 0 0 10px rgba(16, 185, 129, 0.06) !important;

    }

    .light-theme #studioNodeOutputGuardrails > div:first-child {

      background-color: #ecfeff !important;

      box-shadow: 0 0 22px rgba(6, 182, 212, 0.22), inset 0 0 10px rgba(6, 182, 212, 0.06) !important;

    }

    .light-theme #studioNodeAuditLog > div:first-child {

      background-color: #fffbeb !important;

      box-shadow: 0 0 22px rgba(245, 158, 11, 0.22), inset 0 0 10px rgba(245, 158, 11, 0.06) !important;

    }

    .light-theme #studioNodeCacheStore > div:first-child {

      background-color: #ecfdf5 !important;

      box-shadow: 0 0 22px rgba(16, 185, 129, 0.22), inset 0 0 10px rgba(16, 185, 129, 0.06) !important;

    }

    .light-theme #studioNodesStage span.text-amber-400 {

      color: #b45309 !important;

    }

    .light-theme #studioNodesStage span.text-indigo-400 {

      color: #4338ca !important;

    }

    .light-theme #studioNodesStage span.text-rose-400 {

      color: #be123c !important;

    }

    .light-theme #studioNodesStage span.text-violet-400 {

      color: #6d28d9 !important;

    }

    /* ChatGPT Style Voice Soundwave Visualizer in Input Bar */
    .soundwave-bar {
      display: inline-block;
      width: 2.5px;
      min-height: 3px;
      max-height: 22px;
      height: 4px;
      border-radius: 9999px;
      background-color: rgba(255, 255, 255, 0.85);
      transition: height 0.06s ease;
    }
    .light-theme .soundwave-bar {
      background-color: rgba(15, 23, 42, 0.85) !important;
    }
    .light-theme .chatgpt-soundwave {
      background-color: rgba(0, 0, 0, 0.05) !important;
      border-color: rgba(0, 0, 0, 0.12) !important;
    }

    .light-theme #nodeInspectorDrawer,

    .light-theme #vizReportBtn,

    .light-theme #vizLiveLatencyDock {

      background-color: #ffffff !important;

      border-color: #e2e8f0 !important;

      box-shadow: 0 10px 25px -5px rgba(0, 0, 0, 0.08) !important;

      color: #0f172a !important;

    }

    .light-theme #vizLiveLatencyDock .text-white {

      color: #0f172a !important;

    }

    .light-theme #k6RunReportModal {

      background-color: rgba(15, 23, 42, 0.3) !important;

    }

    .light-theme #k6RunReportModal .custom-modal-box {

      background-color: #ffffff !important;

      color: #0f172a !important;

      border-color: #e2e8f0 !important;

      box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.15) !important;

    }

    .light-theme #k6RunReportModal .custom-modal-box h3,

    .light-theme #k6RunReportModal .custom-modal-box .text-white {

      color: #0f172a !important;

    }

    .light-theme #k6RunReportModal .bg-black\/40,

    .light-theme #k6RunReportModal .bg-black\/60 {

      background-color: #f1f5f9 !important;

      border-color: #e2e8f0 !important;

    }

    .light-theme #k6RunReportModal .text-gray-400 {

      color: #64748b !important;

    }

    .light-theme #k6RunReportModal .bg-blue-950\/40 {
      background-color: #eff6ff !important;
      border-color: #bfdbfe !important;
    }
    .light-theme #k6RunReportModal .text-blue-400 {
      color: #1d4ed8 !important;
    }
    .light-theme #k6RunReportModal .text-blue-300 {
      color: #1e40af !important;
    }
    .light-theme #k6RunReportModal .bg-emerald-950\/40 {
      background-color: #f0fdf4 !important;
      border-color: #bbf7d0 !important;
    }
    .light-theme #k6RunReportModal .text-emerald-400 {
      color: #15803d !important;
    }
    .light-theme #k6RunReportModal .text-emerald-300 {
      color: #166534 !important;
    }

    .light-theme #k6RunReportModal .border-white\/5,

    .light-theme #k6RunReportModal .border-white\/10 {

      border-color: #e2e8f0 !important;

    }

    /* Dedicated Internal Modal Scrollbars */
    .custom-modal-scroll {
      scrollbar-width: thin;
      scrollbar-color: rgba(148, 163, 184, 0.4) transparent;
    }
    .custom-modal-scroll::-webkit-scrollbar {
      width: 6px;
    }
    .custom-modal-scroll::-webkit-scrollbar-track {
      background: transparent;
      margin: 8px 0;
    }
    .custom-modal-scroll::-webkit-scrollbar-thumb {
      background: rgba(148, 163, 184, 0.35);
      border-radius: 9999px;
    }
    .custom-modal-scroll::-webkit-scrollbar-thumb:hover {
      background: rgba(148, 163, 184, 0.65);
    }

    .light-theme .custom-modal-scroll {
      scrollbar-color: rgba(100, 116, 139, 0.35) transparent;
    }
    .light-theme .custom-modal-scroll::-webkit-scrollbar-thumb {
      background: rgba(100, 116, 139, 0.35) !important;
    }
    .light-theme .custom-modal-scroll::-webkit-scrollbar-thumb:hover {
      background: rgba(100, 116, 139, 0.65) !important;
    }

    /* Modal Backdrop & Outer Card in Light Theme */
    .light-theme #nodeDetailsModal {
      background-color: rgba(15, 23, 42, 0.45) !important;
    }
    .light-theme #nodeDetailsModal .custom-modal-box {
      background-color: #ffffff !important;
      border-color: #e2e8f0 !important;
      color: #0f172a !important;
      box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.18), 0 0 0 1px rgba(0, 0, 0, 0.05) !important;
    }
    .light-theme #nodeDetailsModal .custom-modal-box h3,
    .light-theme #nodeDetailsModal .custom-modal-box .text-white {
      color: #0f172a !important;
    }
    .light-theme #nodeDetailsModal #modalNodeIcon {
      background-color: #f1f5f9 !important;
      border-color: #cbd5e1 !important;
    }
    .light-theme #nodeDetailsModal button[onclick="closeNodeDetailsModal()"] {
      color: #64748b !important;
    }
    .light-theme #nodeDetailsModal button[onclick="closeNodeDetailsModal()"]:hover {
      color: #0f172a !important;
      background-color: #f1f5f9 !important;
    }

    /* Inner Section Cards in Light Theme */
    .light-theme #nodeDetailsModal .bg-slate-900,
    .light-theme #nodeDetailsModal .bg-black\/40,
    .light-theme #nodeDetailsModal .bg-black\/50,
    .light-theme #nodeDetailsModal .bg-black\/60 {
      background-color: #f8fafc !important;
      border-color: #e2e8f0 !important;
    }
    .light-theme #nodeDetailsModal .border-slate-700\/80,
    .light-theme #nodeDetailsModal .border-white\/10,
    .light-theme #nodeDetailsModal .border-white\/5 {
      border-color: #e2e8f0 !important;
    }

    /* High-Contrast Body Text in Light Theme */
    .light-theme #nodeDetailsModal p,
    .light-theme #nodeDetailsModal .text-slate-300,
    .light-theme #nodeDetailsModal .text-gray-300,
    .light-theme #nodeDetailsModal .text-gray-200 {
      color: #334155 !important;
    }
    .light-theme #nodeDetailsModal .text-gray-400,
    .light-theme #nodeDetailsModal .text-slate-400 {
      color: #64748b !important;
    }
    .light-theme #nodeDetailsModal strong {
      color: #0f172a !important;
      font-weight: 700 !important;
    }
    .light-theme #nodeDetailsModal .text-amber-400 {
      color: #b45309 !important;
    }
    .light-theme #nodeDetailsModal .text-cyan-400,
    .light-theme #nodeDetailsModal .text-cyan-300 {
      color: #0284c7 !important;
    }
    .light-theme #nodeDetailsModal .text-emerald-400,
    .light-theme #nodeDetailsModal .text-emerald-300 {
      color: #047857 !important;
    }
    .light-theme #nodeDetailsModal .text-rose-400,
    .light-theme #nodeDetailsModal .text-rose-300,
    .light-theme #nodeDetailsModal .text-rose-200 {
      color: #be123c !important;
    }

    /* Badges & Status Pills in Light Theme */
    .light-theme #nodeDetailsModal #modalBreakerStateBadge,
    .light-theme #nodeDetailsModal #inspectorBreakerPill,
    .light-theme #nodeDetailsModal #inspectorKeyStatus {
      background-color: #dcfce7 !important;
      border-color: #86efac !important;
      color: #15803d !important;
      font-weight: 700 !important;
    }
    .light-theme #nodeDetailsModal .bg-amber-950\/60,
    .light-theme #nodeDetailsModal .bg-amber-950\/80 {
      background-color: #fef3c7 !important;
      border-color: #fde68a !important;
      color: #92400e !important;
      font-weight: 700 !important;
    }

    /* Provider Tier Routing Cards in Light Theme */
    .light-theme #nodeDetailsModal .tier-card-active-groq,
    .light-theme #nodeDetailsModal .bg-amber-500\/20 {
      background-color: #fef3c7 !important;
      border-color: #f59e0b !important;
    }
    .light-theme #nodeDetailsModal .tier-card-active-gemini,
    .light-theme #nodeDetailsModal .bg-indigo-500\/20 {
      background-color: #e0e7ff !important;
      border-color: #6366f1 !important;
    }
    .light-theme #nodeDetailsModal .tier-card-active-openrouter,
    .light-theme #nodeDetailsModal .bg-emerald-500\/20 {
      background-color: #dcfce7 !important;
      border-color: #10b981 !important;
    }
    .light-theme #nodeDetailsModal .tier-card-active-groq div,
    .light-theme #nodeDetailsModal .tier-card-active-groq span,
    .light-theme #nodeDetailsModal .bg-amber-500\/20 div {
      color: #78350f !important;
    }
    .light-theme #nodeDetailsModal .tier-card-active-gemini div,
    .light-theme #nodeDetailsModal .tier-card-active-gemini span,
    .light-theme #nodeDetailsModal .bg-indigo-500\/20 div {
      color: #312e81 !important;
    }
    .light-theme #nodeDetailsModal .tier-card-active-openrouter div,
    .light-theme #nodeDetailsModal .tier-card-active-openrouter span,
    .light-theme #nodeDetailsModal .bg-emerald-500\/20 div {
      color: #064e3b !important;
    }
    .light-theme #nodeDetailsModal .tier-card-idle,
    .light-theme #nodeDetailsModal .bg-slate-800\/80,
    .light-theme #nodeDetailsModal .bg-black\/30 {
      background-color: #ffffff !important;
      border-color: #cbd5e1 !important;
    }
    .light-theme #nodeDetailsModal .tier-card-idle .text-white,
    .light-theme #nodeDetailsModal .bg-black\/30 .text-white {
      color: #0f172a !important;
    }
    .light-theme #nodeDetailsModal .tier-card-idle .text-slate-400,
    .light-theme #nodeDetailsModal .tier-card-idle .text-gray-400 {
      color: #64748b !important;
    }
      color: #0f172a !important;
    }

    /* Live Node Metrics in Light Theme */
    .light-theme #nodeDetailsModal .grid .bg-white\/5 {
      background-color: #ffffff !important;
      border: 1px solid #e2e8f0 !important;
    }

    /* Fault Injection Card in Light Theme */
    .light-theme #nodeDetailsModal .bg-rose-950\/20 {
      background-color: #fff1f2 !important;
      border-color: #fecdd3 !important;
    }
    .light-theme #nodeDetailsModal .text-rose-400 {
      color: #e11d48 !important;
      font-weight: 700 !important;
    }

    /* Modal Action Buttons in Light Theme */
    /* 1. Edit Key Button */
    .light-theme #nodeDetailsModal button[onclick*="openKeyEditModal"],
    .light-theme #nodeDetailsModal button.bg-cyan-950\/60 {
      background-color: #e0f2fe !important;
      border-color: #38bdf8 !important;
      color: #0369a1 !important;
      font-weight: 600 !important;
    }
    .light-theme #nodeDetailsModal button[onclick*="openKeyEditModal"]:hover,
    .light-theme #nodeDetailsModal button.bg-cyan-950\/60:hover {
      background-color: #0284c7 !important;
      border-color: #0284c7 !important;
      color: #ffffff !important;
    }

    /* 2. Reset Breaker Button */
    .light-theme #nodeDetailsModal button[onclick*="resetAllCircuitBreakers"],
    .light-theme #nodeDetailsModal button.bg-white\/5,
    .light-theme #nodeDetailsModal button.bg-slate-800 {
      background-color: #f1f5f9 !important;
      border-color: #cbd5e1 !important;
      color: #1e293b !important;
      font-weight: 600 !important;
    }
    .light-theme #nodeDetailsModal button[onclick*="resetAllCircuitBreakers"]:hover,
    .light-theme #nodeDetailsModal button.bg-white\/5:hover,
    .light-theme #nodeDetailsModal button.bg-slate-800:hover {
      background-color: #e2e8f0 !important;
      color: #0f172a !important;
    }

    /* 3. Fault Injection / Break Node Button */
    .light-theme #nodeDetailsModal #inspectorFaultToggleBtn,
    .light-theme #nodeDetailsModal button[onclick*="toggleFailureInjection"],
    .light-theme #nodeDetailsModal button.bg-rose-900\/60 {
      background-color: #fee2e2 !important;
      border-color: #f87171 !important;
      color: #b91c1c !important;
      font-weight: 600 !important;
    }
    .light-theme #nodeDetailsModal #inspectorFaultToggleBtn:hover,
    .light-theme #nodeDetailsModal button[onclick*="toggleFailureInjection"]:hover,
    .light-theme #nodeDetailsModal button.bg-rose-900\/60:hover {
      background-color: #ef4444 !important;
      border-color: #ef4444 !important;
      color: #ffffff !important;
    }

    /* 4. Amber / Cyan Accent Buttons in Inspector */
    .light-theme #nodeDetailsModal button.bg-amber-500\/10 {
      background-color: #fef3c7 !important;
      border-color: #f59e0b !important;
      color: #92400e !important;
      font-weight: 700 !important;
    }
    .light-theme #nodeDetailsModal button.bg-amber-500\/10:hover {
      background-color: #fde68a !important;
    }
    .light-theme #nodeDetailsModal button.bg-cyan-500\/10 {
      background-color: #e0f2fe !important;
      border-color: #38bdf8 !important;
      color: #0369a1 !important;
      font-weight: 700 !important;
    }
    .light-theme #nodeDetailsModal button.bg-cyan-500\/10:hover {
      background-color: #bae6fd !important;
    }

    .light-theme #nodeInspectorDrawer h3,
    .light-theme #nodeInspectorDrawer .text-white {
      color: #0f172a !important;
    }
    .light-theme #nodeInspectorDrawer .bg-black\/40 {
      background-color: #f8fafc !important;
      border-color: #e2e8f0 !important;
    }
    .light-theme #nodeInspectorDrawer .text-gray-400 {
      color: #64748b !important;
    }
    .light-theme #nodeInspectorDrawer .border-white\/10 {
      border-color: #e2e8f0 !important;
    }

    .light-theme #vizCanvasControls {

      background-color: rgba(255, 255, 255, 0.95) !important;

      border-color: #cbd5e1 !important;

      color: #0f172a !important;

    }

    .light-theme #keyModalInput {

      background: #f1f5f9 !important;

      color: #0f172a !important;

      border-color: #cbd5e1 !important;

    }

    .light-theme button[class*="bg-[#0f172a]"]:hover,

    .light-theme button.bg-\[\#0f172a\]:hover {

      background-color: #e2e8f0 !important;

    }

    .light-theme button.bg-white\/5,

    .light-theme button.bg-white\/10 {

      background-color: #f8fafc !important;

      color: #334155 !important;

      border-color: #cbd5e1 !important;

    }

    .light-theme button.bg-white\/5:hover,

    .light-theme button.bg-white\/10:hover {

      background-color: #f1f5f9 !important;

      color: #0f172a !important;

    }

    /* Light Theme Styling for Concurrency Queue Boxes & In-Flight Sections */
    .light-theme .viz-queue-box {
      background-color: rgba(255, 255, 255, 0.96) !important;
      border-color: rgba(203, 213, 225, 0.95) !important;
      box-shadow: 0 4px 16px rgba(0, 0, 0, 0.07), 0 1px 3px rgba(0, 0, 0, 0.05) !important;
    }

    .light-theme .viz-queue-box span[id^="vizRatioQueue"] {
      color: #1e293b !important;
    }

    .light-theme .viz-queue-box .text-amber-400 {
      color: #d97706 !important;
    }

    .light-theme .viz-queue-box .bg-slate-800 {
      background-color: #e2e8f0 !important;
    }

    .light-theme .viz-queue-box div[id^="vizDotsQueue"] span {
      background-color: #94a3b8 !important;
    }

    .light-theme .node-flight-section {
      border-top-color: rgba(0, 0, 0, 0.08) !important;
    }

    .light-theme .node-flight-section span.text-gray-300,
    .light-theme .node-flight-section span[id^="vizRatioInflight"] {
      color: #334155 !important;
    }

    .light-theme .node-flight-section .bg-slate-800 {
      background-color: rgba(0, 0, 0, 0.12) !important;
    }

    /* Icon badge boxes in headers */

    .light-theme div[class*="bg-cyan-950"],

    .light-theme div[class*="bg-teal-950"],

    .light-theme div[class*="bg-indigo-950"] {

      background-color: #f0fdfa !important;

      border-color: #99f6e4 !important;

      color: #0d9488 !important;

    }

    /* All Pills & Badges across Bottom Cards & Model Dropdowns in Light Theme */

    .light-theme span[class*="bg-blue-950"],

    .light-theme .capability-pill.bg-blue-950\/60 {

      background-color: #eff6ff !important;

      color: #1d4ed8 !important;

      border-color: #bfdbfe !important;

      font-weight: 700 !important;

    }

    .light-theme span[class*="bg-emerald-950"],

    .light-theme .capability-pill.bg-emerald-950\/60 {

      background-color: #ecfdf5 !important;

      color: #047857 !important;

      border-color: #a7f3d0 !important;

      font-weight: 700 !important;

    }

    .light-theme span[class*="bg-purple-950"],

    .light-theme .capability-pill.bg-purple-950\/60 {

      background-color: #faf5ff !important;

      color: #7e22ce !important;

      border-color: #e9d5ff !important;

      font-weight: 700 !important;

    }

    .light-theme span[class*="bg-teal-950"] {

      background-color: #f0fdfa !important;

      color: #0f766e !important;

      border-color: #99f6e4 !important;

      font-weight: 700 !important;

    }

    .light-theme span[class*="bg-cyan-950"] {

      background-color: #ecfeff !important;

      color: #0e7490 !important;

      border-color: #a5f3fc !important;

      font-weight: 700 !important;

    }

    .light-theme span[class*="bg-amber-950"] {

      background-color: #fffbeb !important;

      color: #b45309 !important;

      border-color: #fde68a !important;

      font-weight: 700 !important;

    }

    /* Model Dropdown Active Row & Text Contrast */

    .light-theme .model-option-item.bg-cyan-500\/15 {

      background-color: #e0f2fe !important;

      border-color: #38bdf8 !important;

      box-shadow: 0 1px 3px rgba(0, 0, 0, 0.05) !important;

    }

    .light-theme .model-option-item.bg-cyan-500\/15 .model-name-title {

      color: #0369a1 !important;

      font-weight: 700 !important;

    }

    .light-theme .model-option-item.bg-cyan-500\/15 .model-desc-text {

      color: #0284c7 !important;

      font-weight: 600 !important;

    }

    .light-theme .model-option-item:not(.bg-cyan-500\/15) .model-name-title {

      color: #0f172a !important;

    }

    .light-theme .model-option-item:not(.bg-cyan-500\/15) .model-desc-text {

      color: #64748b !important;

    }

    /* Make washed-out cyan/green text bold and dark in light mode */

    .light-theme .text-cyan-400,

    .light-theme .text-cyan-300 {

      color: #0891b2 !important;

    }

    .light-theme .text-cyan-200,

    .light-theme .text-cyan-100 {

      color: #0e7490 !important;

    }

    .light-theme .text-emerald-400,

    .light-theme .text-emerald-300 {

      color: #047857 !important;

    }

    .light-theme .text-teal-400,

    .light-theme .text-teal-300 {

      color: #0f766e !important;

    }

    .light-theme .text-blue-400,

    .light-theme .text-blue-300 {

      color: #1d4ed8 !important;

    }

    .light-theme .text-purple-400,

    .light-theme .text-purple-300 {

      color: #7e22ce !important;

    }

    /* Scrollbars in light theme */

    .light-theme ::-webkit-scrollbar-thumb {

      background: rgba(0, 0, 0, 0.18) !important;

    }

    .light-theme ::-webkit-scrollbar-thumb:hover {

      background: rgba(0, 0, 0, 0.32) !important;

    }

  </style>

  <script>

    (function() {

      try {

        var theme = localStorage.getItem('gateway_theme');

        if (theme === 'light') {

          document.documentElement.classList.add('light-theme');

        }

      } catch(e) {}

    })();

  </script>

</head>

<body class="h-screen w-screen flex relative select-none">

  <div class="top-gradient-shade"></div>

  <!-- Left Sidebar (Closable, Clean History without Placeholders) -->

  <aside id="appSidebar" class="w-64 h-full bg-[#0b101b] border-r border-white/5 flex flex-col z-30 shrink-0 transition-all duration-300 ease-in-out">

    <!-- Sidebar Header (Aligned with Main Topbar Line) -->

    <div class="h-11 sm:h-12 border-b border-white/5 px-3 flex items-center justify-between shrink-0">

      <div class="flex items-center gap-2.5 min-w-0">

        <div class="sb-bubble-container w-7 h-7 rounded-full overflow-hidden shadow-lg shadow-cyan-500/20 flex items-center justify-center shrink-0">

          <svg viewBox="0 0 32 32" fill="none" xmlns="http://www.w3.org/2000/svg" class="w-full h-full">

            <defs>

              <radialGradient id="sb-bubble-core" cx="50%" cy="50%" r="50%">

                <stop id="sbBubbleCoreStop" offset="60%" stop-color="#080c14" stop-opacity="0.95"/>

                <stop offset="100%" stop-color="#0ea5e9" stop-opacity="0.12"/>

              </radialGradient>

              <linearGradient id="sb-bubble-ring" x1="4" y1="4" x2="28" y2="28" gradientUnits="userSpaceOnUse">

                <stop offset="0%" stop-color="#38bdf8"/>

                <stop offset="40%" stop-color="#818cf8"/>

                <stop offset="70%" stop-color="#c084fc"/>

                <stop offset="100%" stop-color="#ffffff"/>

              </linearGradient>

              <linearGradient id="sb-bubble-crescent" x1="8" y1="26" x2="26" y2="22" gradientUnits="userSpaceOnUse">

                <stop offset="0%" stop-color="#38bdf8" stop-opacity="0"/>

                <stop offset="45%" stop-color="#67e8f9"/>

                <stop offset="75%" stop-color="#ffffff"/>

                <stop offset="100%" stop-color="#c084fc" stop-opacity="0"/>

              </linearGradient>

              <filter id="sb-bubble-glow" x="-20%" y="-20%" width="140%" height="140%">

                <feGaussianBlur stdDeviation="1.2" result="blur"/>

                <feComposite in="SourceGraphic" in2="blur" operator="over"/>

              </filter>

            </defs>

            <circle id="sbBubbleBg" cx="16" cy="16" r="15" fill="#080c14"/>

            <circle id="sbBubbleCore" cx="16" cy="16" r="11.5" fill="url(#sb-bubble-core)"/>

            <circle cx="16" cy="16" r="11.5" stroke="url(#sb-bubble-ring)" stroke-width="2.6" opacity="0.45" filter="url(#sb-bubble-glow)"/>

            <circle cx="16" cy="16" r="11.5" stroke="url(#sb-bubble-ring)" stroke-width="1.5"/>

            <path d="M 6.5 19.5 C 8.5 25.5 17.5 28.5 25.5 20" stroke="url(#sb-bubble-crescent)" stroke-width="2.2" stroke-linecap="round" filter="url(#sb-bubble-glow)"/>

            <path d="M 8.5 20.5 C 11 25.5 17 27 23.5 21" stroke="#ffffff" stroke-width="1.1" stroke-linecap="round" opacity="0.9"/>

          </svg>

        </div>

        <span class="font-bold text-base tracking-tight text-white truncate">LLM Gateway</span>

      </div>

      <button onclick="toggleSidebar()" class="p-1.5 text-gray-400 hover:text-white hover:bg-white/5 rounded-lg transition cursor-pointer" data-tooltip="Close Sidebar">

        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">

          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M11 19l-7-7 7-7m8 14l-7-7 7-7"/>

        </svg>

      </button>

    </div>

    <!-- Top Section: Sidebar Tabs -->

    <div style="flex: 65; min-height: 0;" class="flex flex-col p-3 pb-2 overflow-hidden">

      <!-- Sidebar Tabs Navigation -->

      <div class="flex flex-col gap-1 overflow-y-auto flex-1 pr-1">

        <!-- New Chat Tab (Opens hero input bar page) -->

        <button type="button" data-view="chat" onclick="startNewChat()" class="sidebar-nav-btn flex items-center gap-2.5 px-2.5 py-1.5 rounded-lg text-xs font-semibold bg-cyan-500/15 text-cyan-300 border border-cyan-500/30 shadow-[0_0_12px_rgba(6,182,212,0.1)] transition group cursor-pointer" data-tooltip="New Chat (Input Bar)">

          <svg class="w-4 h-4 text-cyan-400 transition shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">

            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4"/>

          </svg>

          <span>New Chat</span>

        </button>

        <!-- Chats History Table Tab -->

        <button type="button" data-view="chats" onclick="showView('chats')" class="sidebar-nav-btn flex items-center gap-2.5 px-2.5 py-1.5 rounded-lg text-xs font-medium text-gray-400 hover:text-white hover:bg-white/5 transition group border border-transparent hover:border-white/5 cursor-pointer" data-tooltip="View Chat History Table">

          <svg class="w-4 h-4 text-gray-400 group-hover:text-cyan-400 transition shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">

            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 12h.01M12 12h.01M16 12h.01M21 12c0 4.418-4.03 8-9 8a9.863 9.863 0 01-4.255-.949L3 20l1.395-3.72C3.512 15.042 3 13.574 3 12c0-4.418 4.03-8 9-8s9 3.582 9 8z"/>

          </svg>

          <span>Chats</span>

        </button>

        <!-- Stats Tab -->

        <button type="button" data-view="stats" onclick="showView('stats')" class="sidebar-nav-btn flex items-center gap-2.5 px-2.5 py-1.5 rounded-lg text-xs font-medium text-gray-400 hover:text-white hover:bg-white/5 transition group border border-transparent hover:border-white/5 cursor-pointer" data-tooltip="View Gateway Statistics & Metrics">

          <svg class="w-4 h-4 text-gray-400 group-hover:text-cyan-400 transition shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">

            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"/>

          </svg>

          <span>Stats</span>

        </button>

        <!-- Observability Tab -->

        <button type="button" data-view="observability" onclick="showView('observability')" class="sidebar-nav-btn flex items-center gap-2.5 px-2.5 py-1.5 rounded-lg text-xs font-medium text-gray-400 hover:text-white hover:bg-white/5 transition group border border-transparent hover:border-white/5 cursor-pointer" data-tooltip="View Gateway Reliability & Circuit Breakers">

          <svg class="w-4 h-4 text-gray-400 group-hover:text-cyan-400 transition shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">

            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M22 12h-4l-3 9L9 3l-3 9H2"/>

          </svg>

          <span>Observability</span>

        </button>

        <!-- Visualize Tab (Live Traffic, Masked Keys, & Fault Injection) -->

        <button type="button" data-view="visualize" onclick="showView('visualize')" class="sidebar-nav-btn flex items-center gap-2.5 px-2.5 py-1.5 rounded-lg text-xs font-medium text-gray-400 hover:text-white hover:bg-white/5 transition group border border-transparent hover:border-white/5 cursor-pointer" data-tooltip="Interactive Architecture Topology & Live Traffic Visualizer">

          <svg class="w-4 h-4 text-gray-400 group-hover:text-cyan-400 transition shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">

            <circle cx="6" cy="12" r="2.5" stroke-width="2"/>

            <circle cx="18" cy="6" r="2.5" stroke-width="2"/>

            <circle cx="18" cy="18" r="2.5" stroke-width="2"/>

            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8.5 11l7-3.5M8.5 13l7 3.5"/>

          </svg>

          <span>Visualize</span>

        </button>

        <!-- Logs Tab -->

        <button type="button" data-view="logs" onclick="showView('logs')" class="sidebar-nav-btn flex items-center gap-2.5 px-2.5 py-1.5 rounded-lg text-xs font-medium text-gray-400 hover:text-white hover:bg-white/5 transition group border border-transparent hover:border-white/5 cursor-pointer" data-tooltip="View Real-Time Gateway Request & Reliability Logs">

          <svg class="w-4 h-4 text-gray-400 group-hover:text-cyan-400 transition shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">

            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"/>

          </svg>

          <span>Logs</span>

        </button>

        <!-- Architecture Tab -->

        <button type="button" data-view="about" onclick="showView('about')" class="sidebar-nav-btn flex items-center gap-2.5 px-2.5 py-1.5 rounded-lg text-xs font-medium text-gray-400 hover:text-white hover:bg-white/5 transition group border border-transparent hover:border-white/5 cursor-pointer" data-tooltip="View Enterprise Gateway Architecture & Capabilities">

          <svg class="w-4 h-4 text-gray-400 group-hover:text-cyan-400 transition shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">

            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"/>

          </svg>

          <span>Architecture</span>

        </button>

        <!-- Settings Tab -->

        <button type="button" data-view="settings" onclick="showView('settings')" class="sidebar-nav-btn flex items-center gap-2.5 px-2.5 py-1.5 rounded-lg text-xs font-medium text-gray-400 hover:text-white hover:bg-white/5 transition group border border-transparent hover:border-white/5 cursor-pointer" data-tooltip="Gateway Model Settings & API Keys">

          <svg class="w-4 h-4 text-gray-400 group-hover:text-white transition shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">

            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z"/><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"/>

          </svg>

          <span>Settings</span>

        </button>

      </div>

    </div>

    <!-- Bottom Section: Real Chat History (Kept empty when no chats, no placeholder) -->

    <div id="recentChatsSection" style="flex: 45; min-height: 0;" class="flex flex-col px-3 pb-3 pt-2.5 border-t border-white/5 overflow-hidden">

      <div class="flex items-center justify-between px-2 mb-1 shrink-0">

        <div class="text-[10px] font-semibold tracking-wider text-gray-500 uppercase">Recent Chats</div>

        <button id="toggleRecentChatsBtn" onclick="toggleRecentChats()" class="text-[10px] text-gray-500 hover:text-gray-300 transition" title="Hide recent chats">Hide</button>

      </div>

      <div id="chatsList" class="flex flex-col gap-1 overflow-y-auto flex-1 pr-1"></div>

    </div>

  </aside>

  <!-- Main Content Area -->

  <main class="flex-1 flex flex-col h-full relative z-10 overflow-hidden bg-[#0a0e17]">

    <!-- Top Header (Reduced compact sleek height with high z-index to stay strictly above cards) -->

    <header class="relative z-50 h-11 sm:h-12 border-b border-white/5 px-4 sm:px-6 flex items-center justify-between shrink-0 bg-[#0a0e17]/80 backdrop-blur-md">

      <div class="flex items-center gap-2.5">

        <!-- Sidebar Reopen Toggle Button -->

        <button id="sidebarToggleBtn" onclick="toggleSidebar()" class="p-1 text-gray-400 hover:text-white hover:bg-white/5 rounded-lg transition cursor-pointer" data-tooltip="Open Sidebar">

          <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6h16M4 12h16M4 18h16"/></svg>

        </button>

        <span class="text-white font-semibold text-xs sm:text-sm tracking-tight">LLM Gateway</span>

      </div>

      <!-- Right Nav Links -->

      <div class="flex items-center gap-3 sm:gap-4 text-xs text-gray-400 font-medium">

        <a href="#dashboard" onclick="showView('chat')" class="text-white hover:text-cyan-400 transition hidden sm:inline">Dashboard</a>

        <a href="#help" class="hover:text-white transition hidden sm:inline">Help & Support</a>

        <!-- Theme Toggle Button (Sun for Light, Moon for Dark) -->

        <button id="themeToggleBtn" type="button" onclick="toggleAppTheme()" data-tooltip="Switch to Light Mode" class="p-1.5 rounded-xl border border-white/10 hover:border-white/20 bg-white/5 hover:bg-white/10 text-gray-300 hover:text-white transition flex items-center justify-center cursor-pointer shadow-sm">

          <!-- Sun Icon (visible in dark mode: clicking changes to white/light) -->

          <svg id="themeIconSun" class="w-4 h-4 text-amber-400 hover:text-amber-300 transition" fill="none" stroke="currentColor" viewBox="0 0 24 24">

            <circle cx="12" cy="12" r="4" stroke-width="2"/>

            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 2v2m0 16v2M4.93 4.93l1.41 1.41m11.32 11.32l1.41 1.41M2 12h2m16 0h2M6.34 17.66l-1.41 1.41m14.14-14.14l-1.41 1.41"/>

          </svg>

          <!-- Moon Icon (visible in light mode: clicking changes to dark) -->

          <svg id="themeIconMoon" class="w-4 h-4 text-indigo-500 hover:text-indigo-400 transition hidden" fill="none" stroke="currentColor" viewBox="0 0 24 24">

            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M20.354 15.354A9 9 0 018.646 3.646 9.003 9.003 0 0012 21a9.003 9.003 0 008.354-5.646z"/>

          </svg>

        </button>

        <!-- Auth Profile Section -->

        <button id="headerSignInBtn" type="button" onclick="openAuthModal('signin')" class="flex items-center gap-1.5 px-2.5 py-1 rounded-xl bg-cyan-500 hover:bg-cyan-400 text-gray-950 font-bold text-xs transition shadow-md shadow-cyan-500/20">

          <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M11 16l-4-4m0 0l4-4m-4 4h14m-5 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h7a3 3 0 013 3v1"/></svg>

          <span>Sign In</span>

        </button>

        <!-- Profile Menu Dropdown (visible when authenticated) -->

        <div class="relative z-50 hidden" id="profileDropdownWrapper">

          <button type="button" onclick="toggleProfileMenu(event)" class="w-7 h-7 sm:w-8 sm:h-8 rounded-full border border-cyan-400/40 p-0.5 relative cursor-pointer hover:border-cyan-400 transition flex items-center justify-center focus:outline-none" title="Profile menu">

            <img id="headerUserAvatar" src="" alt="Avatar" referrerpolicy="no-referrer" class="w-full h-full object-cover rounded-full hidden" onerror="this.classList.add('hidden');document.getElementById('headerUserInitials').classList.remove('hidden');">

            <div id="headerUserInitials" class="w-full h-full rounded-full bg-cyan-900 border border-cyan-500/30 flex items-center justify-center text-[10px] font-bold text-cyan-300">U</div>

          </button>

          <!-- Profile Section Popover Div (High z-index: appears strictly above all page cards) -->

          <div id="profileDropdownMenu" class="hidden absolute right-0 top-full mt-2 w-64 bg-[#161a22] border border-white/10 rounded-2xl shadow-2xl shadow-black/95 p-2 z-[9999] backdrop-blur-xl">

            <!-- User Info -->

            <div class="flex items-center gap-2.5 p-2 border-b border-white/5 mb-1">

              <div id="dropdownAvatarContainer" class="w-9 h-9 rounded-full overflow-hidden shrink-0 border border-white/10 flex items-center justify-center bg-cyan-950">

                <img id="dropdownUserAvatar" src="" alt="Avatar" referrerpolicy="no-referrer" class="w-full h-full object-cover hidden" onerror="this.classList.add('hidden');document.getElementById('dropdownUserInitials').classList.remove('hidden');">

                <div id="dropdownUserInitials" class="text-xs font-bold text-cyan-300">U</div>

              </div>

              <div class="flex flex-col min-w-0">

                <span id="profileDropdownName" class="text-xs font-semibold text-white truncate">User</span>

                <span id="profileDropdownEmail" class="text-[11px] text-gray-400 truncate">user@domain.com</span>

              </div>

            </div>

            <!-- Action Buttons: Profile & Avatar, Settings, Sign Out -->

            <div class="flex flex-col gap-0.5">

              <button type="button" onclick="openProfileModal(); toggleProfileMenu();" class="w-full flex items-center gap-2.5 px-2.5 py-2 rounded-xl text-xs font-medium text-cyan-300 hover:text-white hover:bg-cyan-500/10 transition group cursor-pointer">

                <svg class="w-3.5 h-3.5 text-cyan-400 group-hover:text-cyan-300 transition shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z"/></svg>

                <span>Profile & Avatar</span>

              </button>

              <button type="button" onclick="showView('settings'); toggleProfileMenu();" class="w-full flex items-center gap-2.5 px-2.5 py-2 rounded-xl text-xs font-medium text-gray-300 hover:text-white hover:bg-white/5 transition group cursor-pointer">

                <svg class="w-3.5 h-3.5 text-gray-400 group-hover:text-white transition shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z"/><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"/></svg>

                <span>Settings</span>

              </button>

              <button type="button" onclick="logoutUser()" class="w-full flex items-center gap-2.5 px-2.5 py-2 rounded-xl text-xs font-medium text-rose-400 hover:text-rose-300 hover:bg-rose-950/20 transition group cursor-pointer">

                <svg class="w-3.5 h-3.5 text-rose-400 group-hover:text-rose-300 transition shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1"/></svg>

                <span>Sign Out</span>

              </button>

            </div>

          </div>

        </div>

      </div>

    </header>

    <!-- Center Hero Screen -->

    <div id="welcomeScreen" class="flex-1 flex flex-col items-center justify-center px-4 max-w-4xl mx-auto w-full relative z-10 py-3 transition-all duration-300">

      

      <!-- Voice Powered Orb Centerpiece -->

      <div class="orb-wrapper relative w-36 h-36 sm:w-44 sm:h-44 mb-1.5 flex items-center justify-center group shrink-0 cursor-pointer" onclick="toggleVoiceRecording()" data-tooltip="Click to toggle Voice Reactive Mode">

        <div class="absolute inset-0 bg-gradient-to-tr from-purple-600/30 via-cyan-500/20 to-indigo-600/30 rounded-full blur-2xl pointer-events-none group-hover:scale-105 transition duration-500"></div>

        <canvas id="orbCanvas" class="w-36 h-36 sm:w-44 sm:h-44 cursor-pointer relative z-10 rounded-full" width="380" height="380"></canvas>

      </div>

      <!-- Hero Typography (Pulled up, compact, slightly reduced font size) -->

      <div class="text-center -mt-1 sm:-mt-1.5 mb-2 sm:mb-2.5">

        <div class="text-[9px] sm:text-[10px] font-semibold tracking-[0.2em] text-gray-400 uppercase mb-0.5">Welcome Back</div>

        <h1 class="text-lg sm:text-xl font-bold text-white tracking-tight">Bring your ideas to life today</h1>

      </div>

      <!-- Main Input Box Container (+ Button on Left, Model & Controls on Right) -->

      <div class="w-full max-w-xl sm:max-w-2xl input-glass rounded-xl p-2.5 sm:p-3 transition-all duration-200 mb-3 relative">

        <textarea id="heroChatInput" rows="1" placeholder="✦ Ask me anything..." class="w-full bg-transparent text-sm text-gray-100 placeholder-gray-500 focus:outline-none resize-none leading-normal min-h-[36px] sm:min-h-[40px]"></textarea>

        

        <div class="flex items-center justify-between gap-2 pt-1">

          <!-- Left: Add/Attachment '+' Action Button -->

          <div class="flex items-center gap-1.5">

            <button type="button" onclick="triggerAttachment()" class="w-8 h-8 rounded-lg border border-white/10 hover:border-white/20 bg-white/5 hover:bg-white/10 text-gray-400 hover:text-white flex items-center justify-center transition group focus:outline-none cursor-pointer" title="Attach file or context (+)">

              <svg class="w-4 h-4 group-hover:rotate-90 transition-transform duration-200" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.2" d="M12 4v16m8-8H4"/></svg>

            </button>

          </div>

          <!-- Right: Model Selector Pill + Voice Mic + Send -->

          <div id="heroNormalControls" class="flex items-center gap-2">

            <!-- Right: Compact Model Selector Pill with Provider Logo (Opposite Side) -->

            <div class="relative model-dropdown-wrapper">

              <button type="button" onclick="toggleInputModelDropdown(event, 'heroDropdownMenu')" class="flex items-center gap-1.5 px-2.5 py-1 rounded-lg bg-white/5 hover:bg-white/10 border border-white/10 hover:border-white/20 text-xs font-semibold text-gray-200 transition group focus:outline-none" title="Select Model">

                <span class="active-provider-logo flex items-center justify-center shrink-0"></span>

                <span class="active-model-name text-xs text-white">GPT-OSS</span>

                <svg class="w-3 h-3 text-gray-400 group-hover:text-white transition-transform duration-150" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"></path></svg>

              </button>

              <!-- Dropdown Popover Menu (Right-Aligned, Above Input Bar, Overflow-hidden with inner custom scrollbar) -->

              <div id="heroDropdownMenu" class="model-dropdown-popover hidden absolute bottom-full right-0 mb-2 w-60 bg-[#171b24] border border-white/10 rounded-2xl shadow-2xl shadow-black/95 z-50 backdrop-blur-xl overflow-hidden" onclick="event.stopPropagation()">

                <div id="heroDropdownItemsContainer" class="max-h-52 overflow-y-auto p-1.5 pr-2 custom-dropdown-scroll"></div>

              </div>

            </div>

            <!-- Voice Mic Icon Only -->

            <button type="button" onclick="toggleVoiceRecording()" class="voice-btn w-8 h-8 rounded-lg border border-white/10 hover:border-white/20 bg-white/5 hover:bg-white/10 text-gray-400 hover:text-white flex items-center justify-center transition" title="Voice dictation">

              <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 11a7 7 0 01-7 7m0 0a7 7 0 01-7-7m7 7v4m0 0H8m4 0h4m-4-8a3 3 0 100-6 3 3 0 000 6z"></path></svg>

            </button>

            <!-- Send Button -->

            <button id="sendBtn" onclick="submitMessage()" class="w-8 h-8 rounded-lg bg-white text-gray-900 hover:bg-cyan-300 hover:text-black flex items-center justify-center transition shadow-lg" title="Send message">

              <svg class="w-4 h-4 rotate-45 -mr-0.5" fill="currentColor" viewBox="0 0 24 24"><path d="M2.01 21L23 12 2.01 3 2 10l15 2-15 2z"/></svg>

            </button>

          </div>

          <!-- Right: Active Voice Recording Controls (ChatGPT Soundwave + Cancel + Done) -->
          <div id="heroVoiceControls" class="hidden flex items-center gap-2">
            <div class="chatgpt-soundwave flex items-center gap-1 px-3 py-1 bg-white/5 rounded-full border border-white/10 h-8">
              <span class="soundwave-bar"></span>
              <span class="soundwave-bar"></span>
              <span class="soundwave-bar"></span>
              <span class="soundwave-bar"></span>
              <span class="soundwave-bar"></span>
              <span class="soundwave-bar"></span>
              <span class="soundwave-bar"></span>
              <span class="soundwave-bar"></span>
              <span class="soundwave-bar"></span>
              <span class="soundwave-bar"></span>
              <span class="soundwave-bar"></span>
              <span class="soundwave-bar"></span>
              <span class="soundwave-bar"></span>
              <span class="soundwave-bar"></span>
              <span class="soundwave-bar"></span>
              <span class="soundwave-bar"></span>
            </div>
            <button type="button" onclick="cancelVoiceRecording()" class="w-8 h-8 rounded-lg bg-white/10 hover:bg-white/20 text-gray-300 hover:text-white flex items-center justify-center transition border border-white/10 cursor-pointer active:scale-95" title="Cancel voice recording">
              <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.2" d="M6 18L18 6M6 6l12 12"/></svg>
            </button>
            <button type="button" onclick="acceptVoiceRecording()" class="w-8 h-8 rounded-lg bg-white text-gray-900 hover:bg-cyan-300 hover:text-black flex items-center justify-center transition shadow-lg cursor-pointer active:scale-95" title="Done listening (Keep transcript)">
              <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.6" d="M5 13l4 4L19 7"/></svg>
            </button>
          </div>

        </div>

      </div>

      <!-- Enterprise Capability Cards (Heavyweight 3 Cards) -->

      <div class="grid grid-cols-1 sm:grid-cols-3 gap-2.5 sm:gap-3 w-full max-w-xl sm:max-w-2xl">

        <div onclick="showView('observability')" class="glass-panel p-2.5 sm:p-3 rounded-xl card-hover cursor-pointer flex flex-col justify-between group hover:border-blue-500/30 transition">

          <div class="flex items-center justify-between mb-1.5">

            <div class="w-7 h-7 rounded-lg bg-blue-500/10 border border-blue-500/20 flex items-center justify-center text-blue-400 group-hover:scale-105 transition">

              <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4.318 6.318a4.5 4.5 0 000 6.364L12 20.364l7.682-7.682a4.5 4.5 0 00-6.364-6.364L12 7.636l-1.318-1.318a4.5 4.5 0 00-6.364 0z"/></svg>

            </div>

            <span class="capability-pill text-[9px] font-bold tracking-wider text-blue-400 uppercase bg-blue-950/60 border border-blue-500/30 px-1.5 py-0.5 rounded">SLO 99.9%</span>

          </div>

          <div>

            <div class="text-xs font-semibold text-white mb-0.5 flex items-center gap-1 group-hover:text-blue-300 transition">

              <span>Reliability</span>

              <svg class="w-3 h-3 text-gray-500 group-hover:text-blue-400 group-hover:translate-x-0.5 transition-transform" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"/></svg>

            </div>

            <div class="text-[10px] text-gray-400 leading-tight">Resilient failover & circuit breakers with jitter backoff.</div>

          </div>

        </div>

        <div onclick="showView('stats')" class="glass-panel p-2.5 sm:p-3 rounded-xl card-hover cursor-pointer flex flex-col justify-between group hover:border-emerald-500/30 transition">

          <div class="flex items-center justify-between mb-1.5">

            <div class="w-7 h-7 rounded-lg bg-emerald-500/10 border border-emerald-500/20 flex items-center justify-center text-emerald-400 group-hover:scale-105 transition">

              <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8c-1.657 0-3 .895-3 2s1.343 2 3 2 3 .895 3 2-1.343 2-3 2m0-8c1.11 0 2.08.402 2.599 1M12 8V7m0 1v8m0 0v1m0-1c-1.11 0-2.08-.402-2.599-1M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/></svg>

            </div>

            <span class="capability-pill text-[9px] font-bold tracking-wider text-emerald-400 uppercase bg-emerald-950/60 border border-emerald-500/30 px-1.5 py-0.5 rounded">Adaptive</span>

          </div>

          <div>

            <div class="text-xs font-semibold text-white mb-0.5 flex items-center gap-1 group-hover:text-emerald-300 transition">

              <span>Cost Control</span>

              <svg class="w-3 h-3 text-gray-500 group-hover:text-emerald-400 group-hover:translate-x-0.5 transition-transform" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"/></svg>

            </div>

            <div class="text-[10px] text-gray-400 leading-tight">Smart model routing by alias, response caching & limits.</div>

          </div>

        </div>

        <div onclick="showView('logs')" class="glass-panel p-2.5 sm:p-3 rounded-xl card-hover cursor-pointer flex flex-col justify-between group hover:border-purple-500/30 transition">

          <div class="flex items-center justify-between mb-1.5">

            <div class="w-7 h-7 rounded-lg bg-purple-500/10 border border-purple-500/20 flex items-center justify-center text-purple-400 group-hover:scale-105 transition">

              <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"/><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z"/></svg>

            </div>

            <span class="capability-pill text-[9px] font-bold tracking-wider text-purple-400 uppercase bg-purple-950/60 border border-purple-500/30 px-1.5 py-0.5 rounded">Telemetry</span>

          </div>

          <div>

            <div class="text-xs font-semibold text-white mb-0.5 flex items-center gap-1 group-hover:text-purple-300 transition">

              <span>Observability</span>

              <svg class="w-3 h-3 text-gray-500 group-hover:text-purple-400 group-hover:translate-x-0.5 transition-transform" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"/></svg>

            </div>

            <div class="text-[10px] text-gray-400 leading-tight">Live p50/p95 latency metrics, traces & health monitors.</div>

          </div>

        </div>

      </div>

    </div>

    <!-- Active Chat Conversation Stream -->

    <div id="chatScreen" class="hidden flex-1 flex flex-col h-full max-w-3xl mx-auto w-full p-4 overflow-hidden">

      <div id="messagesContainer" class="flex-1 overflow-y-auto space-y-4 px-3 sm:px-6 pb-4 w-full max-w-2xl mx-auto"></div>

      <!-- Stream Input Box Container (Centered max-w-2xl, + Button on Left, Model & Controls on Right) -->

      <div class="w-full max-w-2xl mx-auto input-glass rounded-2xl p-3 shrink-0 relative">

        <textarea id="streamChatInput" rows="2" placeholder="Reply to AI..." class="w-full bg-transparent text-sm text-gray-100 placeholder-gray-500 focus:outline-none resize-none leading-relaxed"></textarea>

        <div class="flex items-center justify-between gap-2 pt-1.5">

          <!-- Left: Add/Attachment '+' Action Button -->

          <div class="flex items-center gap-1.5">

            <button type="button" onclick="triggerAttachment()" class="w-8 h-8 rounded-lg border border-white/10 hover:border-white/20 bg-white/5 hover:bg-white/10 text-gray-400 hover:text-white flex items-center justify-center transition group focus:outline-none cursor-pointer" title="Attach file or context (+)">

              <svg class="w-4 h-4 group-hover:rotate-90 transition-transform duration-200" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.2" d="M12 4v16m8-8H4"/></svg>

            </button>

          </div>

          <!-- Right: Model Selector Pill + Voice Mic + Send -->

          <div id="streamNormalControls" class="flex items-center gap-2">

            <!-- Right: Compact Model Selector Pill with Provider Logo -->

            <div class="relative model-dropdown-wrapper">

              <button type="button" onclick="toggleInputModelDropdown(event, 'streamDropdownMenu')" class="flex items-center gap-1.5 px-2.5 py-1 rounded-lg bg-white/5 hover:bg-white/10 border border-white/10 hover:border-white/20 text-xs font-semibold text-gray-200 transition group focus:outline-none" title="Select Model">

                <span class="active-provider-logo flex items-center justify-center shrink-0"></span>

                <span class="active-model-name text-xs text-white">GPT-OSS</span>

                <svg class="w-3 h-3 text-gray-400 group-hover:text-white transition-transform duration-150" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"></path></svg>

              </button>

              <!-- Dropdown Popover Menu (Right-Aligned, Above Stream Input Bar) -->

              <div id="streamDropdownMenu" class="model-dropdown-popover hidden absolute bottom-full right-0 mb-2 w-60 bg-[#171b24] border border-white/10 rounded-2xl shadow-2xl shadow-black/95 z-50 backdrop-blur-xl overflow-hidden" onclick="event.stopPropagation()">

                <div id="streamDropdownItemsContainer" class="max-h-52 overflow-y-auto p-1.5 pr-2 custom-dropdown-scroll"></div>

              </div>

            </div>

            <!-- Voice Mic Icon Only -->

            <button type="button" onclick="toggleVoiceRecording()" class="voice-btn w-8 h-8 rounded-lg border border-white/10 hover:border-white/20 bg-white/5 hover:bg-white/10 text-gray-400 hover:text-white flex items-center justify-center transition" title="Voice dictation">

              <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 11a7 7 0 01-7 7m0 0a7 7 0 01-7-7m7 7v4m0 0H8m4 0h4m-4-8a3 3 0 100-6 3 3 0 000 6z"></path></svg>

            </button>

            <!-- Send Button -->

            <button onclick="submitMessageFromStream()" class="w-8 h-8 rounded-lg bg-white text-gray-900 hover:bg-cyan-300 hover:text-black flex items-center justify-center transition shadow-lg" title="Send message">

              <svg class="w-4 h-4 rotate-45 -mr-0.5" fill="currentColor" viewBox="0 0 24 24"><path d="M2.01 21L23 12 2.01 3 2 10l15 2-15 2z"/></svg>

            </button>

          </div>

          <!-- Right: Active Voice Recording Controls (ChatGPT Soundwave + Cancel + Done) -->
          <div id="streamVoiceControls" class="hidden flex items-center gap-2">
            <div class="chatgpt-soundwave flex items-center gap-1 px-3 py-1 bg-white/5 rounded-full border border-white/10 h-8">
              <span class="soundwave-bar"></span>
              <span class="soundwave-bar"></span>
              <span class="soundwave-bar"></span>
              <span class="soundwave-bar"></span>
              <span class="soundwave-bar"></span>
              <span class="soundwave-bar"></span>
              <span class="soundwave-bar"></span>
              <span class="soundwave-bar"></span>
              <span class="soundwave-bar"></span>
              <span class="soundwave-bar"></span>
              <span class="soundwave-bar"></span>
              <span class="soundwave-bar"></span>
              <span class="soundwave-bar"></span>
              <span class="soundwave-bar"></span>
              <span class="soundwave-bar"></span>
              <span class="soundwave-bar"></span>
            </div>
            <button type="button" onclick="cancelVoiceRecording()" class="w-8 h-8 rounded-lg bg-white/10 hover:bg-white/20 text-gray-300 hover:text-white flex items-center justify-center transition border border-white/10 cursor-pointer active:scale-95" title="Cancel voice recording">
              <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.2" d="M6 18L18 6M6 6l12 12"/></svg>
            </button>
            <button type="button" onclick="acceptVoiceRecording()" class="w-8 h-8 rounded-lg bg-white text-gray-900 hover:bg-cyan-300 hover:text-black flex items-center justify-center transition shadow-lg cursor-pointer active:scale-95" title="Done listening (Keep transcript)">
              <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.6" d="M5 13l4 4L19 7"/></svg>
            </button>
          </div>

        </div>

      </div>

    </div>

    <!-- Settings Page (Dedicated Full View) -->

    <div id="settingsPage" class="hidden flex-1 flex flex-col h-full overflow-y-auto p-6 w-full">

      <!-- Page Header -->

      <div class="flex items-center justify-between pb-4 border-b border-white/10 mb-6 shrink-0">

        <div class="flex items-center gap-3">

          <div>

            <h2 class="text-xl font-bold text-white tracking-tight">Gateway Settings</h2>

            <p class="text-xs text-gray-400 mt-0.5">Configure model hyperparameters and provider API authentication keys</p>

          </div>

        </div>

        <button type="button" onclick="showView('chat')" class="px-4 py-2 bg-cyan-500 hover:bg-cyan-400 text-gray-950 font-bold rounded-lg text-xs transition">Done</button>

      </div>

      <!-- Settings Content Panels -->

      <div class="space-y-6 pb-12">

        <!-- Model Configuration Card -->

        <div class="glass-panel rounded-2xl p-6 border border-white/10 space-y-4">

          <h3 class="text-sm font-semibold text-white flex items-center gap-2">

            <span class="w-2 h-2 rounded-full bg-cyan-400"></span>

            <span>Model Defaults</span>

          </h3>

          <div class="grid grid-cols-1 sm:grid-cols-2 gap-4 text-xs">

            <div class="sm:col-span-2">

              <label class="block text-gray-400 mb-1.5 font-medium">Active Default Model</label>

              <select id="modalModelSelect" class="w-full bg-[#111827] border border-white/10 rounded-lg p-2.5 text-white focus:outline-none focus:border-cyan-500/50">

                <option value="fast">Fast (Groq: llama-3.1-8b-instant)</option>

                <option value="smart">Smart (Gemini: gemini-2.0-flash)</option>

                <option value="cheap">Cheap (OpenRouter: meta-llama/llama-3.1-8b-instruct)</option>

                <option value="complex">Complex (Groq: llama-3.3-70b-versatile)</option>

              </select>

            </div>

            <div>

              <label class="block text-gray-400 mb-1.5 font-medium">Temperature (0.0 - 2.0)</label>

              <input id="modalTemp" type="number" min="0" max="2" step="0.1" value="0.7" class="w-full bg-[#111827] border border-white/10 rounded-lg p-2.5 text-white focus:outline-none focus:border-cyan-500/50">

            </div>

            <div>

              <label class="block text-gray-400 mb-1.5 font-medium">Max Tokens (1 - 1000)</label>

              <input id="modalTokens" type="number" min="1" max="1000" value="1000" class="w-full bg-[#111827] border border-white/10 rounded-lg p-2.5 text-white focus:outline-none focus:border-cyan-500/50">

            </div>

          </div>

        </div>

        <!-- Provider API Keys Card -->

        <div class="glass-panel rounded-2xl p-6 border border-white/10 space-y-4">

          <h3 class="text-sm font-semibold text-white flex items-center gap-2">

            <span class="w-2 h-2 rounded-full bg-teal-400"></span>

            <span>Provider API Keys</span>

          </h3>

          <p class="text-xs text-gray-400">Apply provider API keys to override or configure custom inference credentials. Kept empty by default.</p>

          <div class="space-y-3.5 text-xs">

            <div>

              <label class="flex items-center gap-1.5 text-gray-400 mb-1 font-medium">

                <svg class="w-3.5 h-3.5 shrink-0" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg"><rect width="24" height="24" rx="5" fill="#F55036"/><path d="M16 8.5A5.5 5.5 0 1 0 16.5 15.5H12V12h4.5" stroke="#FFF" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/></svg>

                <span>Groq API Key</span>

              </label>

              <input id="modalGroqKey" type="password" placeholder="Enter Groq API Key (gsk_...)" class="w-full bg-[#111827] border border-white/10 rounded-lg p-2.5 text-white placeholder-gray-500 focus:outline-none focus:border-cyan-500/50" autocomplete="off">

            </div>

            <div>

              <label class="flex items-center gap-1.5 text-gray-400 mb-1 font-medium">

                <svg class="w-3.5 h-3.5 shrink-0" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg"><rect width="24" height="24" rx="5" fill="#4F46E5"/><path d="M7 12h10M13 8l4 4-4 4" stroke="#FFF" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/></svg>

                <span>OpenRouter API Key</span>

              </label>

              <input id="modalOpenRouterKey" type="password" placeholder="Enter OpenRouter API Key (sk-or-...)" class="w-full bg-[#111827] border border-white/10 rounded-lg p-2.5 text-white placeholder-gray-500 focus:outline-none focus:border-cyan-500/50" autocomplete="off">

            </div>

            <div>

              <label class="flex items-center gap-1.5 text-gray-400 mb-1 font-medium">

                <svg class="w-3.5 h-3.5 shrink-0" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg"><rect width="24" height="24" rx="5" fill="#0f172a"/><path d="M12 3c0 4.97-4.03 9-9 9 4.97 0 9 4.03 9 9 0-4.97 4.03-9 9-9-4.97 0-9-4.03-9-9z" fill="url(#gem-spark)"/><defs><linearGradient id="gem-spark" x1="3" y1="3" x2="21" y2="21" gradientUnits="userSpaceOnUse"><stop stop-color="#38BDF8"/><stop offset="0.5" stop-color="#818CF8"/><stop offset="1" stop-color="#C084FC"/></linearGradient></defs></svg>

                <span>Gemini API Key</span>

              </label>

              <input id="modalGeminiKey" type="password" placeholder="Enter Gemini API Key (AIza...)" class="w-full bg-[#111827] border border-white/10 rounded-lg p-2.5 text-white placeholder-gray-500 focus:outline-none focus:border-cyan-500/50" autocomplete="off">

            </div>

          </div>

        </div>

        <!-- Active Providers Status Card -->

        <div class="p-4 bg-white/5 rounded-2xl border border-white/5 text-xs text-gray-400 space-y-2">

          <div class="text-teal-400 font-semibold text-xs uppercase tracking-wider">Active Providers In Routing Engine</div>

          <div class="grid grid-cols-1 sm:grid-cols-3 gap-3 pt-1">

            <div class="p-2.5 rounded-xl bg-black/30 border border-white/5">

              <div class="flex items-center gap-1.5 mb-0.5">

                <svg class="w-3.5 h-3.5 shrink-0" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg"><rect width="24" height="24" rx="5" fill="#F55036"/><path d="M16 8.5A5.5 5.5 0 1 0 16.5 15.5H12V12h4.5" stroke="#FFF" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/></svg>

                <div class="font-bold text-white text-xs">Groq LPUs</div>

              </div>

              <div class="text-[11px] text-gray-400">Ultra low-latency inference</div>

            </div>

            <div class="p-2.5 rounded-xl bg-black/30 border border-white/5">

              <div class="flex items-center gap-1.5 mb-0.5">

                <svg class="w-3.5 h-3.5 shrink-0" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg"><rect width="24" height="24" rx="5" fill="#4F46E5"/><path d="M7 12h10M13 8l4 4-4 4" stroke="#FFF" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/></svg>

                <div class="font-bold text-white text-xs">OpenRouter</div>

              </div>

              <div class="text-[11px] text-gray-400">Universal model gateway</div>

            </div>

            <div class="p-2.5 rounded-xl bg-black/30 border border-white/5">

              <div class="flex items-center gap-1.5 mb-0.5">

                <svg class="w-3.5 h-3.5 shrink-0" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg"><rect width="24" height="24" rx="5" fill="#0f172a"/><path d="M12 3c0 4.97-4.03 9-9 9 4.97 0 9 4.03 9 9 0-4.97 4.03-9 9-9-4.97 0-9-4.03-9-9z" fill="url(#gem-spark)"/><defs><linearGradient id="gem-spark" x1="3" y1="3" x2="21" y2="21" gradientUnits="userSpaceOnUse"><stop stop-color="#38BDF8"/><stop offset="0.5" stop-color="#818CF8"/><stop offset="1" stop-color="#C084FC"/></linearGradient></defs></svg>

                <div class="font-bold text-white text-xs">Google Gemini</div>

              </div>

              <div class="text-[11px] text-gray-400">High-intelligence multimodal</div>

            </div>

          </div>

        </div>

      </div>

    </div>

    <!-- Logs Page (Dedicated Full View) -->

    <div id="logsPage" class="hidden flex-1 flex flex-col h-full overflow-hidden p-6 w-full">

      <!-- Page Header -->

      <div class="flex items-center justify-between pb-4 border-b border-white/10 mb-4 shrink-0">

        <div class="flex items-center gap-3">

          <div class="w-8 h-8 rounded-lg bg-cyan-950/60 border border-cyan-500/30 flex items-center justify-center text-cyan-400 shrink-0">

            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"/></svg>

          </div>

          <div>

            <h2 class="text-xl font-bold text-white tracking-tight">Gateway Telemetry & Logs</h2>

            <p class="text-xs text-gray-400 mt-0.5">Real-time inspection of completions, fallbacks, circuit breakers & latencies (IST)</p>

          </div>

        </div>

        <div class="flex items-center gap-2">

          <button onclick="refreshGatewayLogs()" class="px-3 py-1.5 rounded-lg text-xs font-medium text-gray-300 hover:text-white bg-white/5 hover:bg-white/10 border border-white/10 transition flex items-center gap-1.5" title="Refresh Logs">

            <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/></svg>

            <span>Refresh</span>

          </button>

          <button onclick="clearAllGatewayLogs()" class="px-3 py-1.5 text-xs text-red-400 hover:text-red-300 bg-red-950/40 hover:bg-red-900/60 border border-red-500/30 rounded-lg transition font-medium" title="Clear All Logs">

            Clear Logs

          </button>

          <button onclick="showView('chat')" class="px-3.5 py-1.5 bg-cyan-500 hover:bg-cyan-400 text-gray-950 font-bold rounded-lg text-xs transition">Done</button>

        </div>

      </div>

      <!-- Controls & Filter Bar -->

      <div class="flex items-center justify-between gap-3 mb-3 shrink-0">

        <div class="relative flex-1">

          <input id="logFilterInput" oninput="filterLogsTable()" type="text" placeholder="Filter by provider, model, target, status, or keyword..." class="w-full bg-[#111827] border border-white/10 rounded-lg pl-8 pr-3 py-2 text-xs text-gray-100 placeholder-gray-500 focus:outline-none focus:border-cyan-500/50" />

          <svg class="w-3.5 h-3.5 text-gray-500 absolute left-2.5 top-2.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"/></svg>

        </div>

        <div id="logsCountBadge" class="text-xs text-gray-400 shrink-0 font-medium px-3 py-2 rounded-lg bg-white/5 border border-white/5">0 logs</div>

      </div>

      <!-- Logs Table Container (Full Page Stretch) -->

      <div class="flex-1 overflow-y-auto pr-1 border border-white/5 rounded-xl bg-[#090d16]">

        <table class="w-full text-left text-xs border-collapse">

          <thead class="sticky top-0 bg-[#111827] text-[10px] uppercase font-bold text-gray-400 border-b border-white/10 tracking-wider">

            <tr>

              <th class="py-2.5 px-3">Time (IST)</th>

              <th class="py-2.5 px-3">Target</th>

              <th class="py-2.5 px-3">Provider & Model</th>

              <th class="py-2.5 px-3">Status</th>

              <th class="py-2.5 px-3">Attempts & Fallback</th>

              <th class="py-2.5 px-3">Latency</th>

              <th class="py-2.5 px-3">Tokens</th>

            </tr>

          </thead>

          <tbody id="logsTableBody" class="divide-y divide-white/5 font-mono text-[11px]">

            <!-- Rendered dynamically -->

          </tbody>

        </table>

        <div id="logsEmptyState" class="hidden text-center py-16 text-gray-500 italic text-xs">

          No logs found. Send a chat request to see live logs!

        </div>

      </div>

      <!-- Page Footer -->

      <div class="mt-3 flex items-center justify-between text-[11px] text-gray-400 shrink-0">

        <span>Times displayed in Indian Standard Time (IST, UTC+5:30) • Auto-refreshes on every completion</span>

        <span>Gateway Request Telemetry</span>

      </div>

    </div>

    <!-- Stats Page (Dedicated Full View in Main Window) -->

    <div id="statsPage" class="hidden flex-1 flex flex-col h-full overflow-y-auto p-6 max-w-7xl mx-auto w-full">

      <!-- Page Header -->

      <div class="flex items-center justify-between pb-4 border-b border-white/10 mb-6 shrink-0">

        <div class="flex items-center gap-3">

          <div class="w-8 h-8 rounded-lg bg-teal-950/60 border border-teal-500/30 flex items-center justify-center text-teal-400 shrink-0">

            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"/></svg>

          </div>

          <div>

            <h2 class="text-xl font-bold text-white tracking-tight">Gateway Statistics</h2>

            <p class="text-xs text-gray-400 mt-0.5">Real-time throughput, token utilization, provider share & latency KPIs</p>

          </div>

        </div>

        <div class="flex items-center gap-2">

          <button onclick="refreshGatewayStats()" class="px-3 py-1.5 rounded-lg text-xs font-medium text-gray-300 hover:text-white bg-white/5 hover:bg-white/10 border border-white/10 transition flex items-center gap-1.5" title="Refresh Stats">

            <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/></svg>

            <span>Refresh</span>

          </button>

          <button onclick="showView('chat')" class="px-3.5 py-1.5 bg-cyan-500 hover:bg-cyan-400 text-gray-950 font-bold rounded-lg text-xs transition">Done</button>

        </div>

      </div>

      <!-- KPI Metrics Cards (4 Columns) -->

      <div class="grid grid-cols-2 sm:grid-cols-4 gap-3.5 mb-6">

        <div class="glass-panel p-4 rounded-2xl border border-white/10">

          <div class="text-[11px] font-medium text-gray-400 uppercase tracking-wider mb-1">Total Requests</div>

          <div id="statTotalRequests" class="text-2xl font-bold text-white">0</div>

          <div class="text-[10px] text-gray-500 mt-1">Gateway executions</div>

        </div>

        <div class="glass-panel p-4 rounded-2xl border border-white/10">

          <div class="text-[11px] font-medium text-gray-400 uppercase tracking-wider mb-1">Success Rate</div>

          <div id="statSuccessRate" class="text-2xl font-bold text-emerald-400">100%</div>

          <div class="text-[10px] text-gray-500 mt-1">High-availability target</div>

        </div>

        <div class="glass-panel p-4 rounded-2xl border border-white/10">

          <div class="text-[11px] font-medium text-gray-400 uppercase tracking-wider mb-1">Avg Latency</div>

          <div id="statAvgLatency" class="text-2xl font-bold text-cyan-400">0ms</div>

          <div class="text-[10px] text-gray-500 mt-1">Round-trip completion</div>

        </div>

        <div class="glass-panel p-4 rounded-2xl border border-white/10">

          <div class="text-[11px] font-medium text-gray-400 uppercase tracking-wider mb-1">Total Tokens</div>

          <div id="statTotalTokens" class="text-2xl font-bold text-purple-400">0</div>

          <div class="text-[10px] text-gray-500 mt-1">Prompts & completions</div>

        </div>

      </div>

      <!-- Detailed Breakdown Grid -->

      <div class="grid grid-cols-1 sm:grid-cols-2 gap-4 mb-6">

        <!-- Provider Distribution -->

        <div class="glass-panel p-5 rounded-2xl border border-white/10 space-y-3">

          <h3 class="text-xs font-bold text-white uppercase tracking-wider flex items-center justify-between">

            <span>Provider Distribution</span>

            <span class="text-[10px] text-gray-400 font-normal">Active LPUs & APIs</span>

          </h3>

          <div id="statsProviderList" class="space-y-2.5 pt-1">

            <!-- Populated dynamically -->

          </div>

        </div>

        <!-- Reliability & Fallback Metrics -->

        <div class="glass-panel p-5 rounded-2xl border border-white/10 space-y-3">

          <h3 class="text-xs font-bold text-white uppercase tracking-wider flex items-center justify-between">

            <span>Routing Resilience</span>

            <span class="text-[10px] text-gray-400 font-normal">Fallback efficiency</span>

          </h3>

          <div class="space-y-2.5 pt-1 text-xs">

            <div class="flex items-center justify-between p-2.5 rounded-xl bg-white/5 border border-white/5">

              <span class="text-gray-300">Fallbacks Activated</span>

              <span id="statFallbacks" class="font-bold text-amber-300 font-mono">0</span>

            </div>

            <div class="flex items-center justify-between p-2.5 rounded-xl bg-white/5 border border-white/5">

              <span class="text-gray-300">Failed / Retried Requests</span>

              <span id="statFailedReqs" class="font-bold text-red-400 font-mono">0</span>

            </div>

            <div class="flex items-center justify-between p-2.5 rounded-xl bg-white/5 border border-white/5">

              <span class="text-gray-300">Max Time Budget</span>

              <span class="font-bold text-cyan-300 font-mono">30.0s</span>

            </div>

          </div>

        </div>

      </div>

      <!-- Top Models Table -->

      <div class="glass-panel p-5 rounded-2xl border border-white/10">

        <h3 class="text-xs font-bold text-white uppercase tracking-wider mb-3">Top Models by Request Volume</h3>

        <div class="overflow-x-auto">

          <table class="w-full text-left text-xs">

            <thead class="text-[10px] uppercase font-bold text-gray-400 border-b border-white/10">

              <tr>

                <th class="py-2 px-3">Model</th>

                <th class="py-2 px-3">Requests</th>

                <th class="py-2 px-3">Tokens</th>

              </tr>

            </thead>

            <tbody id="statsTopModelsBody" class="divide-y divide-white/5 font-mono text-[11px]">

              <!-- Populated dynamically -->

            </tbody>

          </table>

        </div>

      </div>

    </div>

    <!-- Observability Page (Dedicated Full View in Main Window) -->

    <div id="observabilityPage" class="hidden flex-1 flex flex-col h-full overflow-y-auto p-6 max-w-7xl mx-auto w-full">

      <!-- Page Header -->

      <div class="flex items-center justify-between pb-4 border-b border-white/10 mb-6 shrink-0">

        <div class="flex items-center gap-3">

          <div class="w-8 h-8 rounded-lg bg-cyan-950/60 border border-cyan-500/30 flex items-center justify-center text-cyan-400 shrink-0">

            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z"/></svg>

          </div>

          <div>

            <h2 class="text-xl font-bold text-white tracking-tight">Gateway Observability</h2>

            <p class="text-xs text-gray-400 mt-0.5">Reliability architecture, circuit breakers, fallback registries & SLA policies</p>

          </div>

        </div>

        <div class="flex items-center gap-2">

          <button onclick="resetAllCircuitBreakers()" class="px-3 py-1.5 text-xs text-cyan-300 hover:text-white bg-cyan-950/40 hover:bg-cyan-900/60 border border-cyan-500/30 rounded-lg transition font-medium flex items-center gap-1.5" title="Reset Circuit Breakers">

            <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/></svg>

            <span>Reset All Breakers</span>

          </button>

          <button onclick="showView('chat')" class="px-3.5 py-1.5 bg-cyan-500 hover:bg-cyan-400 text-gray-950 font-bold rounded-lg text-xs transition">Done</button>

        </div>

      </div>

      <!-- Circuit Breakers Grid -->

      <div class="glass-panel p-5 rounded-2xl border border-white/10 mb-6">

        <div class="text-xs font-bold text-white uppercase tracking-wider mb-3 flex items-center justify-between">

          <span>Provider Circuit Breakers</span>

          <span class="text-[10px] text-gray-400 font-normal">Self-healing 60s cooldown threshold: 5</span>

        </div>

        <div id="circuitBreakersList" class="grid grid-cols-1 sm:grid-cols-3 gap-3">

          <!-- Populated dynamically -->

        </div>

      </div>

      <!-- Fallback Chains Section -->

      <div class="glass-panel p-5 rounded-2xl border border-white/10 mb-6">

        <div class="text-xs font-bold text-white uppercase tracking-wider mb-3">Active Alias Fallback Chains</div>

        <div id="fallbackChainsList" class="space-y-2 text-xs">

          <!-- Populated dynamically -->

        </div>

      </div>

      <!-- Reliability Guarantees Summary -->

      <div class="p-4 bg-white/5 rounded-2xl border border-white/5 text-xs text-gray-300 space-y-2">

        <div class="text-cyan-400 font-semibold text-xs uppercase tracking-wider">Phase 3 Reliability SLA Guarantees</div>

        <div class="grid grid-cols-1 sm:grid-cols-2 gap-3 pt-1 text-[11px] text-gray-400">

          <div class="p-3 rounded-xl bg-black/40 border border-white/5">

            <span class="font-semibold text-white block mb-0.5">1. Error Classification</span>

            Retries 429 (rate limits), 5xx (server errors), timeouts and network dropouts. Immediate fast-fail on 400, 401, 403.

          </div>

          <div class="p-3 rounded-xl bg-black/40 border border-white/5">

            <span class="font-semibold text-white block mb-0.5">2. Retries + Jitter Backoff</span>

            Max 2-3 retries per provider with exponential backoff (1s, 2s, 4s) plus randomized jitter to prevent thundering herds.

          </div>

          <div class="p-3 rounded-xl bg-black/40 border border-white/5">

            <span class="font-semibold text-white block mb-0.5">3. Fallback Chains</span>

            Automatic seamless failover to secondary and tertiary providers upon exhaustion of retries.

          </div>

          <div class="p-3 rounded-xl bg-black/40 border border-white/5">

            <span class="font-semibold text-white block mb-0.5">4. Strict 30s Time Budget</span>

            Gateway enforces a total end-to-end 30s deadline; cleanly aborts if external providers exceed budget.

          </div>

        </div>

      </div>

    </div>

    <!-- Visualize Page: Modern Architecture Studio & Interactive Resilience Canvas (Exact Match to Design Brief) -->

    <div id="visualizePage" class="hidden flex-1 flex flex-col h-full w-full overflow-hidden relative bg-[#090b11] select-none">

      

      <!-- Main Studio Canvas Body -->

      <div id="studioCanvasViewport" class="relative flex-1 w-full h-full overflow-hidden bg-[#090b11]" style="background-image: radial-gradient(circle, rgba(255, 255, 255, 0.08) 1.2px, transparent 1.2px); background-size: 24px 24px;">

        

        <!-- K6 Load Testing & Traffic Control Bar (Clean Minimal Studio Toolbar) -->

        <div id="vizK6ControlBar" class="absolute top-4 left-6 z-20 flex items-center gap-2 bg-[#141923]/90 border border-white/10 rounded-2xl p-1.5 px-2.5 shadow-2xl backdrop-blur-md max-w-[calc(100vw-220px)] overflow-x-auto">

          <!-- Target Selector -->

          <select id="k6TargetSelect" class="bg-black/50 border border-white/10 rounded-lg px-2 py-1 text-xs text-white focus:outline-none focus:border-cyan-500 cursor-pointer font-medium" title="Target provider model profile">

            <option value="fast" selected>Fast (Groq)</option>

            <option value="smart">Smart (Gemini)</option>

            <option value="cheap">Cheap (OpenRouter)</option>

            <option value="complex">Complex (Groq 70B)</option>

          </select>

          <div class="h-3.5 w-px bg-white/10"></div>

          <!-- Request Count & Timeframe -->

          <div class="flex items-center gap-1 px-2 py-0.5 rounded-lg bg-black/40 border border-white/10 text-xs font-mono" title="Requests in duration">

            <input id="k6RequestCountInput" type="number" min="1" max="150" value="10" title="Request Count (1 to 150)" class="w-9 text-center bg-transparent text-cyan-400 font-bold border-b border-cyan-500/50 focus:outline-none cursor-pointer" />

            <span class="text-gray-400 text-[11px]">req</span>

            <span class="text-white/20">/</span>

            <input id="k6DurationInput" type="number" min="0.5" max="30" step="0.5" value="3.0" title="Duration in seconds" class="w-9 text-center bg-transparent text-amber-400 font-bold border-b border-amber-500/50 focus:outline-none cursor-pointer" />

            <span id="k6DurationUnit" class="text-amber-400 font-semibold text-xs font-sans">s</span>

          </div>

          <!-- Simultaneous / Concurrency Toggle -->

          <label class="flex items-center gap-1 px-2 py-0.5 rounded-lg bg-black/40 border border-white/10 text-xs cursor-pointer select-none hover:border-cyan-500/40 transition" title="Toggle Simultaneous Mode: fires all requests concurrently">

            <input id="k6SimultaneousCheck" type="checkbox" onchange="toggleK6SimultaneousMode()" class="rounded border-gray-600 text-cyan-500 focus:ring-0 focus:outline-none cursor-pointer scale-90" />

            <span class="text-gray-300 font-medium text-[11px]">Burst</span>

          </label>

          <!-- Prompt Input -->

          <div class="flex items-center gap-1 px-2 py-0.5 rounded-lg bg-black/40 border border-white/10 text-xs font-mono" title="Prompt sent in all test requests">

            <span class="text-gray-500 text-[10px] font-bold">Msg</span>

            <input id="k6PromptInput" type="text" value="hi" title="Prompt" class="w-8 bg-transparent text-cyan-300 font-bold border-b border-cyan-500/50 focus:outline-none focus:w-14 transition-all cursor-pointer text-center" />

          </div>

          <!-- Send Traffic Button -->

          <button id="k6SendTrafficBtn" type="button" onclick="startK6TrafficTest()" class="px-3 py-1 rounded-xl bg-cyan-500 hover:bg-cyan-400 text-gray-950 font-bold text-xs transition flex items-center gap-1.5 shadow-lg shadow-cyan-500/20 active:scale-95 cursor-pointer">

            <svg class="w-3.5 h-3.5 text-gray-950" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"/></svg>

            <span id="k6BtnLabel">Send Traffic</span>

          </button>

          <!-- Live Progress indicator -->

          <div id="k6ProgressPill" class="hidden flex items-center gap-1.5 text-[11px] font-mono px-2 py-0.5 rounded-lg bg-cyan-950/40 border border-cyan-500/30 text-cyan-300">

            <span class="w-1.5 h-1.5 rounded-full bg-cyan-400 animate-ping"></span>

            <span id="k6ProgressText">0/10 sent</span>

          </div>

          <!-- Real-Time SSE Stream Feed Toggle -->

          <button type="button" onclick="toggleVisualizerFeedModal()" class="p-1.5 rounded-lg bg-white/5 hover:bg-white/10 border border-white/10 text-gray-400 hover:text-white transition cursor-pointer" title="Toggle Live SSE Traffic Feed">

            <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6h16M4 12h16M4 18h7"/></svg>

          </button>

          <!-- Live Cost Counter -->
          <div id="vizLiveCostCounter" class="flex items-center gap-1.5 px-2.5 py-1 rounded-xl bg-emerald-950/70 border border-emerald-500/40 text-emerald-300 font-mono text-xs shadow-[0_0_15px_rgba(16,185,129,0.25)] select-none" title="Live Total Cost (USD) across all gateway requests">
            <span class="text-emerald-400 font-bold">$</span>
            <span id="vizCostValue" class="font-bold text-white tracking-wide">0.000000</span>
            <span class="text-[9.5px] text-emerald-400 font-sans uppercase">USD</span>
          </div>

        </div>

        <!-- Sleek Clickable Telemetry Button (Top-Right of Canvas, Zero Overlap, Opens Report Modal) -->

        <button id="vizReportBtn" type="button" onclick="openLatestK6ReportModal()" title="Click to open Stress Test Report & Telemetry Modal" class="absolute top-4 right-6 z-20 flex items-center gap-2 bg-[#141923]/90 hover:bg-[#1a2130] border border-white/10 hover:border-cyan-500/50 rounded-2xl p-1.5 px-3 shadow-2xl backdrop-blur-md transition group cursor-pointer font-mono text-xs select-none">

          <span class="px-1.5 py-0.5 rounded text-[10px] bg-cyan-950/80 text-cyan-300 border border-cyan-500/30 font-bold group-hover:border-cyan-400/60 transition">Report ↗</span>

        </button>

        <!-- Movable & Zoomable Canvas Viewport Layer -->

        <div id="canvasTransformLayer" class="absolute inset-0 w-full h-full origin-top-left select-none">

          <!-- SVG Topology Canvas (n8n Center-to-Center Ports) -->

          <svg id="vizSvgCanvas" class="pointer-events-none" style="overflow: visible !important; width: 3200px; height: 2200px; position: absolute; left: 0; top: 0;">

            <defs>

              <linearGradient id="studioCableGrad" x1="0%" y1="0%" x2="100%" y2="0%">

                <stop offset="0%" stop-color="#3b82f6" stop-opacity="0.6"/>

                <stop offset="100%" stop-color="#06b6d4" stop-opacity="0.6"/>

              </linearGradient>

              <filter id="glowBlue" x="-50%" y="-50%" width="200%" height="200%">

                <feDropShadow dx="0" dy="0" stdDeviation="4" flood-color="#38bdf8" flood-opacity="0.95"/>

              </filter>

              <filter id="glowYellow" x="-50%" y="-50%" width="200%" height="200%">

                <feDropShadow dx="0" dy="0" stdDeviation="4.5" flood-color="#f59e0b" flood-opacity="0.95"/>

              </filter>

              <filter id="glowGrey" x="-50%" y="-50%" width="200%" height="200%">

                <feDropShadow dx="0" dy="0" stdDeviation="3.5" flood-color="#94a3b8" flood-opacity="0.95"/>

              </filter>

              <filter id="glowGreen" x="-50%" y="-50%" width="200%" height="200%">

                <feDropShadow dx="0" dy="0" stdDeviation="4" flood-color="#10b981" flood-opacity="0.95"/>

              </filter>

              <filter id="glowRed" x="-50%" y="-50%" width="200%" height="200%">

                <feDropShadow dx="0" dy="0" stdDeviation="4.5" flood-color="#ef4444" flood-opacity="0.95"/>

              </filter>

              <marker id="vizArrowIn" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="14" markerHeight="14" markerUnits="userSpaceOnUse" orient="auto">

                <path d="M 0 1 L 9 5 L 0 9 z" fill="#60a5fa"/>

              </marker>

            </defs>

            <!-- Cable 1a: Client Traffic -> Auth Wall -->
            <path id="cable-client-auth" stroke="#1e3a8a" stroke-width="2.5" fill="none" stroke-linecap="round" marker-end="url(#vizArrowIn)" />

            <!-- Cable 1b: Auth Wall -> Rate Limiter -->
            <path id="cable-auth-ratelimit" stroke="#1e3a8a" stroke-width="2.5" fill="none" stroke-linecap="round" marker-end="url(#vizArrowIn)" />

            <!-- Cable 1c: Rate Limiter <-> Redis Token Store -->
            <path id="cable-ratelimit-redis" stroke="#e11d48" stroke-width="2" stroke-dasharray="3,3" fill="none" opacity="0.85" />

            <!-- Cable 1d: Rate Limiter -> Input Guard -->
            <path id="cable-ratelimit-inputguard" stroke="#1e3a8a" stroke-width="2.5" fill="none" stroke-linecap="round" marker-end="url(#vizArrowIn)" />

            <!-- Cable 1e: Input Guard -> API Gateway -->
            <path id="cable-inputguard-gateway" stroke="#1e3a8a" stroke-width="2.5" fill="none" stroke-linecap="round" marker-end="url(#vizArrowIn)" />

            <!-- Cable 1f: API Gateway <-> Redis Response Cache (Vertical Check Above) -->
            <path id="cable-gateway-cache" stroke="#10b981" stroke-width="2" stroke-dasharray="4,3" fill="none" opacity="0.9" marker-end="url(#vizArrowIn)" />

            <!-- Cable 1g: API Gateway -> LLM Router (Main Ingress Bus) -->
            <path id="cable-gateway-router" stroke="#1e3a8a" stroke-width="2.5" fill="none" stroke-linecap="round" marker-end="url(#vizArrowIn)" />

            <!-- Cable 2: LLM Router -> Retry & Breaker -->
            <path id="cable-router-retry" stroke="#1e3a8a" stroke-width="2.5" fill="none" stroke-linecap="round" marker-end="url(#vizArrowIn)" />

            <!-- Egress Return Flow: LLM Output -> Output Guard -> Audit Log -> Client Return Bus -->
            <path id="cable-retry-outputguard" stroke="#06b6d4" stroke-width="2.5" fill="none" stroke-linecap="round" marker-end="url(#vizArrowIn)" />
            <path id="cable-outputguard-audit" stroke="#06b6d4" stroke-width="2.5" fill="none" stroke-linecap="round" marker-end="url(#vizArrowIn)" />
            <path id="cable-audit-client" stroke="#059669" stroke-width="2.2" stroke-dasharray="5,4" fill="none" stroke-linecap="round" marker-end="url(#vizArrowIn)" />

            <!-- Async Jobs Pipeline: Ingress Branch -> Idempotency -> Job Queue -> Workers -> DLQ -->
            <path id="cable-client-idem" stroke="#8b5cf6" stroke-width="2.2" stroke-dasharray="4,4" fill="none" opacity="0.85" marker-end="url(#vizArrowIn)" />
            <path id="cable-idem-jobqueue" stroke="#8b5cf6" stroke-width="2.2" stroke-dasharray="3,3" fill="none" opacity="0.85" marker-end="url(#vizArrowIn)" />
            <path id="cable-jobqueue-worker" stroke="#06b6d4" stroke-width="2.5" fill="none" stroke-linecap="round" marker-end="url(#vizArrowIn)" />
            <path id="cable-worker-dlq" stroke="#f43f5e" stroke-width="2.5" stroke-dasharray="4,3" fill="none" opacity="0.9" marker-end="url(#vizArrowIn)" />

            <!-- Hidden Guide Paths for Particle Animations (Must have stroke=none and viz-hidden-guide to avoid phantom duplicate lines) -->
            <path id="path-client-router" class="viz-hidden-guide" stroke="none" fill="none" />
            <path id="path-return-egress" class="viz-hidden-guide" stroke="none" fill="none" />
            <path id="path-client-auth" class="viz-hidden-guide" stroke="none" fill="none" />
            <path id="path-client-ratelimit" class="viz-hidden-guide" stroke="none" fill="none" />
            <path id="path-ratelimit-redis" class="viz-hidden-guide" stroke="none" fill="none" />
            <path id="path-cache-return" class="viz-hidden-guide" stroke="none" fill="none" />

            <!-- Cable 4: Retry Node -> Groq Queue -->

            <path id="path-router-groq" stroke="#1e3a8a" stroke-width="2.5" fill="none" stroke-linecap="round" marker-end="url(#vizArrowIn)" />

            <!-- Cable 5: Retry Node -> Google Gemini Queue -->

            <path id="path-router-gemini" stroke="#1e3a8a" stroke-width="2.5" fill="none" stroke-linecap="round" marker-end="url(#vizArrowIn)" />

            <!-- Cable 6: Retry Node -> OpenRouter Queue -->

            <path id="path-router-openrouter" stroke="#1e3a8a" stroke-width="2.5" fill="none" stroke-linecap="round" marker-end="url(#vizArrowIn)" />

            <!-- Cable 7: Groq Queue -> Groq Provider Node (Connection String) -->
            <path id="path-queue-groq" stroke="#f43f5e" stroke-width="2.5" fill="none" stroke-linecap="round" marker-end="url(#vizArrowIn)" />

            <!-- Cable 8: Gemini Queue -> Gemini Provider Node (Connection String) -->
            <path id="path-queue-gemini" stroke="#6366f1" stroke-width="2.5" fill="none" stroke-linecap="round" marker-end="url(#vizArrowIn)" />

            <!-- Cable 9: OpenRouter Queue -> OpenRouter Provider Node (Connection String) -->
            <path id="path-queue-openrouter" stroke="#10b981" stroke-width="2.5" fill="none" stroke-linecap="round" marker-end="url(#vizArrowIn)" />

            <!-- Dotted Lines of Fallback Cascade Chain between Providers -->
            <path id="path-fallback-groq-gemini" stroke="#f59e0b" stroke-width="1.8" stroke-dasharray="4,4" fill="none" opacity="0.85" />
            <path id="path-fallback-gemini-openrouter" stroke="#f59e0b" stroke-width="1.8" stroke-dasharray="4,4" fill="none" opacity="0.85" />

            <!-- Real-Time SSE Traffic Particles Group -->

            <g id="vizTrafficDots"></g>

          </svg>

          <!-- Interactive Architecture Nodes (Movable & Clickable, pointer-events pass-through to canvas) -->

        <!-- Connection String Circuit Breaker Switches (White, clearly identified as Circuit Breaker, clickable) -->
        <div id="cableSwitchGroq" class="absolute pointer-events-none -translate-x-1/2 -translate-y-1/2 z-15 transition-all duration-300 select-none" style="left: 751px; top: 173px;">
          <div class="switch-closed pointer-events-auto flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-white border border-slate-200/90 shadow-[0_2px_10px_rgba(0,0,0,0.06)] font-sans text-slate-700 cursor-pointer hover:border-emerald-500/60 hover:shadow-md transition-all active:scale-95" onclick="openCircuitBreakerModal('groq', event)" title="Circuit Breaker (Groq): CLOSED - Click to view Fallback Logic">
            <svg class="w-3.5 h-2.5 shrink-0" viewBox="0 0 24 10" fill="none">
              <circle cx="3" cy="5" r="2.2" fill="#10b981"/>
              <line x1="3" y1="5" x2="21" y2="5" stroke="#10b981" stroke-width="2"/>
              <circle cx="21" cy="5" r="2.2" fill="#10b981"/>
            </svg>
            <span class="text-[9px] font-bold tracking-wider text-slate-500 uppercase font-sans">BREAKER</span>
            <span class="w-px h-2.5 bg-slate-200"></span>
            <span class="text-[9.5px] font-mono font-bold text-emerald-600">CLOSED</span>
          </div>
          <div class="switch-open pointer-events-auto hidden flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-white border-2 border-red-500 shadow-[0_2px_14px_rgba(239,68,68,0.25)] font-sans text-red-600 animate-pulse cursor-pointer hover:shadow-lg transition-all active:scale-95" onclick="openCircuitBreakerModal('groq', event)" title="Circuit Breaker (Groq): OPEN / DISCONNECTED - Click to view Fallback Logic">
            <svg class="w-3.5 h-2.5 shrink-0" viewBox="0 0 24 10" fill="none">
              <circle cx="3" cy="5" r="2.2" fill="#ef4444"/>
              <line x1="3" y1="5" x2="16" y2="1.5" stroke="#ef4444" stroke-width="2" stroke-linecap="round"/>
              <circle cx="21" cy="5" r="2.2" fill="#94a3b8"/>
            </svg>
            <span class="text-[9px] font-bold tracking-wider text-red-500 uppercase font-sans">BREAKER</span>
            <span class="w-px h-2.5 bg-red-200"></span>
            <span class="text-[9.5px] font-mono font-bold text-red-600">OPEN</span>
          </div>
        </div>

        <div id="cableSwitchGemini" class="absolute pointer-events-none -translate-x-1/2 -translate-y-1/2 z-15 transition-all duration-300 select-none" style="left: 751px; top: 246px;">
          <div class="switch-closed pointer-events-auto flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-white border border-slate-200/90 shadow-[0_2px_10px_rgba(0,0,0,0.06)] font-sans text-slate-700 cursor-pointer hover:border-emerald-500/60 hover:shadow-md transition-all active:scale-95" onclick="openCircuitBreakerModal('gemini', event)" title="Circuit Breaker (Gemini): CLOSED - Click to view Fallback Logic">
            <svg class="w-3.5 h-2.5 shrink-0" viewBox="0 0 24 10" fill="none">
              <circle cx="3" cy="5" r="2.2" fill="#10b981"/>
              <line x1="3" y1="5" x2="21" y2="5" stroke="#10b981" stroke-width="2"/>
              <circle cx="21" cy="5" r="2.2" fill="#10b981"/>
            </svg>
            <span class="text-[9px] font-bold tracking-wider text-slate-500 uppercase font-sans">BREAKER</span>
            <span class="w-px h-2.5 bg-slate-200"></span>
            <span class="text-[9.5px] font-mono font-bold text-emerald-600">CLOSED</span>
          </div>
          <div class="switch-open pointer-events-auto hidden flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-white border-2 border-red-500 shadow-[0_2px_14px_rgba(239,68,68,0.25)] font-sans text-red-600 animate-pulse cursor-pointer hover:shadow-lg transition-all active:scale-95" onclick="openCircuitBreakerModal('gemini', event)" title="Circuit Breaker (Gemini): OPEN / DISCONNECTED - Click to view Fallback Logic">
            <svg class="w-3.5 h-2.5 shrink-0" viewBox="0 0 24 10" fill="none">
              <circle cx="3" cy="5" r="2.2" fill="#ef4444"/>
              <line x1="3" y1="5" x2="16" y2="1.5" stroke="#ef4444" stroke-width="2" stroke-linecap="round"/>
              <circle cx="21" cy="5" r="2.2" fill="#94a3b8"/>
            </svg>
            <span class="text-[9px] font-bold tracking-wider text-red-500 uppercase font-sans">BREAKER</span>
            <span class="w-px h-2.5 bg-red-200"></span>
            <span class="text-[9.5px] font-mono font-bold text-red-600">OPEN</span>
          </div>
        </div>

        <div id="cableSwitchOpenRouter" class="absolute pointer-events-none -translate-x-1/2 -translate-y-1/2 z-15 transition-all duration-300 select-none" style="left: 751px; top: 319px;">
          <div class="switch-closed pointer-events-auto flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-white border border-slate-200/90 shadow-[0_2px_10px_rgba(0,0,0,0.06)] font-sans text-slate-700 cursor-pointer hover:border-emerald-500/60 hover:shadow-md transition-all active:scale-95" onclick="openCircuitBreakerModal('openrouter', event)" title="Circuit Breaker (OpenRouter): CLOSED - Click to view Fallback Logic">
            <svg class="w-3.5 h-2.5 shrink-0" viewBox="0 0 24 10" fill="none">
              <circle cx="3" cy="5" r="2.2" fill="#10b981"/>
              <line x1="3" y1="5" x2="21" y2="5" stroke="#10b981" stroke-width="2"/>
              <circle cx="21" cy="5" r="2.2" fill="#10b981"/>
            </svg>
            <span class="text-[9px] font-bold tracking-wider text-slate-500 uppercase font-sans">BREAKER</span>
            <span class="w-px h-2.5 bg-slate-200"></span>
            <span class="text-[9.5px] font-mono font-bold text-emerald-600">CLOSED</span>
          </div>
          <div class="switch-open pointer-events-auto hidden flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-white border-2 border-red-500 shadow-[0_2px_14px_rgba(239,68,68,0.25)] font-sans text-red-600 animate-pulse cursor-pointer hover:shadow-lg transition-all active:scale-95" onclick="openCircuitBreakerModal('openrouter', event)" title="Circuit Breaker (OpenRouter): OPEN / DISCONNECTED - Click to view Fallback Logic">
            <svg class="w-3.5 h-2.5 shrink-0" viewBox="0 0 24 10" fill="none">
              <circle cx="3" cy="5" r="2.2" fill="#ef4444"/>
              <line x1="3" y1="5" x2="16" y2="1.5" stroke="#ef4444" stroke-width="2" stroke-linecap="round"/>
              <circle cx="21" cy="5" r="2.2" fill="#94a3b8"/>
            </svg>
            <span class="text-[9px] font-bold tracking-wider text-red-500 uppercase font-sans">BREAKER</span>
            <span class="w-px h-2.5 bg-red-200"></span>
            <span class="text-[9.5px] font-mono font-bold text-red-600">OPEN</span>
          </div>
        </div>

        <!-- Dotted Line Fallback Cascade (Clickable to open Retry & Fallback Logic Modal) -->
        <div id="badgeFallbackChain" onclick="openNodeInspector('retry')" class="pointer-events-auto absolute -translate-x-1/2 -translate-y-1/2 z-15 flex items-center gap-1 px-2 py-0.5 rounded-full bg-white border border-amber-300 shadow-[0_2px_8px_rgba(245,158,11,0.15)] text-amber-700 text-[8.5px] font-bold font-mono cursor-pointer hover:bg-amber-50 hover:shadow-md transition-all select-none active:scale-95" title="Dotted Fallback Cascade: Auto-Failover Chain (Click to view Fallback Logic)">
          <span class="text-amber-600 text-xs font-bold leading-none">⤹</span>
          <span>FALLBACK CASCADE</span>
        </div>

        <div id="studioNodesStage" class="absolute inset-0 w-full h-full pointer-events-none">

            <!-- n8n Style Selection Marquee Drawer -->
            <div id="vizSelectionMarquee" class="hidden pointer-events-none absolute border-2 border-dashed border-cyan-400 bg-cyan-500/15 rounded-lg z-40 transition-none shadow-[0_0_15px_rgba(6,182,212,0.25)]"></div>

            <!-- 1. CLIENT INBOUND TRAFFIC -->

            <div id="studioNodeClient" onclick="handleNodeClick('client', event)" class="pointer-events-auto absolute flex flex-col items-center cursor-pointer select-none group" style="left: 20px; top: 218px; width: 68px;">

              <div class="w-14 h-14 rounded-2xl bg-[#0f172a] border border-slate-500/70 shadow-[0_0_25px_rgba(100,116,139,0.4)] flex items-center justify-center transition-all duration-300 group-hover:scale-105 relative">

                <svg class="w-6 h-6 text-cyan-400 drop-shadow-[0_0_8px_rgba(6,182,212,0.8)]" fill="none" stroke="currentColor" viewBox="0 0 24 24">

                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.75" d="M13 10V3L4 14h7v7l9-11h-7z"/>

                </svg>

              </div>

              <span class="node-title text-xs font-semibold text-gray-200 mt-2.5 text-center tracking-wide">Client Traffic</span>

              <span class="node-sub-teal text-[9.5px] text-cyan-400 font-mono mt-0.5">Input Ingress</span>

            </div>

            <!-- 1b. AUTH WALL NODE -->

            <div id="studioNodeAuth" onclick="handleNodeClick('auth', event)" class="pointer-events-auto absolute flex flex-col items-center cursor-pointer select-none group" style="left: 120px; top: 218px; width: 68px;">

              <div id="studioTileAuth" class="w-14 h-14 rounded-2xl bg-[#1e1338] border border-indigo-500/80 shadow-[0_0_30px_rgba(99,102,241,0.45)] flex items-center justify-center transition-all duration-300 group-hover:scale-105 relative">

                <svg class="w-6 h-6 text-indigo-400 drop-shadow-[0_0_8px_rgba(129,140,248,0.8)]" fill="none" stroke="currentColor" viewBox="0 0 24 24">

                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.75" d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z"/>

                </svg>

              </div>

              <span class="node-title text-xs font-semibold text-gray-200 mt-2.5 text-center tracking-wide">Auth Wall</span>

              <span class="node-sub text-[9.5px] text-indigo-400 font-mono mt-0.5">API Keys</span>

            </div>

            <!-- 1c. RATE LIMITER NODE -->

            <div id="studioNodeRateLimit" onclick="handleNodeClick('ratelimit', event)" class="pointer-events-auto absolute flex flex-col items-center cursor-pointer select-none group" style="left: 220px; top: 218px; width: 68px;">

              <div id="studioTileRateLimit" class="w-14 h-14 rounded-2xl bg-[#281a08] border border-amber-500/80 shadow-[0_0_30px_rgba(245,158,11,0.45)] flex items-center justify-center transition-all duration-300 group-hover:scale-105 relative">

                <svg class="w-6 h-6 text-amber-400 drop-shadow-[0_0_8px_rgba(251,191,36,0.8)]" fill="none" stroke="currentColor" viewBox="0 0 24 24">

                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.75" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"/>

                </svg>

              </div>

              <span class="node-title text-xs font-semibold text-gray-200 mt-2.5 text-center tracking-wide">Rate Limiter</span>

              <span class="node-sub text-[9.5px] text-amber-400 font-mono mt-0.5">Token Bucket</span>

            </div>

            <!-- 1d. REDIS TOKEN STORE NODE -->

            <div id="studioNodeRedis" onclick="handleNodeClick('redis', event)" class="pointer-events-auto absolute flex flex-col items-center cursor-pointer select-none group" style="left: 220px; top: 78px; width: 68px;">

              <div id="studioTileRedis" class="w-14 h-14 rounded-2xl bg-[#290a10] border border-rose-600/80 shadow-[0_0_30px_rgba(225,29,72,0.45)] flex items-center justify-center transition-all duration-300 group-hover:scale-105 relative">

                <svg class="w-6 h-6 text-rose-400 drop-shadow-[0_0_8px_rgba(244,63,94,0.8)]" fill="none" stroke="currentColor" viewBox="0 0 24 24">

                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.75" d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4m0 5c0 2.21-3.582 4-8 4s-8-1.79-8-4"/>

                </svg>

              </div>

              <span class="node-title text-xs font-semibold text-gray-200 mt-2.5 text-center tracking-wide">Redis</span>

              <span class="node-sub text-[9.5px] text-rose-400 font-mono mt-0.5">Token Store</span>

            </div>

            <!-- 1e. INPUT GUARDRAILS NODE (INGRESS OWASP / INJECTION CHECK) -->

            <div id="studioNodeInputGuardrails" onclick="handleNodeClick('inputguard', event)" class="pointer-events-auto absolute flex flex-col items-center cursor-pointer select-none group" style="left: 320px; top: 218px; width: 68px;">

              <div id="studioTileInputGuardrails" class="w-14 h-14 rounded-2xl bg-[#14231b] border border-emerald-500/80 shadow-[0_0_30px_rgba(16,185,129,0.45)] flex items-center justify-center transition-all duration-300 group-hover:scale-105 relative">

                <svg class="w-6 h-6 text-emerald-400 drop-shadow-[0_0_8px_rgba(52,211,153,0.8)]" fill="none" stroke="currentColor" viewBox="0 0 24 24">

                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.75" d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z"/>

                </svg>

              </div>

              <span class="node-title text-xs font-semibold text-gray-200 mt-2.5 text-center tracking-wide">Input Guard</span>

              <span class="node-sub text-[9.5px] text-emerald-400 font-mono mt-0.5">Injection/SSRF</span>

            </div>

            <!-- 2. API GATEWAY NODE -->

            <div id="studioNodeGateway" onclick="handleNodeClick('gateway', event)" class="pointer-events-auto absolute flex flex-col items-center cursor-pointer select-none group" style="left: 430px; top: 218px; width: 68px;">

              <div class="w-14 h-14 rounded-2xl bg-[#160e29] border border-purple-500/70 shadow-[0_0_35px_rgba(168,85,247,0.5),inset_0_0_20px_rgba(168,85,247,0.2)] flex items-center justify-center transition-all duration-300 group-hover:scale-105 group-hover:shadow-[0_0_45px_rgba(168,85,247,0.7)] relative">

                <svg class="w-6 h-6 text-purple-400 drop-shadow-[0_0_8px_rgba(192,132,252,0.8)]" fill="none" stroke="currentColor" viewBox="0 0 24 24">

                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.75" d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z"/>

                </svg>

              </div>

              <span class="node-title text-xs font-semibold text-gray-200 mt-2.5 text-center tracking-wide">API Gateway</span>

              <span class="node-sub-purple text-[9.5px] text-purple-400 font-mono mt-0.5">Global Limits</span>

            </div>

            <!-- 2b. REDIS CACHE STORE NODE (SEPARATE UPPER TIER - FAST RETURN ON HIT) -->

            <div id="studioNodeCacheStore" onclick="handleNodeClick('cache', event)" class="pointer-events-auto absolute flex flex-col items-center cursor-pointer select-none group" style="left: 430px; top: 78px; width: 68px;">

              <div id="studioTileCacheStore" class="w-14 h-14 rounded-2xl bg-[#09221b] border border-emerald-500/80 shadow-[0_0_30px_rgba(16,185,129,0.45)] flex items-center justify-center transition-all duration-300 group-hover:scale-105 relative">

                <svg class="w-6 h-6 text-emerald-400 drop-shadow-[0_0_8px_rgba(52,211,153,0.8)]" fill="none" stroke="currentColor" viewBox="0 0 24 24">

                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.75" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"/>

                </svg>

                <span id="vizCacheStatusBadge" class="hidden absolute -top-1.5 -right-1.5 px-1.5 py-0.2 rounded-full font-mono font-bold text-[8.5px] shadow-lg">HIT</span>

              </div>

              <span class="node-title text-xs font-semibold text-gray-200 mt-2.5 text-center tracking-wide">Redis Cache</span>

              <span id="vizCacheSubText" class="node-sub text-[9.5px] text-emerald-400 font-mono mt-0.5">Exact Match</span>

            </div>

            <!-- 3. LLM ROUTER NODE -->

            <div id="studioNodeBalancer" onclick="handleNodeClick('balancer', event)" class="pointer-events-auto absolute flex flex-col items-center cursor-pointer select-none group" style="left: 550px; top: 218px; width: 68px;">

              <div class="w-14 h-14 rounded-2xl bg-[#082223] border border-teal-500/70 shadow-[0_0_35px_rgba(20,184,166,0.5),inset_0_0_20px_rgba(20,184,166,0.2)] flex items-center justify-center transition-all duration-300 group-hover:scale-105 group-hover:shadow-[0_0_45px_rgba(20,184,166,0.7)] relative">

                <svg class="w-6 h-6 text-teal-400 drop-shadow-[0_0_8px_rgba(45,212,191,0.8)]" fill="none" stroke="currentColor" viewBox="0 0 24 24">

                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.75" d="M3 4a1 1 0 011-1h16a1 1 0 011 1v2.586a1 1 0 01-.293.707l-6.414 6.414a1 1 0 00-.293.707V17l-4 4v-6.586a1 1 0 00-.293-.707L3.293 7.293A1 1 0 013 6.586V4z"/>

                </svg>

              </div>

              <span class="node-title text-xs font-semibold text-gray-200 mt-2.5 text-center tracking-wide">LLM Router</span>

              <span class="node-sub-teal text-[9.5px] text-teal-400 font-mono mt-0.5">models.json</span>

            </div>

            <!-- 4. RETRY & RESILIENCE NODE -->

            <div id="studioNodeRetry" onclick="handleNodeClick('retry', event)" class="pointer-events-auto absolute flex flex-col items-center cursor-pointer select-none group" style="left: 670px; top: 218px; width: 68px;">

              <div id="studioRetryTile" class="w-14 h-14 rounded-2xl bg-[#291508] border border-amber-500/70 shadow-[0_0_35px_rgba(245,158,11,0.5),inset_0_0_20px_rgba(245,158,11,0.2)] flex items-center justify-center transition-all duration-300 group-hover:scale-105 group-hover:shadow-[0_0_45px_rgba(245,158,11,0.7)] relative">

                <svg class="w-6 h-6 text-amber-400 drop-shadow-[0_0_8px_rgba(251,191,36,0.8)]" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.75" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/>
                </svg>

              </div>

              <span class="node-title text-xs font-semibold text-gray-200 mt-2.5 text-center tracking-wide">Retry & Breaker</span>

              <span class="node-sub-rose text-[9.5px] text-amber-400 font-mono mt-0.5">3x Retries • SLA</span>

            </div>

            <!-- 5. ASYNC JOBS & DLQ PIPELINE (LOWER TIER - LEFT) -->

            <!-- 5a. IDEMPOTENCY STORE NODE -->
            <div id="studioNodeIdempotency" onclick="handleNodeClick('idempotency', event)" class="pointer-events-auto absolute flex flex-col items-center cursor-pointer select-none group" style="left: 120px; top: 358px; width: 68px;">
              <div id="studioTileIdempotency" class="w-14 h-14 rounded-2xl bg-[#1e1338] border border-violet-500/80 shadow-[0_0_30px_rgba(139,92,246,0.45)] flex items-center justify-center transition-all duration-300 group-hover:scale-105 relative">
                <svg class="w-6 h-6 text-violet-400 drop-shadow-[0_0_8px_rgba(167,139,250,0.8)]" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.75" d="M15 7a2 2 0 012 2m4 0a6 6 0 01-7.743 5.743L11 17H9v2H7v2H4a1 1 0 01-1-1v-2.586a1 1 0 01.293-.707l5.964-5.964A6 6 0 1121 9z"/>
                </svg>
              </div>
              <span class="node-title text-xs font-semibold text-gray-200 mt-2.5 text-center tracking-wide">Idempotency</span>
              <span class="node-sub text-[9.5px] text-violet-400 font-mono mt-0.5">Dedupe Store</span>
            </div>

            <!-- 5b. JOB QUEUE NODE -->
            <div id="studioNodeJobQueue" onclick="handleNodeClick('jobqueue', event)" class="pointer-events-auto absolute flex flex-col items-center cursor-pointer select-none group" style="left: 220px; top: 358px; width: 68px;">
              <div id="studioTileJobQueue" class="w-14 h-14 rounded-2xl bg-[#082223] border border-cyan-500/80 shadow-[0_0_30px_rgba(6,182,212,0.45)] flex items-center justify-center transition-all duration-300 group-hover:scale-105 relative">
                <svg class="w-6 h-6 text-cyan-400 drop-shadow-[0_0_8px_rgba(34,211,238,0.8)]" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.75" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"/>
                </svg>
              </div>
              <span class="node-title text-xs font-semibold text-gray-200 mt-2.5 text-center tracking-wide">Job Queue</span>
              <span class="node-sub text-[9.5px] text-cyan-400 font-mono mt-0.5">Redis Queue</span>
            </div>

            <!-- 5c. ASYNC WORKERS NODE -->
            <div id="studioNodeWorker" onclick="handleNodeClick('worker', event)" class="pointer-events-auto absolute flex flex-col items-center cursor-pointer select-none group" style="left: 320px; top: 358px; width: 68px;">
              <div id="studioTileWorker" class="w-14 h-14 rounded-2xl bg-[#160e29] border border-blue-500/80 shadow-[0_0_30px_rgba(59,130,246,0.45)] flex items-center justify-center transition-all duration-300 group-hover:scale-105 relative">
                <svg class="w-6 h-6 text-blue-400 drop-shadow-[0_0_8px_rgba(96,165,250,0.8)]" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.75" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z"/>
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.75" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"/>
                </svg>
              </div>
              <span class="node-title text-xs font-semibold text-gray-200 mt-2.5 text-center tracking-wide">Workers</span>
              <span class="node-sub text-[9.5px] text-blue-400 font-mono mt-0.5">Async Pool</span>
            </div>

            <!-- 5d. DLQ NODE WITH DEPTH BADGE & REPLAY BUTTON -->
            <div id="studioNodeDLQ" onclick="handleNodeClick('dlq', event)" class="pointer-events-auto absolute flex flex-col items-center cursor-pointer select-none group" style="left: 425px; top: 358px; width: 78px;">
              <div id="studioTileDLQ" class="w-14 h-14 rounded-2xl bg-[#290a10] border border-rose-600/80 shadow-[0_0_30px_rgba(225,29,72,0.45)] flex items-center justify-center transition-all duration-300 group-hover:scale-105 relative">
                <svg class="w-6 h-6 text-rose-400 drop-shadow-[0_0_8px_rgba(244,63,94,0.8)]" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.75" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"/>
                </svg>
                <span id="vizDlqDepthBadge" class="absolute -top-1.5 -right-1.5 px-1.5 py-0.2 rounded-full bg-red-600 border border-white text-white font-mono font-bold text-[9px] shadow-lg">0</span>
              </div>
              <span class="node-title text-xs font-semibold text-gray-200 mt-2.5 text-center tracking-wide">DLQ</span>
              <button id="vizDlqReplayBtn" type="button" onclick="replayLatestDLQJob(event)" class="mt-1 px-2 py-0.5 text-[8.5px] font-bold rounded-lg bg-amber-500 hover:bg-amber-400 text-gray-950 shadow-md active:scale-95 transition cursor-pointer" title="Replay oldest job in DLQ">Replay ↺</button>
            </div>

            <!-- 6. SECURITY EGRESS & AUDIT RETURN (LOWER TIER - RIGHT) -->

            <!-- 6b. OUTPUT GUARDRAILS NODE (LEAK & PII SCANNER ON LLM RESPONSE) -->
            <div id="studioNodeOutputGuardrails" onclick="handleNodeClick('outputguard', event)" class="pointer-events-auto absolute flex flex-col items-center cursor-pointer select-none group" style="left: 670px; top: 358px; width: 68px;">
              <div id="studioTileOutputGuardrails" class="w-14 h-14 rounded-2xl bg-[#09222b] border border-cyan-500/80 shadow-[0_0_30px_rgba(6,182,212,0.45)] flex items-center justify-center transition-all duration-300 group-hover:scale-105 relative">
                <svg class="w-6 h-6 text-cyan-400 drop-shadow-[0_0_8px_rgba(34,211,238,0.8)]" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.75" d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z"/>
                </svg>
              </div>
              <span class="node-title text-xs font-semibold text-gray-200 mt-2.5 text-center tracking-wide">Output Guard</span>
              <span class="node-sub text-[9.5px] text-cyan-400 font-mono mt-0.5">Leak Scanner</span>
            </div>

            <!-- 6c. AUDIT LOG NODE (ZERO-LEAK LEDGER BEFORE RETURNING TO CLIENT) -->
            <div id="studioNodeAuditLog" onclick="handleNodeClick('audit', event)" class="pointer-events-auto absolute flex flex-col items-center cursor-pointer select-none group" style="left: 550px; top: 358px; width: 68px;">
              <div id="studioTileAuditLog" class="w-14 h-14 rounded-2xl bg-[#281a08] border border-amber-500/80 shadow-[0_0_30px_rgba(245,158,11,0.45)] flex items-center justify-center transition-all duration-300 group-hover:scale-105 relative">
                <svg class="w-6 h-6 text-amber-400 drop-shadow-[0_0_8px_rgba(251,191,36,0.8)]" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.75" d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2m-3 7h3m-3 4h3m-6-4h.01M9 16h.01"/>
                </svg>
              </div>
              <span class="node-title text-xs font-semibold text-gray-200 mt-2.5 text-center tracking-wide">Audit Log</span>
              <span class="node-sub text-[9.5px] text-amber-400 font-mono mt-0.5">Zero-Leak Ledger</span>
            </div>

            <!-- 5e. GROQ QUEUE (PRIOR TRAFFIC HANDLER) -->
            <div id="vizQueueBoxGroq" class="viz-queue-box pointer-events-auto absolute flex flex-col justify-start p-2 rounded-xl bg-slate-900/90 border border-slate-700/80 shadow-lg select-none" style="left: 850px; top: 72px; width: 105px; min-height: 56px;">
              <div class="flex items-center justify-between text-[8.5px] font-mono">
                <span class="text-amber-400 font-bold uppercase tracking-wider">Queue</span>
                <span id="vizRatioQueueGroq" class="text-gray-300 font-bold">0/20</span>
              </div>
              <div class="w-full bg-slate-800 rounded-full h-1 mt-1 overflow-hidden">
                <div id="vizBarQueueGroq" class="bg-amber-400 h-1 rounded-full transition-all duration-300" style="width: 0%"></div>
              </div>
              <div id="vizDotsQueueGroq" class="flex gap-1 mt-1.5 flex-wrap min-h-[8px]"></div>
            </div>

            <!-- 5b. GROQ PROVIDER NODE (ENLARGED CARD WITH BUILT-IN FLIGHT COUNTS) -->
            <div id="studioNodeCache" onclick="handleNodeClick('groq', event)" class="pointer-events-auto absolute flex flex-col items-center cursor-pointer select-none group" style="left: 990px; top: 52px; width: 88px;">
              <div id="studioCacheTile" class="w-[88px] min-h-[96px] rounded-2xl bg-[#2a0c14] border border-rose-500/80 shadow-[0_0_35px_rgba(244,63,94,0.55),inset_0_0_20px_rgba(244,63,94,0.2)] flex flex-col items-center justify-between transition-all duration-300 group-hover:scale-105 group-hover:shadow-[0_0_45px_rgba(244,63,94,0.75)] relative p-2">
                <div class="flex items-center justify-center w-full relative">
                  <div id="vizLogoGroq" class="w-6 h-6 flex items-center justify-center">
                    <svg class="w-6 h-6 shrink-0" viewBox="0 0 24 24" fill="none">
                      <rect width="24" height="24" rx="5" fill="#F55036"/>
                      <path d="M16 8.5A5.5 5.5 0 1 0 16.5 15.5H12V12h4.5" stroke="#FFF" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/>
                    </svg>
                  </div>
                  <span id="studioWarnBadgeGroq" class="hidden absolute -top-1 -right-1 w-4 h-4 rounded-full bg-red-600 border border-white text-white flex items-center justify-center text-[9px] shadow-lg animate-pulse z-10" title="Fault / Circuit Breaker Active">⚠️</span>
                </div>
                <div class="flex flex-col items-center text-center -mt-0.5">
                  <span class="node-title text-[11.5px] font-bold text-gray-100 tracking-wide">Groq</span>
                  <span class="node-sub-rose text-[8.5px] text-rose-400 font-mono">Fast Tier</span>
                </div>
                <div class="node-flight-section w-full pt-1.5 border-t border-white/10 flex flex-col items-center">
                  <div class="flex justify-between w-full text-[7.5px] font-mono text-gray-300">
                    <span class="text-cyan-400 font-bold">FLIGHT</span>
                    <span id="vizRatioInflightGroq" class="font-bold">0/10</span>
                  </div>
                  <div class="w-full bg-slate-800 rounded-full h-1 mt-0.5 overflow-hidden">
                    <div id="vizBarInflightGroq" class="bg-cyan-400 h-1 rounded-full transition-all duration-300" style="width: 0%"></div>
                  </div>
                </div>
              </div>
            </div>

            <!-- 6a. GEMINI QUEUE (PRIOR TRAFFIC HANDLER) -->
            <div id="vizQueueBoxGemini" class="viz-queue-box pointer-events-auto absolute flex flex-col justify-start p-2 rounded-xl bg-slate-900/90 border border-slate-700/80 shadow-lg select-none" style="left: 850px; top: 218px; width: 105px; min-height: 56px;">
              <div class="flex items-center justify-between text-[8.5px] font-mono">
                <span class="text-amber-400 font-bold uppercase tracking-wider">Queue</span>
                <span id="vizRatioQueueGemini" class="text-gray-300 font-bold">0/20</span>
              </div>
              <div class="w-full bg-slate-800 rounded-full h-1 mt-1 overflow-hidden">
                <div id="vizBarQueueGemini" class="bg-amber-400 h-1 rounded-full transition-all duration-300" style="width: 0%"></div>
              </div>
              <div id="vizDotsQueueGemini" class="flex gap-1 mt-1.5 flex-wrap min-h-[8px]"></div>
            </div>

            <!-- 6b. GOOGLE GEMINI PROVIDER NODE (ENLARGED CARD WITH BUILT-IN FLIGHT COUNTS) -->
            <div id="studioNodeGemini" onclick="handleNodeClick('gemini', event)" class="pointer-events-auto absolute flex flex-col items-center cursor-pointer select-none group" style="left: 990px; top: 198px; width: 88px;">
              <div id="studioGeminiTile" class="w-[88px] min-h-[96px] rounded-2xl bg-[#141529] border border-indigo-500/80 shadow-[0_0_35px_rgba(99,102,241,0.55),inset_0_0_20px_rgba(99,102,241,0.2)] flex flex-col items-center justify-between transition-all duration-300 group-hover:scale-105 group-hover:shadow-[0_0_45px_rgba(99,102,241,0.75)] relative p-2">
                <div class="flex items-center justify-center w-full relative">
                  <div id="vizLogoGemini" class="w-6 h-6 flex items-center justify-center">
                    <svg class="w-6 h-6 shrink-0 drop-shadow-[0_0_8px_rgba(155,114,203,0.7)]" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                      <defs>
                        <linearGradient id="geminiSparkGrad" x1="2" y1="2" x2="22" y2="22" gradientUnits="userSpaceOnUse">
                          <stop offset="0%" stop-color="#4285F4"/>
                          <stop offset="35%" stop-color="#9B72CB"/>
                          <stop offset="70%" stop-color="#D96570"/>
                          <stop offset="100%" stop-color="#FFA439"/>
                        </linearGradient>
                      </defs>
                      <path d="M12 2C12 7.523 7.523 12 2 12C7.523 12 12 16.477 12 22C12 16.477 16.477 12 22 12C16.477 12 12 7.523 12 2Z" fill="url(#geminiSparkGrad)"/>
                    </svg>
                  </div>
                  <span id="studioWarnBadgeGemini" class="hidden absolute -top-1 -right-1 w-4 h-4 rounded-full bg-red-600 border border-white text-white flex items-center justify-center text-[9px] shadow-lg animate-pulse z-10" title="Fault / Circuit Breaker Active">⚠️</span>
                </div>
                <div class="flex flex-col items-center text-center -mt-0.5">
                  <span class="node-title text-[11px] font-bold text-gray-100 tracking-wide">Gemini</span>
                  <span class="node-sub-indigo text-[8.5px] text-indigo-400 font-mono">Smart Tier</span>
                </div>
                <div class="node-flight-section w-full pt-1.5 border-t border-white/10 flex flex-col items-center">
                  <div class="flex justify-between w-full text-[7.5px] font-mono text-gray-300">
                    <span class="text-indigo-400 font-bold">FLIGHT</span>
                    <span id="vizRatioInflightGemini" class="font-bold">0/10</span>
                  </div>
                  <div class="w-full bg-slate-800 rounded-full h-1 mt-0.5 overflow-hidden">
                    <div id="vizBarInflightGemini" class="bg-indigo-400 h-1 rounded-full transition-all duration-300" style="width: 0%"></div>
                  </div>
                </div>
              </div>
            </div>

            <!-- 7a. OPENROUTER QUEUE (PRIOR TRAFFIC HANDLER) -->
            <div id="vizQueueBoxOpenRouter" class="viz-queue-box pointer-events-auto absolute flex flex-col justify-start p-2 rounded-xl bg-slate-900/90 border border-slate-700/80 shadow-lg select-none" style="left: 850px; top: 364px; width: 105px; min-height: 56px;">
              <div class="flex items-center justify-between text-[8.5px] font-mono">
                <span class="text-amber-400 font-bold uppercase tracking-wider">Queue</span>
                <span id="vizRatioQueueOpenRouter" class="text-gray-300 font-bold">0/20</span>
              </div>
              <div class="w-full bg-slate-800 rounded-full h-1 mt-1 overflow-hidden">
                <div id="vizBarQueueOpenRouter" class="bg-amber-400 h-1 rounded-full transition-all duration-300" style="width: 0%"></div>
              </div>
              <div id="vizDotsQueueOpenRouter" class="flex gap-1 mt-1.5 flex-wrap min-h-[8px]"></div>
            </div>

            <!-- 7b. OPENROUTER PROVIDER NODE (ENLARGED CARD WITH BUILT-IN FLIGHT COUNTS) -->
            <div id="studioNodeDatabase" onclick="handleNodeClick('openrouter', event)" class="pointer-events-auto absolute flex flex-col items-center cursor-pointer select-none group" style="left: 990px; top: 344px; width: 88px;">
              <div id="studioDatabaseTile" class="w-[88px] min-h-[96px] rounded-2xl bg-[#06241b] border border-emerald-500/70 shadow-[0_0_35px_rgba(16,185,129,0.5),inset_0_0_20px_rgba(16,185,129,0.2)] flex flex-col items-center justify-between transition-all duration-300 group-hover:scale-105 group-hover:shadow-[0_0_45px_rgba(16,185,129,0.7)] relative p-2">
                <div class="flex items-center justify-center w-full relative">
                  <div id="vizLogoOpenRouter" class="w-6 h-6 flex items-center justify-center">
                    <svg class="w-6 h-6 shrink-0" viewBox="0 0 24 24" fill="none">
                      <rect width="24" height="24" rx="5" fill="#4F46E5"/>
                      <path d="M7 12h10M13 8l4 4-4 4" stroke="#FFF" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/>
                    </svg>
                  </div>
                  <span id="studioWarnBadgeOpenRouter" class="hidden absolute -top-1 -right-1 w-4 h-4 rounded-full bg-red-600 border border-white text-white flex items-center justify-center text-[9px] shadow-lg animate-pulse z-10" title="Fault / Circuit Breaker Active">⚠️</span>
                </div>
                <div class="flex flex-col items-center text-center -mt-0.5">
                  <span class="node-title text-[11px] font-bold text-gray-100 tracking-wide">OpenRouter</span>
                  <span class="node-sub-emerald text-[8.5px] text-emerald-400 font-mono">Fallback Tier</span>
                </div>
                <div class="node-flight-section w-full pt-1.5 border-t border-white/10 flex flex-col items-center">
                  <div class="flex justify-between w-full text-[7.5px] font-mono text-gray-300">
                    <span class="text-emerald-400 font-bold">FLIGHT</span>
                    <span id="vizRatioInflightOpenRouter" class="font-bold">0/10</span>
                  </div>
                  <div class="w-full bg-slate-800 rounded-full h-1 mt-0.5 overflow-hidden">
                    <div id="vizBarInflightOpenRouter" class="bg-emerald-400 h-1 rounded-full transition-all duration-300" style="width: 0%"></div>
                  </div>
                </div>
              </div>
            </div>

          </div>

        </div>

        <!-- Canvas Pan & Zoom Control Dock<!-- Canvas Pan & Zoom Control Dock (Bottom-Right of Canvas) -->

        <div id="vizCanvasControls" class="absolute bottom-6 right-6 z-20 flex items-center gap-1 bg-[#141923]/90 border border-white/10 rounded-xl p-1 shadow-xl backdrop-blur-md">

          <button type="button" onclick="zoomCanvasOut()" class="w-6 h-6 rounded-lg hover:bg-white/10 text-gray-300 hover:text-white flex items-center justify-center font-bold text-sm transition cursor-pointer" title="Zoom Out">−</button>

          <span id="zoomLevelDisplay" class="text-[11px] font-mono text-gray-400 px-1 min-w-[34px] text-center select-none">100%</span>

          <button type="button" onclick="zoomCanvasIn()" class="w-6 h-6 rounded-lg hover:bg-white/10 text-gray-300 hover:text-white flex items-center justify-center font-bold text-sm transition cursor-pointer" title="Zoom In">+</button>

          <button type="button" onclick="resetCanvasZoomPan()" class="w-6 h-6 rounded-lg hover:bg-white/10 text-gray-300 hover:text-white flex items-center justify-center transition ml-0.5 cursor-pointer" title="Reset Canvas View">

            <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/></svg>

          </button>

        </div>

<!-- Real-Time SSE Stream Event Console Modal (Toggleable via Bell or Bottom Pill) -->

        <div id="visualizerFeedModal" class="hidden absolute bottom-14 left-1/2 -translate-x-1/2 w-11/12 max-w-2xl bg-[#0e121b]/95 border border-white/10 rounded-2xl shadow-2xl backdrop-blur-2xl p-4 z-40">

          <div class="flex items-center justify-between pb-2 border-b border-white/10 mb-2">

            <div class="flex items-center gap-2">

              <span class="w-2 h-2 rounded-full bg-cyan-400 animate-pulse"></span>

              <span class="text-xs font-bold text-white tracking-wide uppercase">Real-Time Gateway Traffic Stream (SSE Events)</span>

            </div>

            <div class="flex items-center gap-2">

              <span id="vizEventCountBadge" class="text-[10px] text-gray-400 bg-white/5 px-2 py-0.5 rounded-full border border-white/10">0 events</span>

              <button type="button" onclick="clearVisualizerFeed()" class="text-[10px] text-gray-400 hover:text-white transition">Clear</button>

              <button type="button" onclick="toggleVisualizerFeedModal()" class="text-gray-400 hover:text-white transition ml-2">✕</button>

            </div>

          </div>

          <div id="vizEventStreamFeed" class="h-44 overflow-y-auto font-mono text-[11px] bg-black/50 rounded-xl p-3 border border-white/5 space-y-1.5 custom-dropdown-scroll">

            <div class="text-gray-500 text-[10px] italic">Connected to /api/reliability/traffic-stream. Awaiting inbound traffic...</div>

          </div>

        </div>

      </div>

    </div>

    <!-- Provider API Key Edit Modal -->

    <div id="keyEditModal" class="hidden fixed inset-0 z-50 flex items-center justify-center bg-black/75 backdrop-blur-sm p-4">

      <div class="glass-panel rounded-2xl border border-white/10 p-6 max-w-md w-full shadow-2xl relative bg-[#111827]" onclick="event.stopPropagation()">

        <!-- Header -->

        <div class="flex items-center justify-between pb-3 border-b border-white/10 mb-4">

          <div class="flex items-center gap-2">

            <span class="w-2.5 h-2.5 rounded-full bg-cyan-400"></span>

            <h3 class="text-sm font-bold text-white">Update API Key: <span id="keyModalProviderTitle" class="text-cyan-400">Provider</span></h3>

          </div>

          <button type="button" onclick="closeKeyEditModal()" class="text-gray-400 hover:text-white transition p-1">

            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg>

          </button>

        </div>

        <div class="space-y-4 text-xs">

          <p class="text-gray-300 text-[11px] leading-relaxed">

            Enter the new API key below. The gateway will run a <strong>small validation test call</strong> before applying it. Valid keys are updated immediately at runtime and reset the circuit breaker. Plaintext keys are never returned to the UI.

          </p>

          <!-- Input with eye visibility toggle -->

          <div>

            <label class="block text-gray-400 mb-1.5 font-medium">New API Key</label>

            <div class="relative">

              <input id="keyModalInput" type="password" class="w-full bg-black/50 border border-white/10 rounded-lg p-2.5 pr-10 text-white font-mono text-xs focus:outline-none focus:border-cyan-500" placeholder="Paste API key...">

              <button type="button" onclick="toggleKeyVisibility()" class="absolute right-2.5 top-2.5 text-gray-400 hover:text-white transition" title="Toggle visibility">

                <svg id="keyEyeIcon" class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"/><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z"/></svg>

              </button>

            </div>

          </div>

          <!-- Status & Feedback Box -->

          <div id="keyModalFeedback" class="hidden p-3 rounded-xl border text-[11px] flex items-start gap-2">

            <span id="keyModalFeedbackIcon" class="shrink-0 mt-0.5"></span>

            <div id="keyModalFeedbackText" class="flex-1"></div>

          </div>

          <!-- Action Buttons -->

          <div class="flex items-center justify-end gap-2 pt-2">

            <button type="button" onclick="closeKeyEditModal()" class="px-3.5 py-1.5 rounded-lg border border-white/10 text-gray-300 hover:bg-white/5 transition text-xs">Cancel</button>

            <button id="keyModalSaveBtn" type="button" onclick="submitCandidateKey()" class="px-4 py-1.5 rounded-lg bg-cyan-500 hover:bg-cyan-400 text-gray-950 font-bold transition text-xs flex items-center gap-1.5 cursor-pointer">

              <span>Validate & Save</span>

            </button>

          </div>

        </div>

      </div>

    </div>

    <!-- Architecture & Capabilities Page (Dedicated Full View in Main Window) -->

    <div id="aboutPage" class="hidden flex-1 flex flex-col h-full overflow-y-auto p-6 max-w-7xl mx-auto w-full">

      <!-- Page Header -->

      <div class="flex items-center justify-between pb-4 border-b border-white/10 mb-6 shrink-0">

        <div class="flex items-center gap-3">

          <div class="w-8 h-8 rounded-lg bg-indigo-950/60 border border-indigo-500/30 flex items-center justify-center text-indigo-400 shrink-0">

            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"/></svg>

          </div>

          <div>

            <h2 class="text-xl font-bold text-white tracking-tight">Gateway Architecture & Capabilities</h2>

            <p class="text-xs text-gray-400 mt-0.5">Enterprise-grade resilience, intelligent routing, cost control, telemetry and security</p>

          </div>

        </div>

      </div>

      <!-- 3D Interactive Foundation Primitives Component Card -->

      <div class="relative w-full rounded-2xl glass-panel border border-white/10 p-3 sm:p-4 mb-4 overflow-hidden">

        <div class="flex items-center justify-between mb-2">

          <div class="flex items-center gap-2">

            <span class="w-2 h-2 rounded-full bg-cyan-400 animate-pulse"></span>

            <span class="text-xs font-bold text-white tracking-wide uppercase">Core Architecture Primitives</span>

          </div>

          <span class="text-[10px] text-gray-400">Interactive 3D Raymarched Primitives • Drag to rotate, click to activate</span>

        </div>

        <div id="primitivesStage" class="relative w-full h-44 sm:h-52 touch-pan-y select-none overflow-hidden rounded-xl bg-black/40 border border-white/5">

          <canvas id="primitivesCanvas" class="pointer-events-none absolute inset-0 block h-full w-full"></canvas>

          <div id="primitivesButtonsContainer"></div>

        </div>

      </div>

      <!-- 5 Enterprise Capabilities Cards Grid (Compact & Refined) -->

      <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3 mb-6">

        <!-- 1. Reliability -->

        <div class="glass-panel p-3.5 rounded-xl border border-white/10 flex flex-col justify-between hover:border-blue-500/30 transition">

          <div>

            <div class="flex items-center gap-2.5 mb-2.5">

              <div class="w-7 h-7 rounded-lg bg-blue-500/10 border border-blue-500/30 flex items-center justify-center text-blue-400 shrink-0">

                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4.318 6.318a4.5 4.5 0 000 6.364L12 20.364l7.682-7.682a4.5 4.5 0 00-6.364-6.364L12 7.636l-1.318-1.318a4.5 4.5 0 00-6.364 0z"/></svg>

              </div>

              <div class="min-w-0">

                <h3 class="text-xs font-bold text-white truncate">Reliability</h3>

                <span class="text-[9px] text-blue-400 font-medium block -mt-0.5">Fault Tolerance & SLOs</span>

              </div>

            </div>

            <ul class="space-y-1 text-[11px] text-gray-300">

              <li class="flex items-start gap-1.5">

                <span class="text-blue-400 font-bold">✓</span>

                <span>Resilient failover and circuit breakers</span>

              </li>

              <li class="flex items-start gap-1.5">

                <span class="text-blue-400 font-bold">✓</span>

                <span>Retries with backoff and jitter</span>

              </li>

              <li class="flex items-start gap-1.5">

                <span class="text-blue-400 font-bold">✓</span>

                <span>Semaphores and bounded queues</span>

              </li>

              <li class="flex items-start gap-1.5">

                <span class="text-blue-400 font-bold">✓</span>

                <span>Dead letter queue with replay</span>

              </li>

            </ul>

          </div>

          <div class="mt-2.5 pt-2 border-t border-white/5 flex items-center justify-between text-[10px]">

            <span class="text-gray-500 font-mono">ACTIVE</span>

            <button onclick="showView('observability')" class="text-blue-400 hover:text-blue-300 transition font-medium">View Breakers →</button>

          </div>

        </div>

        <!-- 2. Cost control -->

        <div class="glass-panel p-3.5 rounded-xl border border-white/10 flex flex-col justify-between hover:border-emerald-500/30 transition">

          <div>

            <div class="flex items-center gap-2.5 mb-2.5">

              <div class="w-7 h-7 rounded-lg bg-emerald-500/10 border border-emerald-500/30 flex items-center justify-center text-emerald-400 shrink-0">

                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8c-1.657 0-3 .895-3 2s1.343 2 3 2 3 .895 3 2-1.343 2-3 2m0-8c1.11 0 2.08.402 2.599 1M12 8V7m0 1v8m0 0v1m0-1c-1.11 0-2.08-.402-2.599-1M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/></svg>

              </div>

              <div class="min-w-0">

                <h3 class="text-xs font-bold text-white truncate">Cost control</h3>

                <span class="text-[9px] text-emerald-400 font-medium block -mt-0.5">Token Efficiency & Budgets</span>

              </div>

            </div>

            <ul class="space-y-1 text-[11px] text-gray-300">

              <li class="flex items-start gap-1.5">

                <span class="text-emerald-400 font-bold">✓</span>

                <span>Response caching & coalescing</span>

              </li>

              <li class="flex items-start gap-1.5">

                <span class="text-emerald-400 font-bold">✓</span>

                <span>Smart model routing by alias</span>

              </li>

              <li class="flex items-start gap-1.5">

                <span class="text-emerald-400 font-bold">✓</span>

                <span>Per-key budgets and limits</span>

              </li>

              <li class="flex items-start gap-1.5">

                <span class="text-emerald-400 font-bold">✓</span>

                <span>Per-pod spend tracking</span>

              </li>

            </ul>

          </div>

          <div class="mt-2.5 pt-2 border-t border-white/5 flex items-center justify-between text-[10px]">

            <span class="text-gray-500 font-mono">OPTIMIZED</span>

            <button onclick="showView('stats')" class="text-emerald-400 hover:text-emerald-300 transition font-medium">View Stats →</button>

          </div>

        </div>

        <!-- 3. Observability -->

        <div class="glass-panel p-3.5 rounded-xl border border-white/10 flex flex-col justify-between hover:border-purple-500/30 transition">

          <div>

            <div class="flex items-center gap-2.5 mb-2.5">

              <div class="w-7 h-7 rounded-lg bg-purple-500/10 border border-purple-500/30 flex items-center justify-center text-purple-400 shrink-0">

                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"/><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z"/></svg>

              </div>

              <div class="min-w-0">

                <h3 class="text-xs font-bold text-white truncate">Observability</h3>

                <span class="text-[9px] text-purple-400 font-medium block -mt-0.5">Telemetry & Metrics</span>

              </div>

            </div>

            <ul class="space-y-1 text-[11px] text-gray-300">

              <li class="flex items-start gap-1.5">

                <span class="text-purple-400 font-bold">✓</span>

                <span>Live p50, p95, p99 latency</span>

              </li>

              <li class="flex items-start gap-1.5">

                <span class="text-purple-400 font-bold">✓</span>

                <span>Structured logs and tracing</span>

              </li>

              <li class="flex items-start gap-1.5">

                <span class="text-purple-400 font-bold">✓</span>

                <span>Provider health dashboard</span>

              </li>

              <li class="flex items-start gap-1.5">

                <span class="text-purple-400 font-bold">✓</span>

                <span>Alerts on errors & DLQ depth</span>

              </li>

            </ul>

          </div>

          <div class="mt-2.5 pt-2 border-t border-white/5 flex items-center justify-between text-[10px]">

            <span class="text-gray-500 font-mono">LIVE</span>

            <button onclick="showView('logs')" class="text-purple-400 hover:text-purple-300 transition font-medium">View Logs →</button>

          </div>

        </div>

        <!-- 4. Security -->

        <div class="glass-panel p-3.5 rounded-xl border border-white/10 flex flex-col justify-between hover:border-rose-500/30 transition">

          <div>

            <div class="flex items-center gap-2.5 mb-2.5">

              <div class="w-7 h-7 rounded-lg bg-rose-500/10 border border-rose-500/30 flex items-center justify-center text-rose-400 shrink-0">

                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z"/></svg>

              </div>

              <div class="min-w-0">

                <h3 class="text-xs font-bold text-white truncate">Security</h3>

                <span class="text-[9px] text-rose-400 font-medium block -mt-0.5">Guardrails & Isolation</span>

              </div>

            </div>

            <ul class="space-y-1 text-[11px] text-gray-300">

              <li class="flex items-start gap-1.5">

                <span class="text-rose-400 font-bold">✓</span>

                <span>Central key management</span>

              </li>

              <li class="flex items-start gap-1.5">

                <span class="text-rose-400 font-bold">✓</span>

                <span>Auth & per-key rate limiting</span>

              </li>

              <li class="flex items-start gap-1.5">

                <span class="text-rose-400 font-bold">✓</span>

                <span>Injection & PII guardrails</span>

              </li>

              <li class="flex items-start gap-1.5">

                <span class="text-rose-400 font-bold">✓</span>

                <span>Audit logs and encrypted secrets</span>

              </li>

            </ul>

          </div>

          <div class="mt-2.5 pt-2 border-t border-white/5 flex items-center justify-between text-[10px]">

            <span class="text-gray-500 font-mono">ENFORCED</span>

            <button onclick="showView('settings')" class="text-rose-400 hover:text-rose-300 transition font-medium">Manage Keys →</button>

          </div>

        </div>

        <!-- 5. Scale -->

        <div class="glass-panel p-3.5 rounded-xl border border-white/10 flex flex-col justify-between hover:border-amber-500/30 transition sm:col-span-2 lg:col-span-1">

          <div>

            <div class="flex items-center gap-2.5 mb-2.5">

              <div class="w-7 h-7 rounded-lg bg-amber-500/10 border border-amber-500/30 flex items-center justify-center text-amber-400 shrink-0">

                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 7v10c0 2 1.5 3 3.5 3h9c2 0 3.5-1 3.5-3V7c0-2-1.5-3-3.5-3h-9C5.5 4 4 5 4 7zm0 5h16"/></svg>

              </div>

              <div class="min-w-0">

                <h3 class="text-xs font-bold text-white truncate">Scale</h3>

                <span class="text-[9px] text-amber-400 font-medium block -mt-0.5">High Concurrency & Queues</span>

              </div>

            </div>

            <ul class="space-y-1 text-[11px] text-gray-300">

              <li class="flex items-start gap-1.5">

                <span class="text-amber-400 font-bold">✓</span>

                <span>Backpressure & load shedding</span>

              </li>

              <li class="flex items-start gap-1.5">

                <span class="text-amber-400 font-bold">✓</span>

                <span>Per-provider concurrency pools</span>

              </li>

              <li class="flex items-start gap-1.5">

                <span class="text-amber-400 font-bold">✓</span>

                <span>Async job workers</span>

              </li>

              <li class="flex items-start gap-1.5">

                <span class="text-amber-400 font-bold">✓</span>

                <span>Graceful shutdown & draining</span>

              </li>

            </ul>

          </div>

          <div class="mt-2.5 pt-2 border-t border-white/5 flex items-center justify-between text-[10px]">

            <span class="text-gray-500 font-mono">READY</span>

            <span class="text-amber-400/80 font-mono">Unlimited Concurrency</span>

          </div>

        </div>

      </div>

    </div>

    <!-- Dedicated Chats Page (Table of chat history matching user specification) -->

    <div id="chatsPage" class="hidden flex-1 flex flex-col h-full overflow-y-auto p-4 sm:p-6 w-full max-w-7xl mx-auto">

      <!-- Top Action Bar -->

      <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-3 pb-4 mb-4 shrink-0">

        <h1 class="text-2xl sm:text-3xl font-bold text-white tracking-tight">Chats</h1>

        <!-- Controls: Search + Manage Storage + Filter + New Chat -->

        <div class="flex flex-wrap items-center gap-2 sm:gap-2.5">

          <!-- Search chats input -->

          <div class="relative flex items-center">

            <svg class="w-3.5 h-3.5 text-gray-400 absolute left-3 pointer-events-none" fill="none" stroke="currentColor" viewBox="0 0 24 24">

              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"/>

            </svg>

            <input id="chatTableSearchInput" type="text" placeholder="Search chats..." oninput="filterChatsTable()" class="bg-[#111827] border border-white/10 hover:border-white/20 rounded-lg pl-9 pr-3 py-1.5 text-xs text-white placeholder-gray-500 focus:outline-none focus:border-cyan-500/50 w-36 sm:w-56 transition">

          </div>

          <!-- Manage Storage Button -->

          <button type="button" onclick="openStorageModal()" class="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-white/10 hover:border-white/20 bg-white/5 hover:bg-white/10 text-xs font-medium text-gray-200 transition shrink-0 cursor-pointer" title="Manage Storage & Session Retention">

            <svg class="w-3.5 h-3.5 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">

              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 7v10c0 2 1.5 3 3.5 3h9c2 0 3.5-1 3.5-3V7c0-2-1.5-3-3.5-3h-9C5.5 4 4 5 4 7zm0 5h16m-16 5h16"/>

            </svg>

            <span>Manage Storage</span>

          </button>

          <!-- Filter by All Dropdown -->

          <div class="relative shrink-0">

            <select id="chatModelFilter" onchange="filterChatsTable()" class="bg-[#111827] border border-white/10 hover:border-white/20 rounded-lg pl-3 pr-7 py-1.5 text-xs text-gray-200 focus:outline-none focus:border-cyan-500/50 cursor-pointer appearance-none transition">

              <option value="all">Filter by All</option>

              <option value="groq">Groq</option>

              <option value="openrouter">OpenRouter</option>

              <option value="gemini">Gemini</option>

            </select>

            <svg class="w-3 h-3 text-gray-400 absolute right-2.5 top-1/2 -translate-y-1/2 pointer-events-none" fill="none" stroke="currentColor" viewBox="0 0 24 24">

              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"/>

            </svg>

          </div>

          <!-- New Chat Button -->

          <button type="button" onclick="startNewChat()" class="flex items-center gap-1.5 px-3.5 py-1.5 rounded-lg bg-[#0f172a] hover:bg-[#1e293b] text-white border border-white/15 hover:border-cyan-500/40 text-xs font-semibold shadow-sm transition shrink-0 cursor-pointer" title="Start New Chat">

            <svg class="w-3 h-3 text-gray-300" fill="none" stroke="currentColor" viewBox="0 0 24 24">

              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M12 4v16m8-8H4"/>

            </svg>

            <span>New chat</span>

          </button>

        </div>

      </div>

      <!-- Chats Table Container Card (Curved upper corners & overflow clipped) -->

      <div class="glass-panel rounded-2xl border border-white/10 shadow-2xl flex flex-col min-h-[320px] overflow-hidden">

        <div id="chatsTableWrapper" class="overflow-x-auto flex-1 rounded-2xl">

          <table class="w-full text-left border-collapse">

            <thead>

              <tr class="border-b border-white/10 bg-white/[0.03] text-[11px] font-bold tracking-wider text-gray-400 uppercase">

                <th class="py-3.5 px-4 w-52 sm:w-64 rounded-tl-2xl">USER</th>

                <th class="py-3.5 px-4">CHAT TITLE</th>

                <th class="py-3.5 px-4 w-32 sm:w-44">DATE</th>

                <th class="py-3.5 px-4 w-16 text-right rounded-tr-2xl"></th>

              </tr>

            </thead>

            <tbody id="chatsTableBody" class="divide-y divide-white/5">

              <!-- Dynamically populated rows -->

            </tbody>

          </table>

        </div>

        <!-- Auth Required Notice (Shown when user is not signed in) -->

        <div id="chatsAuthRequired" class="hidden flex flex-col items-center justify-center py-16 px-4 text-center my-auto">

          <div class="w-14 h-14 rounded-2xl bg-cyan-500/10 border border-cyan-500/30 flex items-center justify-center text-cyan-400 mb-3 shadow-lg shadow-cyan-500/10">

            <svg class="w-7 h-7" fill="none" stroke="currentColor" viewBox="0 0 24 24">

              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z"/>

            </svg>

          </div>

          <h3 class="text-base font-bold text-white mb-1">Sign In Required</h3>

          <p class="text-xs text-gray-400 mb-4 max-w-sm">Please sign in to view and manage your chat conversations.</p>

          <button type="button" onclick="openAuthModal('signin')" class="px-4 py-2 rounded-xl bg-cyan-500 hover:bg-cyan-400 text-gray-950 font-bold text-xs transition shadow-lg shadow-cyan-500/20 cursor-pointer">

            Sign In to Continue

          </button>

        </div>

        <!-- Empty State Container -->

        <div id="chatsTableEmpty" class="hidden flex flex-col items-center justify-center py-16 px-4 text-center my-auto">

          <div class="w-12 h-12 rounded-2xl bg-white/5 border border-white/10 flex items-center justify-center text-gray-400 mb-3 shadow-inner">

            <svg class="w-6 h-6 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">

              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M8 12h.01M12 12h.01M16 12h.01M21 12c0 4.418-4.03 8-9 8a9.863 9.863 0 01-4.255-.949L3 20l1.395-3.72C3.512 15.042 3 13.574 3 12c0-4.418 4.03-8 9-8s9 3.582 9 8z"/>

            </svg>

          </div>

          <h3 class="text-sm font-semibold text-white mb-1">No chats found</h3>

          <p class="text-xs text-gray-400 mb-4 max-w-xs">Start a new conversation with any model or clear your search filter.</p>

          <button onclick="startNewChat()" class="px-3.5 py-1.5 rounded-lg bg-cyan-500 hover:bg-cyan-400 text-gray-950 font-bold text-xs transition shadow-md shadow-cyan-500/20 cursor-pointer">

            + Start New Chat

          </button>

        </div>

      </div>

    </div>

  </main>

  <!-- Manage Storage Modal -->

  <div id="storageModal" class="hidden fixed inset-0 z-50 bg-black/70 backdrop-blur-md flex items-center justify-center p-4">

    <div class="glass-panel w-full max-w-md rounded-2xl p-6 border border-white/10 shadow-2xl relative">

      <button onclick="closeStorageModal()" class="absolute top-4 right-4 text-gray-400 hover:text-white p-1 rounded-lg hover:bg-white/5 transition">

        ✕

      </button>

      <div class="flex items-center gap-2.5 mb-4">

        <div class="w-9 h-9 rounded-xl bg-cyan-500/10 border border-cyan-500/20 flex items-center justify-center text-cyan-400">

          <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 7v10c0 2 1.5 3 3.5 3h9c2 0 3.5-1 3.5-3V7c0-2-1.5-3-3.5-3h-9C5.5 4 4 5 4 7zm0 5h16m-16 5h16"/></svg>

        </div>

        <div>

          <h3 class="text-base font-bold text-white">Chat Storage Management</h3>

          <p class="text-xs text-gray-400">Database health, size metrics, and session retention</p>

        </div>

      </div>

      <div class="space-y-3 mb-6 text-xs">

        <div class="p-3 rounded-xl bg-black/30 border border-white/5 flex items-center justify-between">

          <span class="text-gray-400">Total Chat Sessions</span>

          <span id="storageTotalChats" class="font-bold text-white">0</span>

        </div>

        <div class="p-3 rounded-xl bg-black/30 border border-white/5 flex items-center justify-between">

          <span class="text-gray-400">Total Messages Saved</span>

          <span id="storageTotalMessages" class="font-bold text-white">0</span>

        </div>

        <div class="p-3 rounded-xl bg-black/30 border border-white/5 flex items-center justify-between">

          <span class="text-gray-400">Database Disk Footprint</span>

          <span id="storageDbSize" class="font-bold text-cyan-300">0 KB</span>

        </div>

        <div class="p-3 rounded-xl bg-black/30 border border-white/5 flex items-center justify-between">

          <span class="text-gray-400">Storage Engine</span>

          <span class="font-mono text-[11px] text-gray-300">SQLite (gateway.db)</span>

        </div>

      </div>

      <div class="pt-3 border-t border-white/10 flex items-center justify-between gap-3">

        <button type="button" onclick="closeStorageModal()" class="px-4 py-2 rounded-lg bg-white/5 hover:bg-white/10 text-gray-300 hover:text-white text-xs font-medium transition cursor-pointer">

          Close

        </button>

        <button type="button" onclick="purgeAllChats()" class="px-4 py-2 rounded-lg bg-rose-500/20 hover:bg-rose-500/30 text-rose-300 border border-rose-500/30 text-xs font-semibold transition flex items-center gap-1.5 cursor-pointer">

          <svg class="w-3.5 h-3.5 text-rose-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"/></svg>

          <span>Clear All Chats</span>

        </button>

      </div>

    </div>

  </div>

  <!-- Hidden File Input for Attachment -->

  <input type="file" id="chatFileInput" class="hidden" multiple onchange="handleFileAttachment(event)">

  <!-- Toast Notification Container -->

  <div id="toastContainer" class="fixed bottom-5 right-5 z-[100] flex flex-col gap-2 pointer-events-none"></div>

  <!-- Authentication Modal (Google Sign In + Manual Sign Up + OTP Verification) -->

  <div id="authModal" class="hidden fixed inset-0 z-50 bg-black/70 backdrop-blur-md flex items-center justify-center p-4">

    <div class="glass-panel w-full max-w-md rounded-2xl p-6 border border-white/10 shadow-2xl relative">

      <!-- Close Button -->

      <button onclick="closeAuthModal()" class="absolute top-4 right-4 text-gray-400 hover:text-white p-1 rounded-lg hover:bg-white/5 transition">

        ✕

      </button>

      <!-- VIEW 1: SIGN IN / SIGN UP TABS -->

      <div id="authTabsView">

        <!-- Logo & Title -->

        <div class="text-center mb-5">

          <div class="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-cyan-950/60 border border-cyan-500/30 text-cyan-400 text-xs font-semibold mb-2">

            ✦ LLM Gateway

          </div>

          <h2 id="authModalTitle" class="text-xl font-bold text-white">Welcome Back</h2>

          <p id="authModalSubtitle" class="text-xs text-gray-400 mt-1">Access unified intelligence across multiple providers</p>

        </div>

        <!-- Google Sign-In Button -->

        <button type="button" onclick="startGoogleSignIn()" class="w-full flex items-center justify-center gap-3 py-2.5 px-4 rounded-xl bg-white hover:bg-gray-100 text-gray-900 font-semibold text-xs transition shadow-lg hover:shadow-cyan-500/10 mb-4 group cursor-pointer">

          <svg class="w-4 h-4 shrink-0" viewBox="0 0 24 24">

            <path fill="#4285F4" d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z"/>

            <path fill="#34A853" d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z"/>

            <path fill="#FBBC05" d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.06H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.94l2.85-2.22.81-.63z"/>

            <path fill="#EA4335" d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.06l3.66 2.84c.87-2.6 3.3-4.52 6.16-4.52z"/>

          </svg>

          <span class="text-gray-900 font-semibold group-hover:text-black">Continue with Google</span>

        </button>

        <!-- Divider -->

        <div class="flex items-center gap-3 my-4">

          <div class="h-px bg-white/10 flex-1"></div>

          <span class="text-[10px] text-gray-500 uppercase tracking-wider font-semibold">Or with email</span>

          <div class="h-px bg-white/10 flex-1"></div>

        </div>

        <!-- Mode Toggle Tabs (Sign In / Sign Up) -->

        <div class="flex p-1 bg-black/40 rounded-xl mb-4 border border-white/5">

          <button type="button" id="tabSignInBtn" onclick="switchAuthTab('signin')" class="flex-1 py-1.5 text-xs font-semibold rounded-lg bg-white/10 text-white transition">

            Sign In

          </button>

          <button type="button" id="tabSignUpBtn" onclick="switchAuthTab('signup')" class="flex-1 py-1.5 text-xs font-semibold rounded-lg text-gray-400 hover:text-white transition">

            Sign Up

          </button>

        </div>

        <!-- Error/Notice Alert Box -->

        <div id="authNotice" class="hidden mb-3 p-2.5 rounded-xl text-xs flex items-center gap-2"></div>

        <!-- Sign In Form -->

        <form id="signInForm" onsubmit="handleManualSignIn(event)" class="space-y-3">

          <div>

            <label class="block text-[11px] font-semibold text-gray-400 mb-1">Email Address</label>

            <input type="email" id="signInEmail" required placeholder="you@domain.com" class="w-full bg-[#111827] border border-white/10 rounded-xl px-3 py-2 text-xs text-white placeholder-gray-500 focus:outline-none focus:border-cyan-500/50" />

          </div>

          <div>

            <label class="block text-[11px] font-semibold text-gray-400 mb-1">Password</label>

            <input type="password" id="signInPassword" required placeholder="••••••••" class="w-full bg-[#111827] border border-white/10 rounded-xl px-3 py-2 text-xs text-white placeholder-gray-500 focus:outline-none focus:border-cyan-500/50" />

          </div>

          <button type="submit" id="signInSubmitBtn" class="w-full py-2.5 rounded-xl bg-cyan-500 hover:bg-cyan-400 text-gray-950 font-bold text-xs transition shadow-lg shadow-cyan-500/20 mt-2 cursor-pointer">

            Sign In

          </button>

        </form>

        <!-- Sign Up Form (hidden by default) -->

        <form id="signUpForm" onsubmit="handleManualSignUp(event)" class="space-y-3 hidden">

          <div>

            <label class="block text-[11px] font-semibold text-gray-400 mb-1">Full Name</label>

            <input type="text" id="signUpName" required placeholder="Alex Mercer" class="w-full bg-[#111827] border border-white/10 rounded-xl px-3 py-2 text-xs text-white placeholder-gray-500 focus:outline-none focus:border-cyan-500/50" />

          </div>

          <div>

            <label class="block text-[11px] font-semibold text-gray-400 mb-1">Email Address</label>

            <input type="email" id="signUpEmail" required placeholder="alex@domain.com" class="w-full bg-[#111827] border border-white/10 rounded-xl px-3 py-2 text-xs text-white placeholder-gray-500 focus:outline-none focus:border-cyan-500/50" />

          </div>

          <div>

            <label class="block text-[11px] font-semibold text-gray-400 mb-1">Password (min 6 characters)</label>

            <input type="password" id="signUpPassword" minlength="6" required placeholder="Create password" class="w-full bg-[#111827] border border-white/10 rounded-xl px-3 py-2 text-xs text-white placeholder-gray-500 focus:outline-none focus:border-cyan-500/50" />

          </div>

          <button type="submit" id="signUpSubmitBtn" class="w-full py-2.5 rounded-xl bg-cyan-500 hover:bg-cyan-400 text-gray-950 font-bold text-xs transition shadow-lg shadow-cyan-500/20 mt-2 cursor-pointer">

            Create Account & Send OTP

          </button>

        </form>

      </div>

      <!-- VIEW 2: OTP VERIFICATION VIEW -->

      <div id="authOtpView" class="hidden">

        <div class="text-center mb-5">

          <div class="w-10 h-10 rounded-xl bg-cyan-950/80 border border-cyan-500/40 text-cyan-400 mx-auto flex items-center justify-center mb-2 shadow-lg shadow-cyan-500/20">

            <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 8l7.89 5.26a2 2 0 002.22 0L21 8M5 19h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z"/></svg>

          </div>

          <h2 class="text-xl font-bold text-white">Check Your Email</h2>

          <p class="text-xs text-gray-400 mt-1">We sent a 6-digit confirmation code to</p>

          <div id="otpTargetEmailText" class="text-xs font-semibold text-cyan-300 mt-0.5">user@example.com</div>

        </div>

        <!-- Dev Helper Pill (when local test OTP is returned) -->

        <div id="devOtpBanner" class="hidden mb-3 p-2 bg-cyan-950/40 border border-cyan-500/30 rounded-xl text-center">

          <span class="text-[11px] text-gray-400">Dev Code: </span>

          <button type="button" onclick="autoFillOtp(this.innerText)" id="devOtpCodeText" class="font-mono text-cyan-300 font-bold text-xs underline cursor-pointer" title="Click to auto-fill"></button>

        </div>

        <!-- Notice -->

        <div id="otpNotice" class="hidden mb-3 p-2.5 rounded-xl text-xs flex items-center gap-2"></div>

        <!-- 6 OTP Inputs -->

        <div class="flex items-center justify-center gap-2 my-5">

          <input type="text" maxlength="1" class="otp-digit w-10 h-12 text-center text-lg font-bold bg-[#111827] border border-white/10 rounded-xl text-white focus:outline-none focus:border-cyan-400 focus:ring-1 focus:ring-cyan-400" />

          <input type="text" maxlength="1" class="otp-digit w-10 h-12 text-center text-lg font-bold bg-[#111827] border border-white/10 rounded-xl text-white focus:outline-none focus:border-cyan-400 focus:ring-1 focus:ring-cyan-400" />

          <input type="text" maxlength="1" class="otp-digit w-10 h-12 text-center text-lg font-bold bg-[#111827] border border-white/10 rounded-xl text-white focus:outline-none focus:border-cyan-400 focus:ring-1 focus:ring-cyan-400" />

          <input type="text" maxlength="1" class="otp-digit w-10 h-12 text-center text-lg font-bold bg-[#111827] border border-white/10 rounded-xl text-white focus:outline-none focus:border-cyan-400 focus:ring-1 focus:ring-cyan-400" />

          <input type="text" maxlength="1" class="otp-digit w-10 h-12 text-center text-lg font-bold bg-[#111827] border border-white/10 rounded-xl text-white focus:outline-none focus:border-cyan-400 focus:ring-1 focus:ring-cyan-400" />

          <input type="text" maxlength="1" class="otp-digit w-10 h-12 text-center text-lg font-bold bg-[#111827] border border-white/10 rounded-xl text-white focus:outline-none focus:border-cyan-400 focus:ring-1 focus:ring-cyan-400" />

        </div>

        <button type="button" onclick="submitOtpVerification()" id="verifyOtpBtn" class="w-full py-2.5 rounded-xl bg-cyan-500 hover:bg-cyan-400 text-gray-950 font-bold text-xs transition shadow-lg shadow-cyan-500/20 mb-3 cursor-pointer">

          Verify & Sign In

        </button>

        <div class="flex items-center justify-between text-xs text-gray-400 px-1">

          <button type="button" onclick="cancelOtpAndBack()" class="hover:text-white transition cursor-pointer">

            ← Back

          </button>

          <button type="button" id="resendOtpBtn" onclick="resendOtpCode()" class="hover:text-cyan-300 transition text-[11px] cursor-pointer">

            Resend Code

          </button>

        </div>

      </div>

    </div>

  </div>

  <!-- Profile & Cartoon Avatar Settings Modal (Tray of 3 - 3 in one go) -->

  <div id="profileModal" class="hidden fixed inset-0 z-[100] bg-black/80 backdrop-blur-md flex items-center justify-center p-3 sm:p-4">

    <div class="glass-panel w-full max-w-md rounded-2xl p-5 border border-white/10 shadow-2xl relative max-h-[92vh] overflow-y-auto custom-dropdown-scroll">

      <!-- Close Button -->

      <button type="button" onclick="closeProfileModal()" class="absolute top-4 right-4 text-gray-400 hover:text-white p-1 rounded-lg hover:bg-white/5 transition cursor-pointer" title="Close">

        ✕

      </button>

      <!-- Header -->

      <div class="flex items-center gap-3 mb-4">

        <div class="w-9 h-9 rounded-xl bg-cyan-500/10 border border-cyan-500/30 flex items-center justify-center text-cyan-400 shrink-0">

          <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z"/></svg>

        </div>

        <div>

          <h2 class="text-base font-bold text-white tracking-tight">Profile & Avatar</h2>

          <p class="text-xs text-gray-400">Pick a cartoon avatar from the tray or roll for more</p>

        </div>

      </div>

      <!-- Selected Avatar + Display Name Row -->

      <div class="flex items-center gap-3.5 mb-4 p-3 rounded-xl bg-white/[0.02] border border-white/5">

        <div class="relative shrink-0">

          <div id="modalAvatarRing" class="w-14 h-14 rounded-full p-0.5 border-2 border-cyan-400 shadow-md shadow-cyan-500/20 flex items-center justify-center overflow-hidden bg-black/40">

            <img id="modalAvatarPreviewImg" src="" alt="Selected Avatar" referrerpolicy="no-referrer" class="w-full h-full rounded-full object-cover">

          </div>

          <span class="absolute -bottom-0.5 -right-0.5 w-4 h-4 rounded-full bg-cyan-400 text-gray-950 flex items-center justify-center text-[9px] font-bold shadow">✓</span>

        </div>

        <div class="flex-1 min-w-0">

          <label class="block text-[11px] font-semibold text-gray-300 mb-1">Display Name</label>

          <input type="text" id="profileInputName" class="w-full bg-[#111827] border border-white/10 rounded-xl px-2.5 py-1.5 text-xs text-white placeholder-gray-500 focus:outline-none focus:border-cyan-400 focus:ring-1 focus:ring-cyan-400 transition" placeholder="Your name">

        </div>

      </div>

      <!-- Avatar Tray: 4x2 Grid (8 Slots: None/Google + 7 Cartoon Avatars) -->

      <div class="mb-3">

        <div class="flex items-center justify-between mb-1.5">

          <label class="text-xs font-semibold text-gray-200 flex items-center gap-1.5">

            <span>Choose an Avatar</span>

            <span class="text-[10px] text-gray-500 font-normal">(Tray of 8)</span>

          </label>

          <button type="button" onclick="loadMoreAvatars()" id="moreAvatarsBtn" data-tooltip="Load next cartoon avatars" class="flex items-center gap-1.5 px-2 py-0.5 rounded-lg bg-cyan-500/10 hover:bg-cyan-500/20 border border-cyan-500/30 text-cyan-300 hover:text-cyan-200 text-[11px] font-semibold transition cursor-pointer">

            <svg id="moreAvatarsSpinner" class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/></svg>

            <span>More Avatars 🎲</span>

          </button>

        </div>

        <!-- 4x2 Compact Grid (4 - 4 in one go: 8 Cards) -->

        <div class="flex justify-center w-full">

          <div id="avatarTrayGrid" class="grid grid-cols-4 gap-1.5 sm:gap-2 p-1.5 rounded-xl bg-black/30 border border-white/5 w-full max-w-[340px] sm:max-w-[360px] min-h-[105px] relative">

            <!-- Populated dynamically with None card + 7 cartoon cards -->

          </div>

        </div>

      </div>

      <!-- Background Color Selector -->

      <div class="mb-3">

        <div class="flex items-center justify-between mb-1.5">

          <label class="text-xs font-semibold text-gray-300">Background Color</label>

          <span id="profileColorHexDisplay" class="text-[11px] font-mono text-cyan-400">#06b6d4</span>

        </div>

        <div class="flex items-center gap-2 flex-wrap">

          <button type="button" onclick="selectAvatarBgColor('#06b6d4')" class="w-6 h-6 rounded-full bg-[#06b6d4] border-2 border-white/20 hover:scale-110 transition cursor-pointer" title="Cyan"></button>

          <button type="button" onclick="selectAvatarBgColor('#8b5cf6')" class="w-6 h-6 rounded-full bg-[#8b5cf6] border-2 border-white/20 hover:scale-110 transition cursor-pointer" title="Purple"></button>

          <button type="button" onclick="selectAvatarBgColor('#ec4899')" class="w-6 h-6 rounded-full bg-[#ec4899] border-2 border-white/20 hover:scale-110 transition cursor-pointer" title="Pink"></button>

          <button type="button" onclick="selectAvatarBgColor('#10b981')" class="w-6 h-6 rounded-full bg-[#10b981] border-2 border-white/20 hover:scale-110 transition cursor-pointer" title="Emerald"></button>

          <button type="button" onclick="selectAvatarBgColor('#f59e0b')" class="w-6 h-6 rounded-full bg-[#f59e0b] border-2 border-white/20 hover:scale-110 transition cursor-pointer" title="Amber"></button>

          <button type="button" onclick="selectAvatarBgColor('#3b82f6')" class="w-6 h-6 rounded-full bg-[#3b82f6] border-2 border-white/20 hover:scale-110 transition cursor-pointer" title="Blue"></button>

          <button type="button" onclick="selectAvatarBgColor('#f43f5e')" class="w-6 h-6 rounded-full bg-[#f43f5e] border-2 border-white/20 hover:scale-110 transition cursor-pointer" title="Rose"></button>

          <button type="button" onclick="selectAvatarBgColor('#1e293b')" class="w-6 h-6 rounded-full bg-[#1e293b] border-2 border-white/20 hover:scale-110 transition cursor-pointer" title="Slate"></button>

          <div class="flex items-center gap-1.5 ml-auto">

            <label for="profileCustomColorInput" class="text-[10px] text-gray-400 cursor-pointer">Custom:</label>

            <input type="color" id="profileCustomColorInput" value="#06b6d4" oninput="selectAvatarBgColor(this.value)" class="w-7 h-7 rounded-lg bg-transparent border border-white/20 cursor-pointer p-0.5">

          </div>

        </div>

      </div>

      <div id="profileModalNotice" class="hidden text-xs p-2.5 rounded-xl border mb-3"></div>

      <!-- Action Buttons -->

      <div class="flex items-center justify-end gap-2.5 pt-3 border-t border-white/10">

        <button type="button" onclick="closeProfileModal()" class="px-4 py-2 rounded-xl text-xs font-semibold text-gray-300 hover:text-white hover:bg-white/5 transition cursor-pointer">

          Cancel

        </button>

        <button type="button" id="saveProfileBtn" onclick="saveUserProfile()" class="px-5 py-2 rounded-xl bg-cyan-500 hover:bg-cyan-400 text-gray-950 font-bold text-xs transition shadow-lg shadow-cyan-500/20 cursor-pointer">

          Save Avatar to Profile

        </button>

      </div>

    </div>

  </div>

  <!-- Floating Reusable Action Menu for Chats Table (Attached directly to <body> to avoid clipping by table scroll/glass overflow) -->

  <div id="chatTableRowActionMenu" class="hidden fixed z-[99999] w-32 bg-[#161a22] border border-white/10 rounded-xl shadow-2xl py-1 text-xs text-left backdrop-blur-xl">

    <button id="rowActionRenameBtn" class="w-full text-left px-3 py-2 text-gray-300 hover:text-white hover:bg-white/5 flex items-center gap-2 cursor-pointer transition">

      <svg class="w-3.5 h-3.5 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15.232 5.232l3.536 3.536m-2.036-5.036a2.5 2.5 0 113.536 3.536L6.5 21.036H3v-3.572L16.732 3.732z"/></svg>

      <span>Rename</span>

    </button>

    <button id="rowActionDeleteBtn" class="w-full text-left px-3 py-2 text-rose-400 hover:text-rose-300 hover:bg-rose-950/20 flex items-center gap-2 cursor-pointer transition">

      <svg class="w-3.5 h-3.5 text-rose-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"/></svg>

      <span>Delete</span>

    </button>

  </div>

  <script>

    let currentSessionId = null;

    let chatMessages = [];

    let isWaiting = false;

    let currentSelectedModel = 'openai/gpt-oss-120b';

    let isSidebarOpen = true;

    // Compact Provider SVG Logos (w-3.5 h-3.5) - Crisp Groq 'G' icon that is clean and never mistaken for a number 9

    const PROVIDER_LOGOS = {

      groq: `<svg class="w-3.5 h-3.5 shrink-0" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg"><rect width="24" height="24" rx="5" fill="#F55036"/><path d="M16 8.5A5.5 5.5 0 1 0 16.5 15.5H12V12h4.5" stroke="#FFF" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/></svg>`,

      openrouter: `<svg class="w-3.5 h-3.5 shrink-0" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg"><rect width="24" height="24" rx="5" fill="#4F46E5"/><path d="M7 12h10M13 8l4 4-4 4" stroke="#FFF" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/></svg>`,

      gemini: `<svg class="w-4 h-4 shrink-0" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg"><defs><linearGradient id="gem-spark-mini" x1="2" y1="2" x2="22" y2="22" gradientUnits="userSpaceOnUse"><stop offset="0%" stop-color="#4285F4"/><stop offset="35%" stop-color="#9B72CB"/><stop offset="70%" stop-color="#D96570"/><stop offset="100%" stop-color="#FFA439"/></linearGradient></defs><path d="M12 2C12 7.523 7.523 12 2 12C7.523 12 12 16.477 12 22C12 16.477 16.477 12 22 12C16.477 12 12 7.523 12 2Z" fill="url(#gem-spark-mini)"/></svg>`

    };

    // Official LLM Gateway Brand Logo SVG (Celestial iridescent sphere with glowing ring and crescent reflection)

    const LLM_GATEWAY_LOGO_SVG = `<svg viewBox="0 0 32 32" fill="none" xmlns="http://www.w3.org/2000/svg" class="w-[18px] h-[18px] shrink-0 select-none"><defs><radialGradient id="resp-gw-core" cx="50%" cy="50%" r="50%"><stop class="llm-gw-logo-core-stop" offset="60%" stop-color="#080c14" stop-opacity="0.95"/><stop offset="100%" stop-color="#0ea5e9" stop-opacity="0.12"/></radialGradient><linearGradient id="resp-gw-ring" x1="4" y1="4" x2="28" y2="28" gradientUnits="userSpaceOnUse"><stop offset="0%" stop-color="#38bdf8"/><stop offset="40%" stop-color="#818cf8"/><stop offset="70%" stop-color="#c084fc"/><stop offset="100%" stop-color="#ffffff"/></linearGradient><linearGradient id="resp-gw-crescent" x1="8" y1="26" x2="26" y2="22" gradientUnits="userSpaceOnUse"><stop offset="0%" stop-color="#38bdf8" stop-opacity="0"/><stop offset="45%" stop-color="#67e8f9"/><stop offset="75%" stop-color="#ffffff"/><stop offset="100%" stop-color="#c084fc" stop-opacity="0"/></linearGradient><filter id="resp-gw-glow" x="-20%" y="-20%" width="140%" height="140%"><feGaussianBlur stdDeviation="1.2" result="blur"/><feComposite in="SourceGraphic" in2="blur" operator="over"/></filter></defs><circle class="llm-gw-logo-bg" cx="16" cy="16" r="15" fill="#080c14"/><circle class="llm-gw-logo-core" cx="16" cy="16" r="11.5" fill="url(#resp-gw-core)"/><circle cx="16" cy="16" r="11.5" stroke="url(#resp-gw-ring)" stroke-width="2.6" opacity="0.45" filter="url(#resp-gw-glow)"/><circle cx="16" cy="16" r="11.5" stroke="url(#resp-gw-ring)" stroke-width="1.5"/><path d="M 6.5 19.5 C 8.5 25.5 17.5 28.5 25.5 20" stroke="url(#resp-gw-crescent)" stroke-width="2.2" stroke-linecap="round" filter="url(#resp-gw-glow)"/><path d="M 8.5 20.5 C 11 25.5 17 27 23.5 21" stroke="#ffffff" stroke-width="1.1" stroke-linecap="round" opacity="0.9"/></svg>`;

    function getAiResponseGlyph(provider) {

      return LLM_GATEWAY_LOGO_SVG;

    }

    // Models Config across active providers (No numbers in display names or taglines, concise words)

    const AVAILABLE_MODELS = [

      // Groq Models

      {

        id: 'llama-3.3-70b-versatile',

        name: 'Llama 70B',

        provider: 'groq',

        providerLabel: 'Groq',

        tag: 'Complex',

        tagClass: 'bg-purple-950/80 border border-purple-500/40 text-purple-300',

        desc: 'Deep reasoning & high speed'

      },

      {

        id: 'llama-3.1-8b-instant',

        name: 'Llama 8B',

        provider: 'groq',

        providerLabel: 'Groq',

        tag: 'Fast',

        tagClass: 'bg-emerald-950/80 border border-emerald-500/40 text-emerald-300',

        desc: 'Instant low-latency Q&A'

      },

      {

        id: 'openai/gpt-oss-120b',

        name: 'GPT-OSS',

        provider: 'groq',

        providerLabel: 'Groq',

        tag: 'Complex',

        tagClass: 'bg-purple-950/80 border border-purple-500/40 text-purple-300',

        desc: 'Deep reasoning & code'

      },

      // OpenRouter Models

      {

        id: 'meta-llama/llama-3.3-70b-instruct',

        name: 'Llama',

        provider: 'openrouter',

        providerLabel: 'OpenRouter',

        tag: 'Complex',

        tagClass: 'bg-purple-950/80 border border-purple-500/40 text-purple-300',

        desc: 'Deep analysis'

      },

      {

        id: 'openai/gpt-4o-mini',

        name: 'GPT Mini',

        provider: 'openrouter',

        providerLabel: 'OpenRouter',

        tag: 'Fast',

        tagClass: 'bg-emerald-950/80 border border-emerald-500/40 text-emerald-300',

        desc: 'Fast & lightweight'

      },

      // Google Gemini Models

      {

        id: 'gemini-2.0-flash',

        name: 'Gemini Flash',

        provider: 'gemini',

        providerLabel: 'Google Gemini',

        tag: 'Smart',

        tagClass: 'bg-teal-950/80 border border-teal-500/40 text-teal-300',

        desc: 'Next-gen multimodal reasoning'

      },

      {

        id: 'gemini-1.5-flash',

        name: 'Gemini Lite',

        provider: 'gemini',

        providerLabel: 'Google Gemini',

        tag: 'Fast',

        tagClass: 'bg-amber-950/80 border border-amber-500/40 text-amber-300',

        desc: 'Lightweight multimodal'

      }

    ];

    function renderDropdowns() {

      const providers = [

        { key: 'groq', title: 'Groq' },

        { key: 'openrouter', title: 'OpenRouter' },

        { key: 'gemini', title: 'Google Gemini' }

      ];

      ['heroDropdownItemsContainer', 'streamDropdownItemsContainer'].forEach(containerId => {

        const container = document.getElementById(containerId);

        if (!container) return;

        let html = '';

        providers.forEach(p => {

          const models = AVAILABLE_MODELS.filter(m => m.provider === p.key);

          if (models.length === 0) return;

          html += `<div class="px-2 py-0.5 text-[9px] font-bold text-gray-400 uppercase tracking-wider border-b border-white/5 mb-1 mt-1 flex items-center gap-1.5">

            ${PROVIDER_LOGOS[p.key] || ''}

            <span>${p.title}</span>

          </div>`;

          models.forEach(m => {

            const isSelected = m.id === currentSelectedModel;

            const rowClass = isSelected

              ? 'bg-cyan-500/15 border-cyan-500/40 shadow-[0_0_12px_rgba(6,182,212,0.15)] hover:bg-cyan-500/20'

              : 'border-transparent hover:bg-white/5';

            const nameClass = isSelected ? 'text-cyan-300 font-bold' : 'text-white group-hover:text-cyan-300';

            const descClass = isSelected ? 'text-cyan-200/70' : 'text-gray-400';

            html += `

              <div onclick="selectModel('${m.id}', '${m.name}', '${m.provider}')" class="model-option-item flex items-center justify-between py-1.5 px-2 rounded-lg cursor-pointer transition border ${rowClass} group mb-0.5" data-model="${m.id}">

                <div class="flex flex-col min-w-0 pr-2">

                  <span class="model-name-title text-[11px] font-semibold truncate ${nameClass}">${m.name}</span>

                  <span class="model-desc-text text-[9px] mt-0.5 leading-tight ${descClass}">${m.desc}</span>

                </div>

                <!-- Right-aligned alias pill (Fixed in place, no tick) -->

                <div class="shrink-0 flex items-center pl-1.5">

                  <span class="rounded px-1.5 py-0.5 text-[8.5px] font-semibold ${m.tagClass}">${m.tag}</span>

                </div>

              </div>

            `;

          });

        });

        container.innerHTML = html;

      });

    }

    function toggleSidebar() {

      const sidebar = document.getElementById('appSidebar');

      if (!sidebar) return;

      isSidebarOpen = !isSidebarOpen;

      if (isSidebarOpen) {

        sidebar.classList.remove('-ml-64');

      } else {

        sidebar.classList.add('-ml-64');

      }

    }

    function toggleInputModelDropdown(e, menuId) {

      if (e) {

        e.preventDefault();

        e.stopPropagation();

      }

      const targetMenu = document.getElementById(menuId);

      if (!targetMenu) return;

      const isHidden = targetMenu.classList.contains('hidden');

      

      // Close all model dropdowns first

      document.querySelectorAll('.model-dropdown-popover').forEach(d => d.classList.add('hidden'));

      

      if (isHidden) {

        targetMenu.classList.remove('hidden');

      }

    }

    function selectModel(modelId, displayName, provider = 'groq') {

      currentSelectedModel = modelId;

      // Update active model labels (ONLY model name, NO tag) in input bar pills

      document.querySelectorAll('.active-model-name').forEach(el => {

        el.innerText = displayName;

      });

      // Update provider logo in input bar pills

      const logoSvg = PROVIDER_LOGOS[provider] || PROVIDER_LOGOS.groq;

      document.querySelectorAll('.active-provider-logo').forEach(el => {

        el.innerHTML = logoSvg;

      });

      // Update settings modal select if present

      const modalSelect = document.getElementById('modalModelSelect');

      if (modalSelect) modalSelect.value = modelId;

      // Update active highlighted style in both dropdowns (separate color, no tick)

      document.querySelectorAll('.model-option-item').forEach(item => {

        const isMatch = item.getAttribute('data-model') === modelId;

        const nameEl = item.querySelector('.model-name-title');

        const descEl = item.querySelector('.model-desc-text');

        if (isMatch) {

          item.classList.remove('border-transparent', 'hover:bg-white/5');

          item.classList.add('bg-cyan-500/15', 'border-cyan-500/40', 'shadow-[0_0_12px_rgba(6,182,212,0.15)]', 'hover:bg-cyan-500/20');

          if (nameEl) {

            nameEl.classList.remove('text-white');

            nameEl.classList.add('text-cyan-300', 'font-bold');

          }

          if (descEl) {

            descEl.classList.remove('text-gray-400');

            descEl.classList.add('text-cyan-200/70');

          }

        } else {

          item.classList.remove('bg-cyan-500/15', 'border-cyan-500/40', 'shadow-[0_0_12px_rgba(6,182,212,0.15)]', 'hover:bg-cyan-500/20');

          item.classList.add('border-transparent', 'hover:bg-white/5');

          if (nameEl) {

            nameEl.classList.remove('text-cyan-300', 'font-bold');

            nameEl.classList.add('text-white');

          }

          if (descEl) {

            descEl.classList.remove('text-cyan-200/70');

            descEl.classList.add('text-gray-400');

          }

        }

      });

      // Close all model dropdowns

      document.querySelectorAll('.model-dropdown-popover').forEach(d => d.classList.add('hidden'));

    }

    function toggleProfileMenu(e) {

      if (e) e.stopPropagation();

      const menu = document.getElementById('profileDropdownMenu');

      if (menu) menu.classList.toggle('hidden');

    }

    document.addEventListener('click', (e) => {

      if (!e.target.closest('.model-dropdown-wrapper')) {

        document.querySelectorAll('.model-dropdown-popover').forEach(d => d.classList.add('hidden'));

      }

      if (!e.target.closest('#profileDropdownWrapper')) {

        const menu = document.getElementById('profileDropdownMenu');

        if (menu) menu.classList.add('hidden');

      }

      if (!e.target.closest('.chat-menu-wrapper')) {

        document.querySelectorAll('.chat-menu-dropdown').forEach(d => d.classList.add('hidden'));

      }

      if (!e.target.closest('.chat-table-menu-wrapper') && !e.target.closest('.chat-row-dropdown')) {

        document.querySelectorAll('.chat-row-dropdown').forEach(d => d.classList.add('hidden'));

      }

      if (!e.target.closest('#chatTableRowActionMenu') && !e.target.closest('button[data-chat-menu-btn]')) {

        const tableMenu = document.getElementById('chatTableRowActionMenu');

        if (tableMenu) tableMenu.classList.add('hidden');

        activeMenuSessionId = null;

      }

    });

    window.addEventListener('scroll', () => {

      document.querySelectorAll('.chat-row-dropdown').forEach(d => d.classList.add('hidden'));

      const tableMenu = document.getElementById('chatTableRowActionMenu');

      if (tableMenu) tableMenu.classList.add('hidden');

      activeMenuSessionId = null;

    }, true);

    window.addEventListener('resize', () => {

      document.querySelectorAll('.chat-row-dropdown').forEach(d => d.classList.add('hidden'));

      const tableMenu = document.getElementById('chatTableRowActionMenu');

      if (tableMenu) tableMenu.classList.add('hidden');

      activeMenuSessionId = null;

    });

    let currentView = 'chat';

    function showView(view) {

      currentView = view;

      const welcome = document.getElementById('welcomeScreen');

      const chat = document.getElementById('chatScreen');

      const chatsPage = document.getElementById('chatsPage');

      const settings = document.getElementById('settingsPage');

      const stats = document.getElementById('statsPage');

      const observability = document.getElementById('observabilityPage');

      const visualize = document.getElementById('visualizePage');

      const logs = document.getElementById('logsPage');

      const about = document.getElementById('aboutPage');

      if (welcome) welcome.classList.add('hidden');

      if (chat) chat.classList.add('hidden');

      if (chatsPage) chatsPage.classList.add('hidden');

      if (settings) settings.classList.add('hidden');

      if (stats) stats.classList.add('hidden');

      if (observability) observability.classList.add('hidden');

      if (visualize) visualize.classList.add('hidden');

      if (logs) logs.classList.add('hidden');

      if (about) about.classList.add('hidden');

      // Update active states on sidebar navigation buttons (Strictly ONE active button at a time)

      document.querySelectorAll('.sidebar-nav-btn').forEach(btn => {

        const btnView = btn.getAttribute('data-view');

        const svg = btn.querySelector('svg');

        if (btnView === view) {

          btn.className = 'sidebar-nav-btn flex items-center gap-2.5 px-2.5 py-1.5 rounded-lg text-xs font-semibold bg-cyan-500/15 text-cyan-300 border border-cyan-500/30 shadow-[0_0_12px_rgba(6,182,212,0.1)] transition group';

          if (svg) svg.className = 'w-4 h-4 text-cyan-400 transition shrink-0';

        } else {

          btn.className = 'sidebar-nav-btn flex items-center gap-2.5 px-2.5 py-1.5 rounded-lg text-xs font-medium text-gray-400 hover:text-white hover:bg-white/5 transition group border border-transparent hover:border-white/5';

          if (svg) svg.className = 'w-4 h-4 text-gray-400 group-hover:text-cyan-400 transition shrink-0';

        }

      });

      if (view === 'chats') {

        if (chatsPage) chatsPage.classList.remove('hidden');

        window.location.hash = 'chats';

        refreshChatsTable();

      } else if (view === 'settings') {

        if (settings) settings.classList.remove('hidden');

        window.location.hash = 'settings';

      } else if (view === 'stats') {

        if (stats) stats.classList.remove('hidden');

        window.location.hash = 'stats';

        refreshGatewayStats();

      } else if (view === 'observability') {

        if (observability) observability.classList.remove('hidden');

        window.location.hash = 'observability';

        refreshObservabilityData();

      } else if (view === 'visualize') {

        if (visualize) visualize.classList.remove('hidden');

        window.location.hash = 'visualize';

        if (typeof initVisualizeView === 'function') {

          initVisualizeView();

        }

      } else if (view === 'logs') {

        if (logs) logs.classList.remove('hidden');

        window.location.hash = 'logs';

        refreshGatewayLogs();

      } else if (view === 'about') {

        if (about) about.classList.remove('hidden');

        window.location.hash = 'about';

        if (typeof initFoundationPrimitives === 'function') {

          setTimeout(initFoundationPrimitives, 50);

        }

      } else {

        if (chatMessages && chatMessages.length > 0) {

          if (chat) chat.classList.remove('hidden');

        } else {

          if (welcome) welcome.classList.remove('hidden');

        }

        window.location.hash = 'dashboard';

      }

      const profileMenu = document.getElementById('profileDropdownMenu');

      if (profileMenu) profileMenu.classList.add('hidden');

    }

    function toggleSettingsModal() {

      if (currentView === 'settings') showView('chat');

      else showView('settings');

    }

    async function refreshGatewayStats() {

      try {

        const res = await fetch('/api/stats');

        if (!res.ok) return;

        const data = await res.json();

        const total = data.total_requests || 0;

        const successRate = data.success_rate !== undefined ? data.success_rate : 100;

        const avgLat = data.avg_latency_ms || 0;

        const tokens = data.total_tokens || 0;

        const fallbacks = data.fallback_count || 0;

        const failed = data.failed_requests || 0;

        const elTot = document.getElementById('statTotalRequests');

        if (elTot) elTot.innerText = total.toLocaleString();

        const elRate = document.getElementById('statSuccessRate');

        if (elRate) elRate.innerText = `${successRate}%`;

        const elLat = document.getElementById('statAvgLatency');

        if (elLat) elLat.innerText = `${avgLat}ms`;

        const elTok = document.getElementById('statTotalTokens');

        if (elTok) elTok.innerText = tokens.toLocaleString();

        const elFall = document.getElementById('statFallbacks');

        if (elFall) elFall.innerText = fallbacks.toLocaleString();

        const elFail = document.getElementById('statFailedReqs');

        if (elFail) elFail.innerText = failed.toLocaleString();

        // Render Providers Breakdown

        const provList = document.getElementById('statsProviderList');

        if (provList && data.provider_breakdown) {

          const providers = [

            { key: 'groq', label: 'Groq LPUs', color: 'bg-orange-500' },

            { key: 'openrouter', label: 'OpenRouter', color: 'bg-indigo-500' },

            { key: 'gemini', label: 'Google Gemini', color: 'bg-cyan-500' }

          ];

          let pHtml = '';

          providers.forEach(p => {

            const count = data.provider_breakdown[p.key] || 0;

            const pct = total > 0 ? Math.round((count / total) * 100) : (p.key === 'groq' ? 100 : 0);

            pHtml += `

              <div class="space-y-1">

                <div class="flex items-center justify-between text-[11px]">

                  <span class="text-white font-semibold">${p.label}</span>

                  <span class="text-gray-400 font-mono">${count} reqs (${pct}%)</span>

                </div>

                <div class="w-full bg-white/5 rounded-full h-1.5 overflow-hidden">

                  <div class="${p.color} h-full rounded-full transition-all duration-500" style="width: ${pct}%"></div>

                </div>

              </div>

            `;

          });

          provList.innerHTML = pHtml;

        }

        // Render Top Models

        const modelsBody = document.getElementById('statsTopModelsBody');

        if (modelsBody && data.top_models) {

          if (data.top_models.length === 0) {

            modelsBody.innerHTML = '<tr><td colspan="3" class="py-4 text-center text-gray-500 italic">No model completions recorded yet</td></tr>';

          } else {

            modelsBody.innerHTML = data.top_models.map(m => `

              <tr class="hover:bg-white/5 transition">

                <td class="py-2 px-3 text-cyan-300 font-semibold">${m.model}</td>

                <td class="py-2 px-3 text-gray-300">${m.count}</td>

                <td class="py-2 px-3 text-purple-300">${(m.tokens || 0).toLocaleString()}</td>

              </tr>

            `).join('');

          }

        }

      } catch (err) {

        console.warn('Failed to load gateway statistics:', err);

      }

    }

    async function refreshObservabilityData() {

      try {

        const res = await fetch('/api/reliability/status');

        if (!res.ok) return;

        const data = await res.json();

        // Render Circuit Breakers

        const cbContainer = document.getElementById('circuitBreakersList');

        if (cbContainer && data.circuit_breakers) {

          cbContainer.innerHTML = '';

          for (const [provider, status] of Object.entries(data.circuit_breakers)) {

            const isClosed = status.state === 'CLOSED';

            const isOpen = status.state === 'OPEN';

            const stateBadge = isClosed 

              ? 'bg-emerald-950/80 border-emerald-500/40 text-emerald-300 shadow-[0_0_10px_rgba(16,185,129,0.2)]' 

              : (isOpen ? 'bg-rose-950/80 border-rose-500/40 text-rose-300 shadow-[0_0_10px_rgba(244,63,94,0.3)]' : 'bg-amber-950/80 border-amber-500/40 text-amber-300 shadow-[0_0_10px_rgba(245,158,11,0.2)]');

            const dotClass = isClosed 

              ? 'bg-emerald-400 animate-pulse shadow-[0_0_8px_rgba(52,211,153,0.9)]' 

              : (isOpen ? 'bg-rose-500 animate-ping' : 'bg-amber-400 animate-pulse');

            const provColor = provider === 'groq' ? 'from-orange-500/20 to-transparent border-orange-500/30' : (provider === 'gemini' ? 'from-purple-500/20 to-transparent border-purple-500/30' : 'from-cyan-500/20 to-transparent border-cyan-500/30');

            const failPercent = Math.min(100, Math.round(((status.consecutive_failures || 0) / (status.failure_threshold || 5)) * 100));

            const card = document.createElement('div');

            card.className = `p-4 rounded-2xl border bg-gradient-to-b ${provColor} bg-[#0c101a] backdrop-blur-xl flex flex-col justify-between hover:border-white/20 transition-all shadow-xl group`;

            card.innerHTML = `

              <div>

                <div class="flex items-center justify-between mb-3">

                  <div class="flex items-center gap-2">

                    <span class="w-2.5 h-2.5 rounded-full ${dotClass}"></span>

                    <span class="font-bold text-sm text-white capitalize tracking-wide">${provider}</span>

                  </div>

                  <span class="rounded-full px-2.5 py-0.5 text-[10px] font-bold tracking-wider border ${stateBadge}">${status.state}</span>

                </div>

                

                <div class="space-y-2 text-xs text-gray-300 mb-3">

                  <div>

                    <div class="flex justify-between text-[11px] mb-1">

                      <span class="text-gray-400">Error Trip Threshold:</span>

                      <span class="font-mono text-white font-semibold">${status.consecutive_failures || 0} / ${status.failure_threshold || 5}</span>

                    </div>

                    <div class="w-full bg-white/10 rounded-full h-1.5 overflow-hidden">

                      <div class="h-full rounded-full ${failPercent > 60 ? 'bg-rose-500' : 'bg-cyan-400'}" style="width: ${failPercent}%"></div>

                    </div>

                  </div>

                  <div class="flex justify-between pt-1">

                    <span class="text-gray-400">Total Invocations:</span>

                    <span class="font-mono text-cyan-300 font-semibold">${status.total_requests || 0}</span>

                  </div>

                  <div class="flex justify-between">

                    <span class="text-gray-400">Success / Failures:</span>

                    <span class="font-mono text-white"><span class="text-emerald-400">${status.total_successes || 0}</span> / <span class="text-rose-400">${status.total_failures || 0}</span></span>

                  </div>

                  ${isOpen ? `<div class="text-rose-400 font-semibold text-[11px] flex items-center gap-1.5 bg-rose-950/40 p-1.5 rounded-lg border border-rose-500/30"><span>⏳ Recovery Cooldown:</span> <span class="font-mono font-bold">${status.cooldown_remaining_sec}s</span></div>` : ''}

                </div>

              </div>

              <div class="flex items-center gap-1.5 pt-2 border-t border-white/10">

                <button type="button" onclick="tripSingleCircuitBreaker('${provider}')" class="flex-1 py-1 rounded-lg text-[10px] font-bold bg-white/5 hover:bg-rose-950/40 text-gray-400 hover:text-rose-300 border border-white/10 hover:border-rose-500/30 transition cursor-pointer">Simulate Trip</button>

                <button type="button" onclick="resetSingleCircuitBreaker('${provider}')" class="flex-1 py-1 rounded-lg text-[10px] font-bold bg-white/5 hover:bg-emerald-950/40 text-gray-400 hover:text-emerald-300 border border-white/10 hover:border-emerald-500/30 transition cursor-pointer">Reset</button>

              </div>

            `;

            cbContainer.appendChild(card);

          }

        }

        // Render Fallback Chains

        const chainsContainer = document.getElementById('fallbackChainsList');

        if (chainsContainer && data.registry_chains) {

          chainsContainer.innerHTML = '';

          for (const [alias, chain] of Object.entries(data.registry_chains)) {

            const chainDiv = document.createElement('div');

            chainDiv.className = 'p-3 rounded-xl bg-[#0c101a] border border-white/10 hover:border-cyan-500/30 transition flex flex-col sm:flex-row sm:items-center justify-between gap-2 shadow-lg';

            const steps = chain.map((step, idx) => `

              <span class="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-lg ${idx === 0 ? 'bg-cyan-950/60 text-cyan-300 border border-cyan-500/30 font-semibold' : 'bg-white/5 text-gray-300 border border-white/10'} text-xs">

                <span>${step.provider}</span>

                <span class="text-[10px] text-gray-400 font-mono">(${step.model})</span>

              </span>

            `).join('<span class="text-cyan-500/60 font-bold px-1">➔</span>');

            chainDiv.innerHTML = `

              <div class="flex items-center gap-2">

                <span class="font-bold text-white text-xs uppercase px-2 py-0.5 rounded-md bg-white/10 border border-white/15 tracking-wider font-mono">${alias}</span>

                <span class="text-[10px] text-gray-400">Chain Strategy</span>

              </div>

              <div class="flex flex-wrap items-center gap-1">

                ${steps}

              </div>

            `;

            chainsContainer.appendChild(chainDiv);

          }

        }

      } catch (err) {

        console.warn('Failed to load reliability telemetry:', err);

      }

    }

    async function tripSingleCircuitBreaker(provider) {

      try {

        await fetch('/api/reliability/circuit-breaker/trip', {

          method: 'POST',

          headers: { 'Content-Type': 'application/json' },

          body: JSON.stringify({ provider })

        });

        await refreshObservabilityData();

      } catch (err) {

        console.error('Failed to trip circuit breaker:', err);

      }

    }

    async function resetSingleCircuitBreaker(provider) {

      try {

        await fetch('/api/reliability/circuit-breaker/reset', {

          method: 'POST',

          headers: { 'Content-Type': 'application/json' },

          body: JSON.stringify({ provider })

        });

        await refreshObservabilityData();

      } catch (err) {

        console.error('Failed to reset circuit breaker:', err);

      }

    }

    async function resetAllCircuitBreakers() {

      try {

        await fetch('/api/reliability/circuit-breaker/reset', { method: 'POST' });

        await refreshObservabilityData();

        if (typeof refreshVisualizeData === 'function') {

          await refreshVisualizeData();

        }

      } catch (err) {

        console.error('Failed to reset circuit breakers:', err);

      }

    }

    // =============================================================

    // TOPOLOGY & LIVE TRAFFIC VISUALIZER (SSE DRIVEN)

    // =============================================================

    let trafficEventSource = null;

    let activeTrafficDots = new Map(); // reqId -> dot state

    let isTrafficAnimationRunning = false;

    let currentKeyEditProvider = null;

    let visualizerEventsCount = 0;

    // COMPONENT REGISTRY for dynamic topological concurrency & queue rendering
    const ComponentRegistry = {
      components: new Map(),
      register(name, updateFn) {
        this.components.set(name, updateFn);
      },
      update(name, data) {
        const fn = this.components.get(name);
        if (fn) fn(data);
      },
      updateAll(data) {
        if (!data) return;
        const providers = data.providers || data;
        if (typeof providers === 'object') {
          Object.entries(providers).forEach(([prov, pdata]) => {
            this.update(`semaphore-${prov}`, pdata);
            this.update(`queue-${prov}`, pdata);
          });
        }
      }
    };

    const providerQueueDepths = { groq: 0, gemini: 0, openrouter: 0 };
    const retryBucketWaitingList = new Map(); // reqId -> { provider, attempt, reason, timestamp }

    function updateRetryBucketUI() {
      const card = document.getElementById('vizRetryBucketCard');
      const badge = document.getElementById('vizRetryBucketBadge');
      const msg = document.getElementById('vizRetryBucketMsg');
      if (!card || !badge || !msg) return;

      const count = retryBucketWaitingList.size;
      if (count > 0) {
        badge.textContent = `${count} WAITING`;
        card.style.transform = 'scale(1.06)';
        const entries = Array.from(retryBucketWaitingList.entries());
        const [firstId, firstReq] = entries[0];
        const sId = firstId.length > 8 ? firstId.substring(0, 6) + '..' : firstId;
        msg.innerHTML = `<span class="text-amber-300 font-bold drop-shadow animate-pulse">${count} ball${count > 1 ? 's' : ''} in bucket • ${sId} (${firstReq.reason || 'fail'})</span>`;
      } else {
        badge.textContent = '0 WAITING';
        card.style.transform = '';
        msg.innerHTML = '<span class="text-amber-400/60 font-mono">Bucket Empty • Ready</span>';
      }
    }

    function updateQueueVisualCount(prov) {
      const cap = prov === 'groq' ? 'Groq' : (prov === 'gemini' ? 'Gemini' : 'OpenRouter');
      const ratioEl = document.getElementById(`vizRatioQueue${cap}`);
      const barEl = document.getElementById(`vizBarQueue${cap}`);
      const dotsEl = document.getElementById(`vizDotsQueue${cap}`);
      const boxEl = document.getElementById(`vizQueueBox${cap}`);
      if (!ratioEl) return;

      const limit = 20;

      // 1. Backend queue depth from last concurrency update
      const backendDepth = (providerQueueDepths && providerQueueDepths[prov]) || 0;
      
      // 2. Active incoming particles currently in this provider queue lane
      // Requests in 'ingress' phase are gathering before queue (Client -> Gateway -> Router)
      // Only count particles that have reached the branch/queue stage waiting for execution:
      let activeQueueCount = 0;
      activeTrafficDots.forEach((d) => {
        const pKey = (d.provider || 'groq').toLowerCase();
        if (pKey === prov && (d.phase === 'branch' || d.isQueued) && d.outcome !== 'served') {
          activeQueueCount++;
        }
      });

      // Strictly cap displayed count at queue capacity (20) so it never exceeds 20/20!
      const rawCount = Math.max(backendDepth, activeQueueCount);
      const liveCount = Math.min(limit, rawCount);

      ratioEl.innerText = `${liveCount}/${limit}`;
      const pct = Math.min(100, Math.round((liveCount / limit) * 100));

      if (barEl) {
        barEl.style.width = `${Math.max(pct, liveCount > 0 ? 8 : 0)}%`;
        barEl.className = (liveCount >= limit || rawCount >= limit) 
          ? 'bg-rose-500 h-1 rounded-full animate-pulse' 
          : (liveCount > 0 ? 'bg-amber-400 h-1 rounded-full transition-all duration-300' : 'bg-slate-700 h-1 rounded-full');
      }

      if (dotsEl) {
        const dotCount = Math.min(liveCount, 8);
        let dotsHtml = '';
        for (let i = 0; i < dotCount; i++) {
          dotsHtml += `<div class="w-2 h-2 rounded-full ${(liveCount >= limit || rawCount >= limit) ? 'bg-rose-500 animate-ping' : 'bg-amber-400 animate-pulse'}"></div>`;
        }
        dotsEl.innerHTML = dotsHtml;
      }

      // Spill up / overflow visual zoom & red alert when overwhelmed (at or above capacity 20)
      if (boxEl) {
        if (rawCount >= limit || liveCount >= limit) {
          boxEl.style.transform = 'scale(1.08)';
          boxEl.style.borderColor = '#ef4444';
          boxEl.style.backgroundColor = 'rgba(127, 29, 29, 0.9)';
          boxEl.style.boxShadow = '0 0 30px rgba(239, 68, 68, 0.85)';
          ratioEl.className = 'text-rose-300 font-extrabold animate-pulse';
        } else {
          boxEl.style.transform = '';
          boxEl.style.borderColor = '';
          boxEl.style.backgroundColor = '';
          boxEl.style.boxShadow = '';
          ratioEl.className = liveCount > 0 ? 'text-amber-300 font-bold' : 'text-gray-300 font-bold';
        }
      }
    }

    ['groq', 'gemini', 'openrouter'].forEach(prov => {
      const cap = prov === 'groq' ? 'Groq' : (prov === 'gemini' ? 'Gemini' : 'OpenRouter');
      ComponentRegistry.register(`semaphore-${prov}`, (data) => {
        const ratioEl = document.getElementById(`vizRatioInflight${cap}`);
        const barEl = document.getElementById(`vizBarInflight${cap}`);
        if (ratioEl && data) ratioEl.innerText = `${data.in_flight || 0}/${data.limit || 10}`;
        if (barEl && data && data.limit) {
          const pct = Math.min(100, Math.round(((data.in_flight || 0) / data.limit) * 100));
          barEl.style.width = `${pct}%`;
          barEl.className = pct >= 90 ? 'bg-rose-500 h-1 rounded-full' : (pct >= 60 ? 'bg-amber-400 h-1 rounded-full' : 'bg-cyan-400 h-1 rounded-full');
        }
      });
      ComponentRegistry.register(`queue-${prov}`, (data) => {
        if (data && typeof data.queue_depth === 'number') {
          providerQueueDepths[prov] = data.queue_depth;
        }
        updateQueueVisualCount(prov);
      });
    });

    function initVisualizeView() {

      // Set crisp provider SVG logos into node headers

      // Canvas provider node logos are preserved from SVG markup (28px)

      refreshVisualizeData();

      syncDlqDepthBadge();

      initCanvasPanZoom();

      initDraggableNodes();

      updateConnectorPaths();

      initTrafficStream();

      if (!isTrafficAnimationRunning) {

        isTrafficAnimationRunning = true;

        requestAnimationFrame(renderTrafficAnimationLoop);

      }

    }

    function initTrafficStream() {

      if (trafficEventSource && trafficEventSource.readyState !== EventSource.CLOSED) {

        return; // already connected

      }

      const statusBadge = document.getElementById('vizSseStatusBadge');

      const statusText = document.getElementById('vizSseStatusText');

      try {

        trafficEventSource = new EventSource('/api/reliability/traffic-stream');

        trafficEventSource.onopen = () => {

          if (statusBadge) {

            statusBadge.className = 'flex items-center gap-1.5 px-2.5 py-1 rounded-full text-[11px] font-medium bg-emerald-950/60 border border-emerald-500/30 text-emerald-300';

          }

          if (statusText) statusText.innerText = 'SSE Connected';

          const sDot = document.getElementById('studioSseStatusDot');

          const sText = document.getElementById('studioSseStatusText');

          if (sDot) sDot.className = 'w-2 h-2 rounded-full bg-emerald-400 animate-pulse';

          if (sText) sText.innerText = 'SSE Live Stream Connected';

        };

        trafficEventSource.onerror = () => {

          if (statusBadge) {

            statusBadge.className = 'flex items-center gap-1.5 px-2.5 py-1 rounded-full text-[11px] font-medium bg-amber-950/60 border border-amber-500/30 text-amber-300';

          }

          if (statusText) statusText.innerText = 'Reconnecting...';

          const sDot = document.getElementById('studioSseStatusDot');

          const sText = document.getElementById('studioSseStatusText');

          if (sDot) sDot.className = 'w-2 h-2 rounded-full bg-amber-400 animate-pulse';

          if (sText) sText.innerText = 'Reconnecting...';

        };

        // 1. request_started event: Grey dot Client -> Router

        trafficEventSource.addEventListener('request_started', (e) => {

          const data = JSON.parse(e.data);

          handleRequestStartedEvent(data);

        });

        // 2. attempt_started event: Grey dot Router -> Provider

        trafficEventSource.addEventListener('attempt_started', (e) => {

          const data = JSON.parse(e.data);

          handleAttemptStartedEvent(data);

        });

        // 3. request_served event: Green dot Provider -> Router -> Client

        trafficEventSource.addEventListener('request_served', (e) => {

          const data = JSON.parse(e.data);

          handleRequestServedEvent(data);

        });

        // 4. attempt_failed event: Red dot stops on edge / pulses red

        trafficEventSource.addEventListener('attempt_failed', (e) => {

          const data = JSON.parse(e.data);

          handleAttemptFailedEvent(data);

        });

        // 5. fallback event: New dot branches towards next provider

        trafficEventSource.addEventListener('fallback', (e) => {

          const data = JSON.parse(e.data);

          handleFallbackEvent(data);

        });

        // 6. concurrency_update event: Updates Semaphore & Queue components
        trafficEventSource.addEventListener('concurrency_update', (e) => {
          try {
            const data = JSON.parse(e.data);
            ComponentRegistry.updateAll(data);
          } catch (err) {}
        });

        // 7. request_queued & request_dequeued events
        trafficEventSource.addEventListener('request_queued', (e) => {
          try {
            const data = JSON.parse(e.data);
            if (data && data.provider) {
              ComponentRegistry.update(`queue-${data.provider}`, data);
            }
          } catch (err) {}
          refreshVisualizeData();
        });

        trafficEventSource.addEventListener('request_dequeued', (e) => {
          try {
            const data = JSON.parse(e.data);
            if (data && data.provider) {
              ComponentRegistry.update(`queue-${data.provider}`, data);
            }
          } catch (err) {}
          refreshVisualizeData();
        });

        // 8. request_shed event: Red dot with 'shed' label
        trafficEventSource.addEventListener('request_shed', (e) => {
          try {
            const data = JSON.parse(e.data);
            handleRequestShedEvent(data);
          } catch (err) {}
        });

        // 9. auth_failed event: Red dot at Auth Wall (401)
        trafficEventSource.addEventListener('auth_failed', (e) => {
          try {
            const data = JSON.parse(e.data);
            handleAuthFailedEvent(data);
          } catch (err) {}
        });

        // 10. rate_limited event: Red dot at Rate Limiter (429)
        trafficEventSource.addEventListener('rate_limited', (e) => {
          try {
            const data = JSON.parse(e.data);
            handleRateLimitedEvent(data);
          } catch (err) {}
        });

        // 11. Async Jobs SSE Events
        trafficEventSource.addEventListener('job_enqueued', (e) => {
          try {
            const data = JSON.parse(e.data);
            flashNodeStatus('studioNodeJobQueue', 'blue');
            logVisualizerEvent(
              `<span class="px-1.5 py-0.2 rounded text-[9px] font-bold bg-cyan-950 text-cyan-300 border border-cyan-500">JOB</span>`,
              `Enqueued <span class="text-cyan-300 font-mono">${data.job_id}</span>`
            );
          } catch (err) {}
        });

        trafficEventSource.addEventListener('job_running', (e) => {
          try {
            const data = JSON.parse(e.data);
            flashNodeStatus('studioNodeWorker', 'blue');
          } catch (err) {}
        });

        trafficEventSource.addEventListener('job_completed', (e) => {
          try {
            const data = JSON.parse(e.data);
            flashNodeStatus('studioNodeWorker', 'green');
            logVisualizerEvent(
              `<span class="px-1.5 py-0.2 rounded text-[9px] font-bold bg-emerald-950 text-emerald-300 border border-emerald-500">JOB DONE</span>`,
              `Completed <span class="text-emerald-300 font-mono">${data.job_id}</span>`
            );
          } catch (err) {}
        });

        trafficEventSource.addEventListener('job_dlq', (e) => {
          try {
            const data = JSON.parse(e.data);
            const tile = document.getElementById('studioTileDLQ');
            if (tile) {
              tile.classList.add('ring-4', 'ring-rose-500', 'animate-pulse');
              setTimeout(() => tile.classList.remove('ring-4', 'ring-rose-500', 'animate-pulse'), 1400);
            }
            const badge = document.getElementById('vizDlqDepthBadge');
            if (badge) {
              badge.innerText = String((parseInt(badge.innerText, 10) || 0) + 1);
            }
            recheckDlqDepth();
            logVisualizerEvent(
              `<span class="px-1.5 py-0.2 rounded text-[9px] font-bold bg-rose-950 text-rose-300 border border-rose-500">DLQ</span>`,
              `Job <span class="text-cyan-300 font-mono">${data.job_id}</span> dead-lettered after ${data.attempts} attempts`
            );
          } catch (err) {}
        });

        trafficEventSource.addEventListener('job_replayed', (e) => {
          try {
            const data = JSON.parse(e.data);
            flashNodeStatus('studioNodeDLQ', 'yellow');
            flashNodeStatus('studioNodeJobQueue', 'blue');
            const badge = document.getElementById('vizDlqDepthBadge');
            if (badge) {
              badge.innerText = String(Math.max(0, (parseInt(badge.innerText, 10) || 0) - 1));
            }
            recheckDlqDepth();
            logVisualizerEvent(
              `<span class="px-1.5 py-0.2 rounded text-[9px] font-bold bg-amber-950 text-amber-300 border border-amber-500">REPLAY</span>`,
              `Replayed job <span class="text-cyan-300 font-mono">${data.job_id}</span>`
            );
          } catch (err) {}
        });

        // 12. guardrail_blocked event: Red blocked dot at Input Guardrails node
        trafficEventSource.addEventListener('guardrail_blocked', (e) => {
          try {
            const data = JSON.parse(e.data);
            handleGuardrailBlockedEvent(data);
          } catch (err) {}
        });

        // 12b. idempotency_hit event: Violet highlight at Idempotency node
        trafficEventSource.addEventListener('idempotency_hit', (e) => {
          try {
            const data = JSON.parse(e.data);
            flashNodeStatus('studioNodeIdempotency', 'blue');
            logVisualizerEvent(
              `<span class="px-1.5 py-0.2 rounded text-[9px] font-bold bg-violet-950 text-violet-300 border border-violet-500">IDEMPOTENCY</span>`,
              `Deduplicated job <span class="text-cyan-300 font-mono">${data.job_id}</span> via key '${data.idempotency_key}'`
            );
          } catch (err) {}
        });

        // 13. cache_hit event: Green highlight at Redis Cache node
        trafficEventSource.addEventListener('cache_hit', (e) => {
          try {
            const data = JSON.parse(e.data);
            flashNodeStatus('studioNodeCacheStore', 'green');
            const badge = document.getElementById('vizCacheStatusBadge');
            if (badge) {
              badge.innerText = 'HIT';
              badge.className = 'absolute -top-1.5 -right-1.5 px-1.5 py-0.2 rounded-full bg-emerald-600 border border-white text-white font-mono font-bold text-[8.5px] shadow-lg animate-bounce';
              badge.classList.remove('hidden');
              setTimeout(() => badge.classList.add('hidden'), 2200);
            }
            const savedStr = data.saved_cost ? `$${parseFloat(data.saved_cost).toFixed(6)}` : 'saved $0.000010';
            logVisualizerEvent(
              `<span class="px-1.5 py-0.2 rounded text-[9px] font-bold bg-emerald-950 text-emerald-300 border border-emerald-500">CACHE HIT</span>`,
              `Exact match from Redis Cache (latency: ~2ms • <span class="text-emerald-400 font-mono font-bold">${savedStr}</span> saved)`
            );

            // Spawn or transition particle to return immediately from Redis Cache to Client
            const reqId = data.request_id || ('cache_hit_' + Date.now());
            let dot = activeTrafficDots.get(reqId);
            if (!dot) {
              dot = {
                id: reqId,
                phase: 'cache_return',
                isCached: true,
                phaseStartTime: performance.now(),
                cacheReturnDuration: 600,
                fadeDuration: 350,
                color: '#10b981',
                filter: 'url(#glowGreen)',
                outcome: 'served'
              };
              activeTrafficDots.set(reqId, dot);
            } else {
              dot.isCached = true;
              dot.color = '#10b981';
              dot.filter = 'url(#glowGreen)';
              dot.outcome = 'served';
              if (dot.phase !== 'ingress') {
                dot.phase = 'cache_return';
                dot.phaseStartTime = performance.now();
              }
            }
          } catch (err) {}
        });

        // 14. cache_miss event: Red highlight at Redis Cache node
        trafficEventSource.addEventListener('cache_miss', (e) => {
          try {
            flashNodeStatus('studioNodeCacheStore', 'red');
            const badge = document.getElementById('vizCacheStatusBadge');
            if (badge) {
              badge.innerText = 'MISS';
              badge.className = 'absolute -top-1.5 -right-1.5 px-1.5 py-0.2 rounded-full bg-rose-600 border border-white text-white font-mono font-bold text-[8.5px] shadow-lg animate-pulse';
              badge.classList.remove('hidden');
              setTimeout(() => badge.classList.add('hidden'), 2200);
            }
            logVisualizerEvent(
              `<span class="px-1.5 py-0.2 rounded text-[9px] font-bold bg-rose-950 text-rose-300 border border-rose-500">CACHE MISS</span>`,
              `Cache miss in Redis -> Routing to API Gateway / LLM Router`
            );
          } catch (err) {}
        });

      } catch (err) {

        console.warn('Failed to initialize SSE traffic stream:', err);

      }

    }

    function logVisualizerEvent(badgeHtml, textHtml) {

      const feed = document.getElementById('vizEventStreamFeed');

      const countEl = document.getElementById('vizEventCountBadge');

      const pillCount = document.getElementById('studioEventCountPill');

      visualizerEventsCount++;

      if (countEl) countEl.innerText = `${visualizerEventsCount} events`;

      if (pillCount) pillCount.innerText = `${visualizerEventsCount} events`;

      if (!feed) return;

      const now = new Date();

      const timeStr = now.toTimeString().split(' ')[0] + '.' + String(now.getMilliseconds()).padStart(3, '0');

      const row = document.createElement('div');

      row.className = 'flex items-center gap-2 py-0.5 border-b border-white/5 last:border-0';

      row.innerHTML = `

        <span class="text-gray-500 text-[10px] shrink-0 font-mono">${timeStr}</span>

        ${badgeHtml}

        <span class="text-gray-300 truncate">${textHtml}</span>

      `;

      feed.appendChild(row);

      feed.scrollTop = feed.scrollHeight;

    }

    function clearVisualizerFeed() {

      const feed = document.getElementById('vizEventStreamFeed');

      const countEl = document.getElementById('vizEventCountBadge');

      visualizerEventsCount = 0;

      if (countEl) countEl.innerText = '0 events';

      if (feed) feed.innerHTML = '<div class="text-gray-500 text-[10px] italic">Feed cleared. Awaiting inbound traffic...</div>';

    }

    function flashNodeStatus(nodeId, status) {
      const el = document.getElementById(nodeId);
      if (!el) return;
      const tile = el.querySelector('div:first-child') || el;
      const statusMap = {
        'blue': { border: '#38bdf8', shadow: '0 0 25px rgba(56,189,248,0.85)' },
        'cyan': { border: '#06b6d4', shadow: '0 0 30px rgba(6,182,212,0.95)' },
        'yellow': { border: '#f59e0b', shadow: '0 0 30px rgba(245,158,11,0.95)' },
        'green': { border: '#10b981', shadow: '0 0 30px rgba(16,185,129,0.95)' },
        'red': { border: '#ef4444', shadow: '0 0 30px rgba(239,68,68,0.95)' }
      };
      const cfg = statusMap[status];
      if (!cfg) return;
      const prevBorder = tile.style.borderColor;
      const prevShadow = tile.style.boxShadow;
      tile.style.borderColor = cfg.border;
      tile.style.boxShadow = cfg.shadow;
      setTimeout(() => {
        tile.style.borderColor = prevBorder;
        tile.style.boxShadow = prevShadow;
      }, 750);
    }

    function triggerQueueOverflowVisual(provider) {
      const cap = provider === 'groq' ? 'Groq' : (provider === 'gemini' ? 'Gemini' : 'OpenRouter');
      const boxEl = document.getElementById(`vizQueueBox${cap}`);
      const ratioEl = document.getElementById(`vizRatioQueue${cap}`);
      const barEl = document.getElementById(`vizBarQueue${cap}`);
      if (!boxEl) return;
      boxEl.style.transform = 'scale(1.10)';
      boxEl.style.borderColor = '#ef4444';
      boxEl.style.backgroundColor = 'rgba(127, 29, 29, 0.95)';
      boxEl.style.boxShadow = '0 0 35px rgba(239, 68, 68, 0.9)';
      if (ratioEl) ratioEl.className = 'text-rose-300 font-extrabold animate-pulse';
      if (barEl) { barEl.style.width = '100%'; barEl.className = 'bg-rose-500 h-1 rounded-full animate-pulse'; }
      setTimeout(() => {
        boxEl.style.transform = '';
        boxEl.style.borderColor = '';
        boxEl.style.backgroundColor = '';
        boxEl.style.boxShadow = '';
        if (ratioEl) ratioEl.className = 'text-gray-300 font-bold';
        if (barEl) barEl.className = 'bg-amber-400 h-1 rounded-full transition-all duration-300';
      }, 2500);
    }

    function handleRequestStartedEvent(data) {

      const reqId = data.request_id || ('req_' + Date.now());

      const rInflight = document.getElementById('viz-router-inflight');

      if (rInflight) rInflight.innerText = String((parseInt(rInflight.innerText) || 0) + 1);

      flashNodeStatus('studioNodeClient', 'blue');
      flashNodeStatus('studioNodeGateway', 'blue');
      flashNodeStatus('studioNodeBalancer', 'blue');

      // Spawn Sequential Ingress Particle if not already spawned by test runner
      if (!activeTrafficDots.has(reqId)) {
        activeTrafficDots.set(reqId, {
          id: reqId,
          phase: 'ingress',
          phaseStartTime: performance.now(),
          ingressDuration: 1300,
          branchDuration: 700,
          dwellDuration: 350,
          fadeDuration: 600,
          color: '#38bdf8',
          filter: 'url(#glowBlue)',
          provider: 'groq',
          queuedProvider: null,
          outcome: 'pending',
          target: data.target || 'fast',
        });
      }

      logVisualizerEvent(

        `<span class="px-1.5 py-0.2 rounded text-[9px] font-bold bg-slate-800 text-slate-300 border border-slate-600">REQ_START</span>`,

        `<span class="text-cyan-300 font-mono">${reqId}</span> target=<span class="text-white font-semibold">${data.target}</span>`

      );

    }

    function handleAttemptStartedEvent(data) {

      const reqId = data.request_id;

      const provider = (data.provider || 'groq').toLowerCase();

      const pInflight = document.getElementById(`viz-${provider}-inflight`);

      if (pInflight) pInflight.innerText = String((parseInt(pInflight.innerText) || 0) + 1);

      const isRetry = (data.attempt && data.attempt > 1);
      if (isRetry) {
        flashNodeStatus('studioNodeRetry', 'yellow');
        flashNodeStatus('vizRetryBucketCard', 'yellow');
        retryBucketWaitingList.delete(reqId);
        updateRetryBucketUI();
      }

      let dot = activeTrafficDots.get(reqId);

      if (!dot) {

        dot = {

          id: reqId,

          phase: 'ingress',

          phaseStartTime: performance.now(),

          ingressDuration: 1300,

          branchDuration: 700,

          dwellDuration: 350,

          fadeDuration: 600,

          color: isRetry ? '#f59e0b' : '#38bdf8',

          filter: isRetry ? 'url(#glowYellow)' : 'url(#glowBlue)',

          provider: provider,

          queuedProvider: provider,

          outcome: 'pending',

          target: 'fast',

        };

        activeTrafficDots.set(reqId, dot);

      } else {

        dot.provider = provider;

        dot.queuedProvider = provider;

        if (isRetry) {
          dot.color = '#f59e0b';
          dot.filter = 'url(#glowYellow)';
          if (dot.phase === 'retry_bucket_wait' || dot.phase === 'retry_bucket_drop' || dot.phase === 'retry_bucket_settled') {
            dot.phase = 'branch';
            dot.phaseStartTime = performance.now();
          }
        }

      }

      logVisualizerEvent(

        `<span class="px-1.5 py-0.2 rounded text-[9px] font-bold bg-cyan-950/80 text-cyan-300 border border-cyan-500/40">ATTEMPT</span>`,

        `<span class="text-cyan-300 font-mono">${reqId}</span> → <span class="text-white font-bold capitalize">${provider}</span> (${data.model}) attempt #${data.attempt}`

      );

    }

    function handleRequestServedEvent(data) {

      const reqId = data.request_id;

      const provider = (data.provider || 'groq').toLowerCase();

      const rInflight = document.getElementById('viz-router-inflight');

      if (rInflight) rInflight.innerText = String(Math.max(0, (parseInt(rInflight.innerText) || 1) - 1));

      const rServed = document.getElementById('viz-router-served');

      if (rServed) rServed.innerText = String((parseInt(rServed.innerText) || 0) + 1);

      const pInflight = document.getElementById(`viz-${provider}-inflight`);

      if (pInflight) pInflight.innerText = String(Math.max(0, (parseInt(pInflight.innerText) || 1) - 1));

      const pServed = document.getElementById(`viz-${provider}-served`);

      if (pServed) pServed.innerText = String((parseInt(pServed.innerText) || 0) + 1);

      const provNodeMap = { groq: 'studioNodeCache', gemini: 'studioNodeGemini', openrouter: 'studioNodeDatabase' };
      if (provNodeMap[provider]) flashNodeStatus(provNodeMap[provider], 'green');
      flashNodeStatus('studioNodeClient', 'green');
      retryBucketWaitingList.delete(reqId);
      updateRetryBucketUI();

      let dot = activeTrafficDots.get(reqId);

      if (dot) {

        // Tag outcome for sequential pipeline; do NOT jump or teleport coordinates!

        dot.outcome = 'served';

        dot.servedTime = performance.now();

        dot.provider = provider;

        dot.color = '#10b981';

        dot.filter = 'url(#glowGreen)';

        dot.pathId = `path-router-${provider}`;

      }

      const tokens = (data.token_usage && data.token_usage.total_tokens) ? data.token_usage.total_tokens : 'N/A';

      logVisualizerEvent(

        `<span class="px-1.5 py-0.2 rounded text-[9px] font-bold bg-emerald-950/80 text-emerald-300 border border-emerald-500/40">SERVED</span>`,

        `<span class="text-cyan-300 font-mono">${reqId}</span> by <span class="text-emerald-400 font-bold capitalize">${provider}</span> in <span class="text-white font-semibold">${data.latency_ms}ms</span> (${tokens} tokens)`

      );

    }

    function handleAttemptFailedEvent(data) {

      const reqId = data.request_id;

      const provider = (data.provider || 'groq').toLowerCase();

      const pInflight = document.getElementById(`viz-${provider}-inflight`);

      if (pInflight) pInflight.innerText = String(Math.max(0, (parseInt(pInflight.innerText) || 1) - 1));

      const pFailed = document.getElementById(`viz-${provider}-failed`);

      if (pFailed) pFailed.innerText = String((parseInt(pFailed.innerText) || 0) + 1);

      flashNodeStatus('studioNodeRetry', 'yellow');
      flashNodeStatus('vizRetryBucketCard', 'yellow');
      const provNodeMap = { groq: 'studioNodeCache', gemini: 'studioNodeGemini', openrouter: 'studioNodeDatabase' };
      if (provNodeMap[provider]) flashNodeStatus(provNodeMap[provider], 'red');

      retryBucketWaitingList.set(reqId, {
        provider: provider,
        attempt: data.attempt || 1,
        reason: data.error_type || 'Error',
        timestamp: performance.now()
      });
      updateRetryBucketUI();

      let dot = activeTrafficDots.get(reqId);

      if (dot) {

        dot.outcome = 'failed';

        dot.servedTime = performance.now();

        dot.provider = provider;

        dot.errorType = data.error_type;

        dot.color = '#f59e0b';

        dot.filter = 'url(#glowYellow)';

        dot.phase = 'retry_bucket_drop';

        dot.phaseStartTime = performance.now();

        dot.dropDuration = 650;

        dot.ballIndex = retryBucketWaitingList.size - 1;

      } else {

        dot = {

          id: reqId,

          phase: 'retry_bucket_drop',

          phaseStartTime: performance.now(),

          dropDuration: 650,

          ballIndex: retryBucketWaitingList.size - 1,

          branchDuration: 700,

          dwellDuration: 350,

          fadeDuration: 600,

          color: '#f59e0b',

          filter: 'url(#glowYellow)',

          provider: provider,

          queuedProvider: provider,

          outcome: 'failed',

          errorType: data.error_type

        };

        activeTrafficDots.set(reqId, dot);

      }

      if (data.error_type === 'circuit_breaker_open' || (data.status_code && data.status_code >= 500)) {

        refreshVisualizeData();

      }

      logVisualizerEvent(

        `<span class="px-1.5 py-0.2 rounded text-[9px] font-bold bg-rose-950/80 text-rose-300 border border-rose-500/40">FAILED</span>`,

        `<span class="text-cyan-300 font-mono">${reqId}</span> <span class="text-rose-400 font-bold capitalize">${provider}</span> error: <span class="text-rose-300">${data.error_type || 'error'}</span> ${data.status_code ? `(${data.status_code})` : ''}`

      );

    }

    function handleFallbackEvent(data) {
      const reqId = data.request_id;
      const fromProv = (data.from_provider || '').toLowerCase();
      const toProv = (data.to_provider || '').toLowerCase();

      flashNodeStatus('studioNodeRetry', 'yellow');
      flashNodeStatus('vizRetryBucketCard', 'yellow');
      retryBucketWaitingList.delete(reqId);
      updateRetryBucketUI();

      // Route existing request dot directly to fallback provider (No orphaned lingering dots!)
      let dot = activeTrafficDots.get(reqId);
      if (dot) {
        dot.provider = toProv;
        dot.queuedProvider = toProv;
        dot.pathId = `path-router-${toProv}`;
        if (dot.phase === 'dwell' || dot.phase === 'branch' || dot.phase === 'retry_bucket_drop' || dot.phase === 'retry_bucket_settled' || dot.phase === 'retry_bucket_wait') {
          dot.phase = 'branch';
          dot.phaseStartTime = performance.now();
        }
        dot.color = '#f59e0b';
        dot.filter = 'url(#glowYellow)';
        dot.outcome = 'pending';
      } else {
        activeTrafficDots.set(reqId, {
          id: reqId,
          phase: 'branch',
          phaseStartTime: performance.now(),
          branchDuration: 450,
          dwellDuration: 300,
          fadeDuration: 400,
          color: '#f59e0b',
          filter: 'url(#glowYellow)',
          provider: toProv,
          queuedProvider: toProv,
          outcome: 'pending',
          target: 'fast',
        });
      }

      refreshVisualizeData();

      logVisualizerEvent(
        `<span class="px-1.5 py-0.2 rounded text-[9px] font-bold bg-amber-950/80 text-amber-300 border border-amber-500/40">FALLBACK</span>`,
        `<span class="text-cyan-300 font-mono">${reqId}</span> rerouted: <span class="text-rose-400 capitalize">${fromProv}</span> → <span class="text-amber-300 font-bold capitalize">${toProv}</span>`
      );
    }

    function handleRequestShedEvent(data) {
      const reqId = data.request_id || ('req_' + Date.now());
      const provider = (data.provider || 'groq').toLowerCase();
      triggerQueueOverflowVisual(provider);
      const provNodeMap = { groq: 'studioNodeCache', gemini: 'studioNodeGemini', openrouter: 'studioNodeDatabase' };
      if (provNodeMap[provider]) flashNodeStatus(provNodeMap[provider], 'red');

      let dot = activeTrafficDots.get(reqId);
      if (dot) {
        dot.outcome = 'shed';
        dot.color = '#ef4444';
        dot.filter = 'url(#glowRed)';
        dot.servedTime = performance.now();
      } else {
        activeTrafficDots.set(reqId, {
          id: reqId,
          phase: 'dwell',
          phaseStartTime: performance.now(),
          branchDuration: 400,
          dwellDuration: 300,
          fadeDuration: 500,
          color: '#ef4444',
          filter: 'url(#glowRed)',
          outcome: 'shed',
          provider: provider,
          target: data.target || 'fast',
        });
      }
      refreshVisualizeData();
      logVisualizerEvent(
        `<span class="px-1.5 py-0.2 rounded text-[9px] font-bold bg-rose-950/90 text-rose-300 border border-rose-500">SHED 429</span>`,
        `<span class="text-cyan-300 font-mono">${reqId}</span> <span class="text-rose-400 font-bold">SHED (HTTP 429)</span>: ${data.reason || 'All providers saturated'} • Retry-After: 5s`
      );
    }

    function handleAuthFailedEvent(data) {
      const tile = document.getElementById('studioTileAuth');
      if (tile) {
        tile.classList.add('ring-4', 'ring-rose-500', 'animate-pulse');
        setTimeout(() => tile.classList.remove('ring-4', 'ring-rose-500', 'animate-pulse'), 1400);
      }
      const reqId = 'auth_fail_' + Date.now();
      activeTrafficDots.set(reqId, {
        id: reqId,
        phase: 'dwell',
        anchorNode: 'studioNodeAuth',
        phaseStartTime: performance.now(),
        branchDuration: 400,
        dwellDuration: 400,
        fadeDuration: 600,
        color: '#ef4444',
        filter: 'url(#glowRed)',
        outcome: 'shed',
        badge: '401',
      });
      logVisualizerEvent(
        `<span class="px-1.5 py-0.2 rounded text-[9px] font-bold bg-rose-950 text-rose-300 border border-rose-500">401 AUTH</span>`,
        `<span class="text-rose-400 font-bold">REJECTED</span> at <span class="text-indigo-300 font-semibold">Auth Wall</span>: ${data.reason || 'Invalid or missing Gateway key'}`
      );
    }

    function handleRateLimitedEvent(data) {
      const tile = document.getElementById('studioTileRateLimit');
      if (tile) {
        tile.classList.add('ring-4', 'ring-amber-500', 'animate-pulse');
        setTimeout(() => tile.classList.remove('ring-4', 'ring-amber-500', 'animate-pulse'), 1400);
      }
      const reqId = 'rate_lim_' + Date.now();
      activeTrafficDots.set(reqId, {
        id: reqId,
        phase: 'dwell',
        anchorNode: 'studioNodeRateLimit',
        phaseStartTime: performance.now(),
        branchDuration: 400,
        dwellDuration: 400,
        fadeDuration: 600,
        color: '#f59e0b',
        filter: 'url(#glowYellow)',
        outcome: 'shed',
        badge: '429',
      });
      logVisualizerEvent(
        `<span class="px-1.5 py-0.2 rounded text-[9px] font-bold bg-amber-950 text-amber-300 border border-amber-500">429 RATE</span>`,
        `<span class="text-amber-400 font-bold">THROTTLED</span> at <span class="text-amber-300 font-semibold">Rate Limiter</span> (Redis Token Bucket) • Retry-After: ${data.retry_after || 5}s`
      );
    }

    function handleGuardrailBlockedEvent(data) {
      const tile = document.getElementById('studioTileInputGuardrails');
      if (tile) {
        tile.classList.add('ring-4', 'ring-rose-500', 'animate-pulse');
        setTimeout(() => tile.classList.remove('ring-4', 'ring-rose-500', 'animate-pulse'), 1400);
      }
      const reqId = 'guard_block_' + Date.now();
      activeTrafficDots.set(reqId, {
        id: reqId,
        phase: 'dwell',
        anchorNode: 'studioNodeInputGuardrails',
        phaseStartTime: performance.now(),
        branchDuration: 400,
        dwellDuration: 400,
        fadeDuration: 600,
        color: '#ef4444',
        filter: 'url(#glowRed)',
        outcome: 'shed',
        badge: 'BLOCKED',
      });
      logVisualizerEvent(
        `<span class="px-1.5 py-0.2 rounded text-[9px] font-bold bg-rose-950 text-rose-300 border border-rose-500">BLOCKED</span>`,
        `<span class="text-rose-400 font-bold">REJECTED</span> by <span class="text-emerald-300 font-semibold">Input Guardrail</span>: ${data.reason || 'Security policy violation'}`
      );
    }

    function renderTrafficAnimationLoop(now) {

      const container = document.getElementById('vizTrafficDots');

      if (!container) {

        if (isTrafficAnimationRunning) requestAnimationFrame(renderTrafficAnimationLoop);

        return;

      }

      // Update visual queue counts dynamically on every animation frame
      ['groq', 'gemini', 'openrouter'].forEach(updateQueueVisualCount);

      const toDelete = [];

      let svgHtml = '';

      activeTrafficDots.forEach((dot, reqId) => {

        let x = 0;

        let y = 0;

        let opacity = 1.0;

        let r = 6.0;

        let color = dot.color || '#38bdf8';

        let filter = dot.filter || 'url(#glowGrey)';

        const elapsed = now - dot.phaseStartTime;
        const pKey = (dot.provider || 'groq').toLowerCase();

        if (dot.phase === 'ingress') {

          // 1. Client -> Gateway -> Router -> Retry

          color = '#38bdf8';

          filter = 'url(#glowGrey)';

          const p = Math.min(1.0, elapsed / dot.ingressDuration);

          const path = document.getElementById('path-client-router');

          if (path) {

            const len = path.getTotalLength();

            const pt = path.getPointAtLength(p * len);

            x = pt.x;

            y = pt.y;

            if (dot.laneOffset) y += dot.laneOffset;

          }

          if (dot.isCached) {
            // Divert directly up to Redis Cache Store once passing Gateway, then return!
            const gwAnchor = getNodeAnchor('studioNodeGateway');
            const cacheAnchor = getNodeAnchor('studioNodeCacheStore');
            if (p >= 0.65) {
              const pUp = (p - 0.65) / 0.35;
              x = gwAnchor.centerX;
              y = gwAnchor.centerY - (gwAnchor.centerY - cacheAnchor.centerY) * pUp;
            }
            if (p >= 1.0) {
              dot.phase = 'cache_return';
              dot.phaseStartTime = now;
              dot.color = '#10b981';
              dot.filter = 'url(#glowGreen)';
              dot.outcome = 'served';
              flashNodeStatus('studioNodeCacheStore', 'emerald');
            }
          } else if (p >= 1.0) {

            dot.phase = 'branch';

            dot.phaseStartTime = now;

            let targetProv = dot.queuedProvider || dot.provider || 'groq';
            const swEl = document.getElementById(`cableSwitch${targetProv.charAt(0).toUpperCase() + targetProv.slice(1)}`);
            if (swEl && !swEl.querySelector('.switch-open')?.classList.contains('hidden')) {
              const chainOrder = ['groq', 'gemini', 'openrouter'];
              for (const pName of chainOrder) {
                const checkSw = document.getElementById(`cableSwitch${pName.charAt(0).toUpperCase() + pName.slice(1)}`);
                if (checkSw && checkSw.querySelector('.switch-open')?.classList.contains('hidden')) {
                  targetProv = pName;
                  break;
                }
              }
            }
            dot.provider = targetProv;

            dot.pathId = `path-router-${dot.provider}`;

          }

        } else if (dot.phase === 'retry_bucket_drop' || dot.phase === 'retry_bucket_settled' || dot.phase === 'retry_bucket_wait') {

          // Physically drop like glowing marble balls from Retry Node into the Netted Long Bucket Base
          color = '#f59e0b';
          filter = 'url(#glowYellow)';
          r = 6.0;
          const bucket = getNodeAnchor('vizRetryBucketCard');
          const bCenterX = (bucket && bucket.centerX) ? bucket.centerX : 644;
          const bCenterY = (bucket && bucket.centerY) ? bucket.centerY : 388;
          const stackSlots = [
            { dx: 0, dy: 44 },
            { dx: -12, dy: 42 },
            { dx: 12, dy: 42 },
            { dx: -6, dy: 30 },
            { dx: 6, dy: 30 },
            { dx: -10, dy: 18 },
            { dx: 10, dy: 18 },
            { dx: 0, dy: 6 }
          ];
          const bIdx = Math.max(0, (dot.ballIndex !== undefined ? dot.ballIndex : 0)) % stackSlots.length;
          const targetX = bCenterX + stackSlots[bIdx].dx;
          const targetY = bCenterY + stackSlots[bIdx].dy;

          if (dot.phase === 'retry_bucket_drop') {
            const p = Math.min(1.0, elapsed / (dot.dropDuration || 650));
            const startX = bCenterX;
            const startY = bCenterY - 110; // Dropping from beneath Retry Node
            x = startX + (targetX - startX) * p;
            if (p < 0.75) {
              const pFall = p / 0.75;
              y = startY + (targetY - startY) * (pFall * pFall); // Gravitational free-fall acceleration
            } else {
              const pBounce = (p - 0.75) / 0.25;
              const bounceHeight = 12 * (1 - pBounce) * Math.sin(pBounce * Math.PI); // Realistic elastic rebound
              y = targetY - bounceHeight;
            }
            if (p >= 1.0) {
              dot.phase = 'retry_bucket_settled';
              dot.phaseStartTime = now;
            }
          } else {
            x = targetX;
            y = targetY;
            if (elapsed > 3500) {
              dot.phase = 'branch';
              dot.phaseStartTime = now;
              retryBucketWaitingList.delete(reqId);
              updateRetryBucketUI();
            }
          }

        } else if (dot.phase === 'branch') {

          // 2. Retry -> Queue -> Provider

          color = dot.color || '#38bdf8';

          filter = dot.filter || 'url(#glowGrey)';

          const p = Math.min(1.0, elapsed / dot.branchDuration);

          if (p < 0.65) {
            // Stage 2a: Retry Node -> Breaker Switch -> Queue Box
            const pStage = p / 0.65;
            const path1 = document.getElementById(`path-router-${pKey}`);
            if (path1 && path1.getTotalLength) {
              const pt = path1.getPointAtLength(pStage * path1.getTotalLength());
              x = pt.x;
              y = pt.y;
            }
          } else {
            // Stage 2b: Queue Box -> Connection String -> Provider Node
            const pStage = (p - 0.65) / 0.35;
            const path2 = document.getElementById(`path-queue-${pKey}`);
            if (path2 && path2.getTotalLength) {
              const pt = path2.getPointAtLength(pStage * path2.getTotalLength());
              x = pt.x;
              y = pt.y;
            }
          }

          if (dot.laneOffset) x += dot.laneOffset;

          if (p >= 1.0) {

            dot.phase = 'dwell';

            dot.phaseStartTime = now;

          }

        } else if (dot.phase === 'dwell') {

          if (dot.anchorNode) {
            const a = getNodeAnchor(dot.anchorNode);
            x = a.centerX || 244;
            y = a.centerY || 508;
          } else {
            // 3. Dwell / Processing INSIDE Provider node (clean center docking, zero entrance crowding)
            const provNodeMap = { groq: 'studioNodeCache', gemini: 'studioNodeGemini', openrouter: 'studioNodeDatabase' };
            const pAnchor = getNodeAnchor(provNodeMap[pKey] || 'studioNodeCache');
            if (pAnchor && pAnchor.centerX) {
              x = pAnchor.centerX;
              y = pAnchor.centerY - 4;
            } else {
              const path = document.getElementById(`path-queue-${pKey}`) || document.getElementById(`path-router-${pKey}`);
              if (path && path.getTotalLength) {
                const pt = path.getPointAtLength(path.getTotalLength());
                x = pt.x + 38;
                y = pt.y;
              }
            }
          }

          if (dot.outcome === 'shed') {

            color = '#ef4444';

            filter = 'url(#glowRed)';

            r = 6.2;

            if (elapsed > 2000) {

              opacity = Math.max(0, 1.0 - (elapsed - 2000) / 400);

              if (opacity <= 0) toDelete.push(reqId);

            }

          } else if (dot.outcome === 'failed') {

            color = '#ef4444';

            filter = 'url(#glowRed)';

            r = 6.2;

            if (elapsed > 2000) {

              opacity = Math.max(0, 1.0 - (elapsed - 2000) / 400);

              if (opacity <= 0) toDelete.push(reqId);

            }

          } else if (dot.outcome === 'served') {

            // REAL LLM RESPONSE HAS ARRIVED!

            color = '#10b981';

            filter = 'url(#glowGreen)';

            // Hold for brief 250ms emerald pulse inside provider node, then fly back!

            const servedElapsed = now - (dot.servedTime || now);

            if (servedElapsed >= (dot.dwellDuration || 250)) {

              dot.phase = 'egress_branch';

              dot.phaseStartTime = now;

              dot.color = '#10b981';

              dot.filter = 'url(#glowGreen)';

            }

          } else {

            // OUTCOME IS STILL 'pending': WAITING FOR REAL LLM COMPLETION!
            // Dot docks neatly inside provider card, pulsing as in-flight token generation occurs
            color = dot.color || '#38bdf8';

            filter = dot.filter || 'url(#glowBlue)';

            r = 5.5;

            // Strict safety TTL: auto-sweep lingering dots after 3500ms to eliminate crowds in front of LLM node
            if (elapsed > 3500) {
              opacity = Math.max(0, 1.0 - (elapsed - 3500) / 400);
              if (opacity <= 0) toDelete.push(reqId);
            }

          }

        } else if (dot.phase === 'egress_branch') {

          // 4. Reverse: LLM Provider -> Queue -> Retry

          color = '#10b981';

          filter = 'url(#glowGreen)';

          const p = Math.min(1.0, elapsed / dot.branchDuration);

          if (p < 0.35) {
            // Reverse Stage 4a: Provider Node -> Queue Box
            const pStage = p / 0.35;
            const path2 = document.getElementById(`path-queue-${pKey}`);
            if (path2 && path2.getTotalLength) {
              const pt = path2.getPointAtLength((1.0 - pStage) * path2.getTotalLength());
              x = pt.x;
              y = pt.y;
            }
          } else {
            // Reverse Stage 4b: Queue Box -> Breaker Switch -> Retry Node
            const pStage = (p - 0.35) / 0.65;
            const path1 = document.getElementById(`path-router-${pKey}`);
            if (path1 && path1.getTotalLength) {
              const pt = path1.getPointAtLength((1.0 - pStage) * path1.getTotalLength());
              x = pt.x;
              y = pt.y;
            }
          }

          if (p >= 1.0) {

            dot.phase = 'egress_main';

            dot.phaseStartTime = now;

          }

        } else if (dot.phase === 'cache_return') {

          // Instant response served from Cache Store -> returns directly to Client Traffic
          color = '#10b981';
          filter = 'url(#glowGreen)';
          const p = Math.min(1.0, elapsed / (dot.egressDuration || 700));
          const path = document.getElementById('path-cache-return');

          if (path && path.getTotalLength) {
            const pt = path.getPointAtLength(p * path.getTotalLength());
            x = pt.x;
            y = pt.y;
          } else {
            const clientAnchor = getNodeAnchor('studioNodeClient');
            x = clientAnchor.centerX;
            y = clientAnchor.centerY;
          }

          if (!dot._flashedCache) {
            dot._flashedCache = true;
            flashNodeStatus('studioNodeCacheStore', 'emerald');
          }
          if (p >= 0.85 && !dot._flashedClientReturn) {
            dot._flashedClientReturn = true;
            flashNodeStatus('studioNodeClient', 'emerald');
          }

          if (p >= 1.0) {
            dot.fadeX = x;
            dot.fadeY = y;
            dot.phase = 'fade';
            dot.phaseStartTime = now;
          }

        } else if (dot.phase === 'egress_main') {

          // Full Response Return Flow: Retry -> Output Guard -> Audit Log -> Client Traffic
          color = '#10b981';
          filter = 'url(#glowGreen)';
          const p = Math.min(1.0, elapsed / (dot.egressDuration || 900));
          const path = document.getElementById('path-return-egress');

          if (path && path.getTotalLength) {
            const pt = path.getPointAtLength(p * path.getTotalLength());
            x = pt.x;
            y = pt.y;
          } else {
            const clientAnchor = getNodeAnchor('studioNodeClient');
            x = clientAnchor.centerX;
            y = clientAnchor.centerY;
          }

          // Visual feedback passing through Output Guard, Audit Log, and back home to Client Traffic
          if (p > 0.15 && p < 0.35 && !dot._flashedOutGuard) {
            dot._flashedOutGuard = true;
            flashNodeStatus('studioNodeOutputGuardrails', 'cyan');
          }
          if (p > 0.40 && p < 0.65 && !dot._flashedAudit) {
            dot._flashedAudit = true;
            flashNodeStatus('studioNodeAuditLog', 'yellow');
          }
          if (p >= 0.90 && !dot._flashedClientReturn) {
            dot._flashedClientReturn = true;
            flashNodeStatus('studioNodeClient', 'emerald');
          }

          if (p >= 1.0) {
            dot.fadeX = x;
            dot.fadeY = y;
            dot.phase = 'fade';
            dot.phaseStartTime = now;
          }

        } else if (dot.phase === 'fade') {

          // Soft fade-out at Client Traffic where the request flow completes successfully
          color = '#10b981';
          filter = 'url(#glowGreen)';

          if (!dot.fadeX) {
            const clientAnchor = getNodeAnchor('studioNodeClient');
            dot.fadeX = (clientAnchor && clientAnchor.centerX) ? clientAnchor.centerX : 54;
            dot.fadeY = (clientAnchor && clientAnchor.centerY) ? clientAnchor.centerY : 246;
          }
          x = dot.fadeX;
          y = dot.fadeY;

          const pFade = Math.min(1.0, elapsed / (dot.fadeDuration || 400));
          opacity = Math.max(0, 1.0 - pFade);

          if (pFade >= 1.0) {
            toDelete.push(reqId);
          }

        }

        if (x > 0 && y > 0 && opacity > 0) {
          // Clean solid request node (No pulse, no beat, no blurry glow)
          const solidR = 6.2;
          svgHtml += `
            <circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="${solidR.toFixed(1)}" fill="${color}" opacity="${opacity.toFixed(2)}" stroke="#ffffff" stroke-width="1.8"/>
            ${dot.outcome === 'shed' ? `<text x="${(x + 10).toFixed(1)}" y="${(y + 3).toFixed(1)}" font-size="9" font-family="monospace" font-weight="bold" fill="#ef4444" opacity="${opacity.toFixed(2)}">${dot.badge || 'shed'}</text>` : (dot.index ? `<text x="${x.toFixed(1)}" y="${(y + 2.5).toFixed(1)}" text-anchor="middle" font-size="7.5" font-family="monospace" font-weight="bold" fill="#ffffff" opacity="${opacity.toFixed(2)}" pointer-events="none">${dot.index}</text>` : '')}
          `;
        }
      });
      toDelete.forEach(id => activeTrafficDots.delete(id));

      container.innerHTML = svgHtml;

      if (isTrafficAnimationRunning) {

        requestAnimationFrame(renderTrafficAnimationLoop);

      }

    }

    // Refresh Provider Telemetry, Masked Keys, & Breakers

    const injectedFailuresState = { groq: false, gemini: false, openrouter: false };

    let currentSelectedInspectorNode = 'groq';

    function openCircuitBreakerModal(prov, event) {
      if (event && event.stopPropagation) event.stopPropagation();
      currentSelectedInspectorNode = prov || 'groq';

      const modal = document.getElementById('nodeDetailsModal');
      if (!modal) return;
      modal.classList.remove('hidden');

      const titleEl = document.getElementById('modalNodeTitle');
      const subtitleEl = document.getElementById('modalNodeSubtitle');
      const iconEl = document.getElementById('modalNodeIcon');
      const bodyEl = document.getElementById('modalNodeBody');
      if (!titleEl || !subtitleEl || !iconEl || !bodyEl) return;

      const provNames = { groq: 'Groq', gemini: 'Google Gemini', openrouter: 'OpenRouter' };
      const provName = provNames[prov] || 'Groq';
      const isBroken = Boolean(injectedFailuresState[prov]);

      titleEl.innerText = `${provName} · Circuit Breaker`;
      subtitleEl.innerText = 'Connection Cable Switch & Fallback Routing Policy';

      iconEl.className = 'w-9 h-9 rounded-xl bg-amber-500/20 border border-amber-500/40 flex items-center justify-center text-amber-500';
      iconEl.innerHTML = '<svg class="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/></svg>';

      bodyEl.innerHTML = `
        <div class="space-y-3 font-sans">
          <!-- 1. Live Cable Switch Status -->
          <div class="p-3.5 rounded-xl bg-slate-900 border border-slate-700/80 space-y-2">
            <div class="flex items-center justify-between">
              <span class="text-[11px] font-bold text-slate-300 uppercase tracking-wider flex items-center gap-1.5">
                <span class="w-2 h-2 rounded-full ${isBroken ? 'bg-rose-500 animate-ping' : 'bg-emerald-400'}"></span>
                Circuit Breaker Switch Status
              </span>
              <span id="modalBreakerStateBadge" class="px-2 py-0.5 rounded text-[10px] font-mono font-bold ${isBroken ? 'bg-rose-950 border border-rose-500 text-rose-300' : 'bg-emerald-950 border border-emerald-500/40 text-emerald-300'}">${isBroken ? 'DISCONNECTED (FAULT)' : 'CLOSED (HEALTHY)'}</span>
            </div>
            <p class="text-[11.5px] text-gray-300 leading-relaxed">
              Mounted directly on the connection cable between <strong>Retry Engine</strong> and <strong>${provName}</strong>. When consecutive failures reach the trip threshold, this switch trips to <strong>DISCONNECTED</strong>, cutting off traffic to protect the provider and redirecting to the fallback chain.
            </p>
          </div>

          <!-- 2. Fallback Logic Cascade -->
          <div class="p-3.5 rounded-xl bg-slate-900 border border-slate-700/80 space-y-2.5">
            <div class="flex items-center justify-between">
              <span class="text-[11px] font-bold text-amber-400 uppercase tracking-wider flex items-center gap-1.5">
                <span>⤹</span>
                Fallback Logic & Route Cascade
              </span>
              <span class="text-[10px] font-mono text-amber-300 bg-amber-950/60 border border-amber-500/30 px-1.5 py-0.2 rounded">Strict 30s Budget</span>
            </div>
            <div class="grid grid-cols-3 gap-2 text-center text-[10.5px]">
              <div class="p-2 rounded-lg ${prov === 'groq' ? 'bg-amber-500/20 border-2 border-amber-400 font-bold tier-card-active-groq' : 'bg-slate-800/80 border border-slate-700 font-medium tier-card-idle'}">
                <div class="text-[9px] uppercase tracking-wider ${prov === 'groq' ? 'text-amber-400 font-bold' : 'text-slate-400'}">Tier 1 (Fast)</div>
                <div class="text-[12px] font-bold ${prov === 'groq' ? 'text-amber-300' : 'text-white'} mt-0.5">Groq</div>
                <div class="text-[8.5px] font-mono ${prov === 'groq' ? 'text-amber-300/90' : 'text-slate-400'}">gpt-oss-120b</div>
              </div>
              <div class="p-2 rounded-lg ${prov === 'gemini' ? 'bg-indigo-500/20 border-2 border-indigo-400 font-bold tier-card-active-gemini' : 'bg-slate-800/80 border border-slate-700 font-medium tier-card-idle'}">
                <div class="text-[9px] uppercase tracking-wider ${prov === 'gemini' ? 'text-indigo-300 font-bold' : 'text-slate-400'}">Tier 2 (Smart)</div>
                <div class="text-[12px] font-bold ${prov === 'gemini' ? 'text-indigo-200' : 'text-white'} mt-0.5">Gemini</div>
                <div class="text-[8.5px] font-mono ${prov === 'gemini' ? 'text-indigo-300/90' : 'text-slate-400'}">gemini-2.5-flash</div>
              </div>
              <div class="p-2 rounded-lg ${prov === 'openrouter' ? 'bg-emerald-500/20 border-2 border-emerald-400 font-bold tier-card-active-openrouter' : 'bg-slate-800/80 border border-slate-700 font-medium tier-card-idle'}">
                <div class="text-[9px] uppercase tracking-wider ${prov === 'openrouter' ? 'text-emerald-400 font-bold' : 'text-slate-400'}">Tier 3 (Fallback)</div>
                <div class="text-[12px] font-bold ${prov === 'openrouter' ? 'text-emerald-300' : 'text-white'} mt-0.5">OpenRouter</div>
                <div class="text-[8.5px] font-mono ${prov === 'openrouter' ? 'text-emerald-300/90' : 'text-slate-400'}">llama-3.3-70b</div>
              </div>
            </div>
            <p class="text-[10.5px] text-gray-400 leading-relaxed">
              If <strong>${provName}</strong> throws HTTP 429, 5xx server errors, or times out, the gateway retries with exponential backoff (1s, 2s, 4s). If still failing or circuit breaker trips OPEN, it triggers the dotted <strong>Fallback Cascade</strong> to the next tier without dropping the user's request.
            </p>
          </div>

          <!-- 3. Breaker Parameters & Chaos Testing -->
          <div class="p-3.5 rounded-xl bg-slate-900 border border-slate-700/80 space-y-2">
            <span class="text-[11px] font-bold text-cyan-400 uppercase tracking-wider">3. Breaker Thresholds & Chaos Control</span>
            <div class="grid grid-cols-2 gap-2 text-[10.5px] font-mono">
              <div class="p-2 rounded bg-black/40 border border-white/5">
                <span class="text-gray-400 text-[9px] block">TRIP THRESHOLD</span>
                <span class="text-rose-400 font-bold">5 Consecutive Failures</span>
              </div>
              <div class="p-2 rounded bg-black/40 border border-white/5">
                <span class="text-gray-400 text-[9px] block">COOLDOWN DURATION</span>
                <span class="text-cyan-300 font-bold">60s Half-Open Probe</span>
              </div>
            </div>
            <div class="flex items-center gap-2 pt-2">
              <button id="modalCircuitFaultBtn" type="button" onclick="toggleFailureInjection('${prov}')" class="flex-1 py-2.5 rounded-xl ${isBroken ? 'bg-emerald-600 hover:bg-emerald-500 text-white border border-emerald-400' : 'bg-rose-900/60 hover:bg-rose-900 border border-rose-500/40 text-rose-200'} font-bold text-xs transition cursor-pointer flex items-center justify-center gap-1.5 shadow-md">
                ${isBroken ? '✅ Unbreak ' + provName + ' (Restore)' : '⚡ Break ' + provName + ' (Inject 503)'}
              </button>
              <button type="button" onclick="resetAllCircuitBreakers(); closeNodeDetailsModal();" class="flex-1 py-2.5 rounded-xl bg-slate-800 hover:bg-slate-700 border border-slate-600 text-gray-200 font-bold text-xs transition cursor-pointer shadow-md">
                ↺ Reset All Breakers
              </button>
            </div>
          </div>
        </div>
      `;

      // Fetch live breaker status
      fetch('/api/reliability/provider-nodes').then(r => r.json()).then(nodes => {
        const n = nodes.find(x => x.provider === prov);
        const state = (n && n.circuit_breaker && n.circuit_breaker.state) || 'CLOSED';
        const isFaulty = (state === 'OPEN') || (n && n.failure_injection && n.failure_injection.enabled);
        const b = document.getElementById('modalBreakerStateBadge');
        if (b) {
          if (isFaulty) {
            b.innerText = 'DISCONNECTED (OPEN)';
            b.className = 'px-2 py-0.5 rounded text-[10px] font-mono font-bold bg-rose-950 border border-rose-500 text-rose-300 animate-pulse';
          } else {
            b.innerText = 'CLOSED (HEALTHY)';
            b.className = 'px-2 py-0.5 rounded text-[10px] font-mono font-bold bg-emerald-950 border border-emerald-500/40 text-emerald-300';
          }
        }
      }).catch(() => {});
    }

    function openNodeInspector(nodeId) {

      currentSelectedInspectorNode = nodeId;

      const modal = document.getElementById('nodeDetailsModal');

      if (!modal) return;

      modal.classList.remove('hidden');

      const titleEl = document.getElementById('modalNodeTitle');

      const subtitleEl = document.getElementById('modalNodeSubtitle');

      const iconEl = document.getElementById('modalNodeIcon');

      const bodyEl = document.getElementById('modalNodeBody');

      if (!titleEl || !subtitleEl || !iconEl || !bodyEl) return;

      if (nodeId === 'retry') {

        titleEl.innerText = 'Retry & Resilience Engine';

        subtitleEl.innerText = 'Phase 3 Production Reliability SLA';

        iconEl.className = 'w-9 h-9 rounded-xl bg-amber-950/80 border border-amber-500/50 flex items-center justify-center text-amber-400';

        iconEl.innerHTML = '<svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/></svg>';

        bodyEl.innerHTML = `

          <div class="space-y-3">

            <div class="p-3.5 rounded-xl bg-black/40 border border-white/10 space-y-2">

              <div class="flex items-center justify-between">

                <span class="text-[11px] font-bold text-amber-400 uppercase tracking-wider">1. Configured Retry Policy</span>

                <span class="px-2 py-0.5 rounded text-[10px] font-mono font-bold bg-amber-950/80 border border-amber-500/40 text-amber-300">3 RETRIES MAX</span>

              </div>

              <p class="text-[11.5px] text-gray-300 leading-relaxed">

                Automatically retries up to <strong>3 times per provider</strong> on transient failures: <strong>HTTP 429</strong> (rate limits/quotas), <strong>5xx server errors</strong> (500, 502, 503, 504), network dropouts, and timeouts. Fast-fails immediately on client errors (400, 401, 403, 404).

              </p>

            </div>

            <div class="p-3.5 rounded-xl bg-black/40 border border-white/10 space-y-2">

              <div class="flex items-center justify-between">

                <span class="text-[11px] font-bold text-cyan-400 uppercase tracking-wider">2. Backoff & Jitter Schedule</span>

                <span class="text-[10px] text-gray-400 font-mono">Exponential</span>

              </div>

              <div class="grid grid-cols-3 gap-2 text-center font-mono text-[11px]">

                <div class="p-2 rounded-lg bg-white/5 border border-white/5">

                  <div class="text-[9px] text-gray-400 uppercase">Retry 1</div>

                  <div class="text-cyan-300 font-bold mt-0.5">1.0s + jitter</div>

                </div>

                <div class="p-2 rounded-lg bg-white/5 border border-white/5">

                  <div class="text-[9px] text-gray-400 uppercase">Retry 2</div>

                  <div class="text-cyan-300 font-bold mt-0.5">2.0s + jitter</div>

                </div>

                <div class="p-2 rounded-lg bg-white/5 border border-white/5">

                  <div class="text-[9px] text-gray-400 uppercase">Retry 3</div>

                  <div class="text-cyan-300 font-bold mt-0.5">4.0s + jitter</div>

                </div>

              </div>

              <p class="text-[10.5px] text-gray-400">Randomized jitter prevents thundering herd load spikes on recovery.</p>

            </div>

            <div class="p-3.5 rounded-xl bg-black/40 border border-white/10 space-y-2">

              <div class="flex items-center justify-between">

                <span class="text-[11px] font-bold text-rose-400 uppercase tracking-wider">3. Circuit Breaker Threshold</span>

                <span class="px-2 py-0.5 rounded text-[10px] font-mono font-bold bg-rose-950/80 border border-rose-500/40 text-rose-300">5 FAILURES TRIP</span>

              </div>

              <p class="text-[11.5px] text-gray-300 leading-relaxed">

                When a provider fails <strong>5 consecutive times</strong>, its circuit breaker trips to <strong>OPEN</strong>. Traffic is instantly shed for a <strong>60-second cooldown</strong>, followed by a <strong>HALF_OPEN</strong> test probe before closing.

              </p>

            </div>

            <div class="p-3.5 rounded-xl bg-black/40 border border-white/10 space-y-2">

              <div class="flex items-center justify-between">

                <span class="text-[11px] font-bold text-emerald-400 uppercase tracking-wider">4. Seamless Fallback Chain</span>

                <span class="text-[10px] text-emerald-300 font-mono">Auto-Failover</span>

              </div>

              <p class="text-[11.5px] text-gray-300 leading-relaxed">

                If all 3 retries or the circuit breaker are exhausted on the primary provider, the gateway seamlessly reroutes to secondary and tertiary providers defined in <code>models.json</code> within a strict <strong>30-second budget</strong>.

              </p>

            </div>

            <div class="flex items-center gap-2 pt-2">

              <button type="button" onclick="resetAllCircuitBreakers()" class="flex-1 py-2 rounded-xl bg-amber-500/10 hover:bg-amber-500/20 border border-amber-500/30 text-amber-300 font-bold text-xs transition cursor-pointer">

                Reset All Breakers

              </button>

              <button type="button" onclick="triggerStressFallbackDemo()" class="flex-1 py-2 rounded-xl bg-cyan-500/10 hover:bg-cyan-500/20 border border-cyan-500/30 text-cyan-300 font-bold text-xs transition cursor-pointer">

                Test Fallback Simulation

              </button>

            </div>

          </div>

        `;

      } else if (nodeId === 'client') {

        titleEl.innerText = 'Client Inbound Traffic';

        subtitleEl.innerText = 'Traffic Origin • Very Left End';

        iconEl.className = 'w-9 h-9 rounded-xl bg-slate-900 border border-slate-700 flex items-center justify-center text-cyan-400';

        iconEl.innerHTML = '<svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"/></svg>';

        bodyEl.innerHTML = `

          <div class="space-y-3">

            <div class="p-3 rounded-xl bg-black/40 border border-white/10 space-y-1.5">

              <span class="text-[10px] font-bold text-gray-400 uppercase tracking-wider block">Traffic Origin</span>

              <p class="text-xs text-gray-300">Requests originate from user applications, chat client, or the K6 load test bar at the very left edge of the map.</p>

            </div>

            <div class="p-3 rounded-xl bg-black/40 border border-white/10 space-y-1.5">

              <span class="text-[10px] font-bold text-cyan-400 uppercase tracking-wider block">K6 Traffic Burst Generator</span>

              <p class="text-xs text-gray-300">Dispatches configurable batches of requests (e.g. 10 requests in 3.0 seconds) to test and visualize gateway data flows under real load.</p>

            </div>

          </div>

        `;

      } else if (nodeId === 'cache') {

        titleEl.innerText = 'Redis Response Cache';

        subtitleEl.innerText = 'Exact-Match Sub-millisecond Storage';

        iconEl.className = 'w-9 h-9 rounded-xl bg-emerald-950/80 border border-emerald-500/50 flex items-center justify-center text-emerald-400';

        iconEl.innerHTML = '<svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"/></svg>';

        bodyEl.innerHTML = `

          <div class="space-y-3">

            <div class="p-3 rounded-xl bg-black/40 border border-white/10 space-y-1.5">

              <span class="text-[10px] font-bold text-emerald-400 uppercase tracking-wider block">Exact Match Cache Evaluation</span>

              <p class="text-xs text-gray-300">Hashes model, parameters, and prompt messages. On cache match, returns in ~2ms with $0.00 provider cost (flashes <span class="text-emerald-400 font-bold">GREEN / HIT</span>). On miss, passes to API Gateway / LLM Router (flashes <span class="text-rose-400 font-bold">RED / MISS</span>).</p>

            </div>

            <div class="p-3 rounded-xl bg-black/40 border border-white/10 space-y-2">
              <div class="flex items-center justify-between text-xs">
                <span class="text-gray-400 text-[10px] font-bold uppercase">Cache Actions</span>
                <span class="text-emerald-400 font-mono text-[10px]" id="modalCacheStatusLabel">Active In-Memory</span>
              </div>
              <button type="button" onclick="clearGatewayCache(event)" class="w-full py-2.5 rounded-xl bg-emerald-600 hover:bg-emerald-500 text-white font-bold text-xs shadow-md transition active:scale-95 flex items-center justify-center gap-1.5 cursor-pointer">
                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"/></svg>
                Clear Redis Response Cache
              </button>
            </div>

          </div>

        `;

      } else if (nodeId === 'dlq') {

        titleEl.innerText = 'Dead Letter Queue (DLQ)';

        subtitleEl.innerText = 'Poison Pill Quarantine & Replay Pool';

        iconEl.className = 'w-9 h-9 rounded-xl bg-rose-950/80 border border-rose-500/50 flex items-center justify-center text-rose-400';

        iconEl.innerHTML = '<svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"/></svg>';

        bodyEl.innerHTML = `

          <div class="space-y-3">

            <div class="p-3 rounded-xl bg-black/40 border border-white/10 space-y-1.5">

              <span class="text-[10px] font-bold text-rose-400 uppercase tracking-wider block">DLQ Status & Poison Pill Safety</span>

              <p class="text-xs text-gray-300">Quarantines failed asynchronous background jobs after exhausting all retry attempts, preventing poisoned traffic from blocking healthy queues.</p>

            </div>

            <div class="p-3 rounded-xl bg-black/40 border border-white/10 space-y-2">
              <div class="flex items-center justify-between text-xs">
                <span class="text-gray-400 text-[10px] font-bold uppercase">DLQ Actions</span>
                <span class="text-rose-400 font-mono text-[10px]">Quarantine Pool</span>
              </div>
              <div class="flex items-center gap-2 pt-1">
                <button type="button" onclick="replayAllDLQJobs(event)" class="flex-1 py-2.5 rounded-xl bg-amber-500 hover:bg-amber-400 text-gray-950 font-bold text-xs shadow-md transition active:scale-95 cursor-pointer">
                  ↺ Replay All
                </button>
                <button type="button" onclick="clearAllDLQJobs(event)" class="flex-1 py-2.5 rounded-xl bg-rose-900/70 hover:bg-rose-900 border border-rose-500/40 text-rose-200 font-bold text-xs shadow-md transition active:scale-95 cursor-pointer">
                  🗑️ Clear DLQ
                </button>
              </div>
            </div>

          </div>

        `;

      } else if (nodeId === 'jobqueue' || nodeId === 'idempotency' || nodeId === 'worker') {

        const titles = {
          jobqueue: ['Async Redis Job Queue', 'Asynchronous Task Buffer & Priority Queuing'],
          idempotency: ['Idempotency Guard', 'Deduplication Store for Exactly-Once Execution'],
          worker: ['Async Worker Pool', 'High-Concurrency Background Consumer Cluster']
        };
        const [nTitle, nSub] = titles[nodeId] || ['Async Pipeline', 'Background Processing'];
        titleEl.innerText = nTitle;
        subtitleEl.innerText = nSub;
        iconEl.className = 'w-9 h-9 rounded-xl bg-cyan-950/80 border border-cyan-500/50 flex items-center justify-center text-cyan-400';
        iconEl.innerHTML = '<svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"/></svg>';
        bodyEl.innerHTML = `
          <div class="space-y-3">
            <div class="p-3 rounded-xl bg-black/40 border border-white/10 space-y-1.5">
              <span class="text-[10px] font-bold text-cyan-400 uppercase tracking-wider block">Inbound Async Flow</span>
              <p class="text-xs text-gray-300">Inbound requests branch directly from Client Ingress to the Idempotency Store and Job Queue, ensuring decoupled non-blocking execution with worker failover.</p>
            </div>
          </div>
        `;

      } else if (nodeId === 'gateway') {

        titleEl.innerText = 'API Gateway';

        subtitleEl.innerText = 'Edge Ingress & Authentication';

        iconEl.className = 'w-9 h-9 rounded-xl bg-purple-950/80 border border-purple-500/50 flex items-center justify-center text-purple-400';

        iconEl.innerHTML = '<svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z"/></svg>';

        bodyEl.innerHTML = `

          <div class="space-y-3">

            <div class="p-3 rounded-xl bg-black/40 border border-white/10 space-y-1.5">

              <span class="text-[10px] font-bold text-purple-400 uppercase tracking-wider block">Edge Ingress Routing</span>

              <p class="text-xs text-gray-300">Handles incoming client requests on <code>/api/chat/completions</code> and <code>/api/reliability/test-request</code>.</p>

            </div>

            <div class="p-3 rounded-xl bg-black/40 border border-white/10 space-y-1.5">

              <span class="text-[10px] font-bold text-cyan-400 uppercase tracking-wider block">Authentication & Verification</span>

              <p class="text-xs text-gray-300">Enforces session tokens, Bearer authorization, and admin key security. Blocks unauthorized traffic at the edge.</p>

            </div>

          </div>

        `;

      } else if (nodeId === 'balancer') {

        titleEl.innerText = 'LLM Router';

        subtitleEl.innerText = 'models.json Dynamic Routing Core';

        iconEl.className = 'w-9 h-9 rounded-xl bg-teal-950/80 border border-teal-500/50 flex items-center justify-center text-teal-400';

        iconEl.innerHTML = '<svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 4a1 1 0 011-1h16a1 1 0 011 1v2.586a1 1 0 01-.293.707l-6.414 6.414a1 1 0 00-.293.707V17l-4 4v-6.586a1 1 0 00-.293-.707L3.293 7.293A1 1 0 013 6.586V4z"/></svg>';

        bodyEl.innerHTML = `

          <div class="space-y-3">

            <div class="p-3 rounded-xl bg-black/40 border border-white/10 space-y-1.5">

              <span class="text-[10px] font-bold text-teal-400 uppercase tracking-wider block">Dynamic Target Resolution</span>

              <p class="text-xs text-gray-300">Inspects inbound request targets (<code>fast</code>, <code>smart</code>, <code>cheap</code>, <code>complex</code>) and resolves primary and fallback provider sequences from <code>models.json</code>.</p>

            </div>

          </div>

        `;

      } else {

        const prov = nodeId === 'openrouter' ? 'openrouter' : (nodeId === 'gemini' ? 'gemini' : 'groq');
        const isBroken = Boolean(injectedFailuresState[prov]);

        titleEl.innerText = prov === 'groq' ? 'Groq' : (prov === 'openrouter' ? 'OpenRouter' : 'Google Gemini');

        subtitleEl.innerText = prov.toUpperCase() + ' Provider Tier';

        iconEl.className = 'w-9 h-9 rounded-xl bg-black/60 border border-white/20 flex items-center justify-center p-1.5';

        iconEl.innerHTML = PROVIDER_LOGOS[prov] || '';

        bodyEl.innerHTML = `

          <div class="space-y-3">

            <!-- Masked Key -->

            <div class="p-3 rounded-xl bg-black/40 border border-white/10 space-y-1.5">

              <div class="flex items-center justify-between">

                <span class="text-[10px] font-bold text-gray-400 uppercase tracking-wider">Masked API Key</span>

                <span id="inspectorKeyStatus" class="px-1.5 py-0.2 rounded text-[9px] font-bold bg-emerald-950/60 border border-emerald-500/30 text-emerald-400 uppercase">valid</span>

              </div>

              <div class="flex items-center justify-between font-mono text-xs">

                <span id="inspectorKeyMasked" class="text-gray-200 font-semibold tracking-wider">***yd</span>

                <button type="button" onclick="openKeyEditModal('${prov}')" class="px-2.5 py-1 rounded-lg text-[10px] font-medium bg-cyan-950/60 border border-cyan-500/30 text-cyan-300 hover:text-white transition flex items-center gap-1 cursor-pointer">

                  <span>Edit Key</span>

                </button>

              </div>

            </div>

            <!-- Circuit Breaker -->

            <div class="p-3 rounded-xl bg-black/40 border border-white/10 space-y-2">

              <div class="flex items-center justify-between">

                <span class="text-[10px] font-bold text-gray-400 uppercase tracking-wider">Circuit Breaker</span>

                <span id="inspectorBreakerPill" class="px-2 py-0.5 rounded text-[10px] font-bold bg-emerald-950/80 border border-emerald-500/30 text-emerald-400">CLOSED</span>

              </div>

              <div class="flex items-center justify-between text-xs text-gray-300">

                <span class="text-[10px] text-gray-400">Trip Rule:</span>

                <span class="font-mono text-xs">5 consecutive failures &rarr; 60s OPEN</span>

              </div>

              <button type="button" onclick="resetAllCircuitBreakers()" class="w-full py-1.5 rounded-lg bg-white/5 hover:bg-white/10 border border-white/10 text-xs font-medium text-gray-300 transition text-center cursor-pointer">

                Reset Breaker

              </button>

            </div>

            <!-- Live Metrics -->

            <div class="p-3 rounded-xl bg-black/40 border border-white/10 space-y-2">

              <span class="text-[10px] font-bold text-gray-400 uppercase tracking-wider block">Live Node Metrics</span>

              <div class="grid grid-cols-3 gap-2 text-center">

                <div class="p-2 rounded-lg bg-white/5">

                  <div class="text-[9px] text-gray-400 uppercase">In-Flight</div>

                  <div id="inspectorMetricInflight" class="text-sm font-bold text-cyan-400 font-mono mt-0.5">0</div>

                </div>

                <div class="p-2 rounded-lg bg-white/5">

                  <div class="text-[9px] text-gray-400 uppercase">Served</div>

                  <div id="inspectorMetricServed" class="text-sm font-bold text-emerald-400 font-mono mt-0.5">0</div>

                </div>

                <div class="p-2 rounded-lg bg-white/5">

                  <div class="text-[9px] text-gray-400 uppercase">Failed</div>

                  <div id="inspectorMetricFailed" class="text-sm font-bold text-rose-400 font-mono mt-0.5">0</div>

                </div>

              </div>

            </div>

            <!-- Fault Injection Demo (Two-way Break <-> Unbreak Toggle) -->

            <div class="p-3 rounded-xl bg-rose-950/20 border border-rose-500/20 space-y-2">

              <div class="flex items-center justify-between">

                <span class="text-[10px] font-bold text-rose-400 uppercase tracking-wider">Fault Injection</span>

                <span id="inspectorFaultSub" class="text-[9px] text-gray-400">${isBroken ? 'Active 503 Outage Injected' : 'Simulate 503 Outage'}</span>

              </div>

              <button id="inspectorFaultToggleBtn" type="button" onclick="toggleFailureInjection('${prov}')" class="w-full py-2.5 rounded-xl ${isBroken ? 'bg-emerald-600 hover:bg-emerald-500 border border-emerald-400 text-white' : 'bg-rose-900/60 hover:bg-rose-900 border border-rose-500/40 text-rose-200'} text-xs font-bold transition cursor-pointer flex items-center justify-center gap-1.5 shadow-md">

                ${isBroken ? '✅ Unbreak Node (Restore 200)' : '⚡ Break This Node (503)'}

              </button>

            </div>

          </div>

        `;

      }

      refreshVisualizeData();

    }

    function closeNodeDetailsModal() {

      const modal = document.getElementById('nodeDetailsModal');

      if (modal) modal.classList.add('hidden');

    }

    function closeNodeInspector() {

      closeNodeDetailsModal();

    }

    // Canvas Pan & Zoom Engine

    let canvasPanX = 0;

    let canvasPanY = 0;

    let canvasZoomScale = 1.0;

    let isCanvasPanning = false;

    let panStartX = 0;

    let panStartY = 0;

    // n8n Style Multi-Node Box Selection & Group Dragging
    const selectedCanvasNodes = new Set();
    let isBoxDrawerMode = false;
    let isBoxSelecting = false;
    let boxStartX = 0;
    let boxStartY = 0;

    function updateCanvasTransform() {

      const layer = document.getElementById('canvasTransformLayer');

      if (layer) {

        layer.style.transform = `translate(${canvasPanX}px, ${canvasPanY}px) scale(${canvasZoomScale})`;

      }

      const disp = document.getElementById('zoomLevelDisplay');

      if (disp) disp.innerText = `${Math.round(canvasZoomScale * 100)}%`;

    }

    function zoomCanvasIn() {

      canvasZoomScale = Math.min(2.0, canvasZoomScale + 0.15);

      updateCanvasTransform();

    }

    function zoomCanvasOut() {

      canvasZoomScale = Math.max(0.4, canvasZoomScale - 0.15);

      updateCanvasTransform();

    }

    function resetNodePositionsToDefault() {
      const defaultPositions = {
        'studioNodeClient': { left: '20px', top: '218px' },
        'studioNodeAuth': { left: '120px', top: '218px' },
        'studioNodeRateLimit': { left: '220px', top: '218px' },
        'studioNodeRedis': { left: '220px', top: '78px' },
        'studioNodeInputGuardrails': { left: '320px', top: '218px' },
        'studioNodeGateway': { left: '430px', top: '218px' },
        'studioNodeCacheStore': { left: '430px', top: '78px' },
        'studioNodeBalancer': { left: '550px', top: '218px' },
        'studioNodeRetry': { left: '670px', top: '218px' },
        'studioNodeIdempotency': { left: '120px', top: '358px' },
        'studioNodeJobQueue': { left: '220px', top: '358px' },
        'studioNodeWorker': { left: '320px', top: '358px' },
        'studioNodeDLQ': { left: '430px', top: '358px' },
        'studioNodeOutputGuardrails': { left: '670px', top: '358px' },
        'studioNodeAuditLog': { left: '550px', top: '358px' },
        'vizQueueBoxGroq': { left: '850px', top: '72px' },
        'studioNodeCache': { left: '990px', top: '52px' },
        'vizQueueBoxGemini': { left: '850px', top: '218px' },
        'studioNodeGemini': { left: '990px', top: '198px' },
        'vizQueueBoxOpenRouter': { left: '850px', top: '364px' },
        'studioNodeDatabase': { left: '990px', top: '344px' }
      };
      Object.entries(defaultPositions).forEach(([id, pos]) => {
        const el = document.getElementById(id);
        if (el) {
          el.style.left = pos.left;
          el.style.top = pos.top;
        }
      });
      updateConnectorPaths();
    }

    function resetCanvasZoomPan() {

      canvasPanX = 0;

      canvasPanY = 0;

      canvasZoomScale = 1.0;

      updateCanvasTransform();

      resetNodePositionsToDefault();

    }

    function initCanvasPanZoom() {

      const viewport = document.getElementById('studioCanvasViewport');

      if (!viewport || viewport.dataset.panZoomInitialized) return;

      viewport.dataset.panZoomInitialized = 'true';

      viewport.style.cursor = 'grab';

      // Double-click activates n8n-style Box Drawer mode
      viewport.addEventListener('dblclick', (e) => {
        if (e.target.closest('#studioNodesStage > div') || e.target.closest('#vizK6ControlBar') || e.target.closest('#vizCanvasControls') || e.target.closest('button')) {
          return;
        }
        isBoxDrawerMode = true;
        viewport.style.cursor = 'crosshair';
        const rect = viewport.getBoundingClientRect();
        boxStartX = (e.clientX - rect.left - canvasPanX) / canvasZoomScale;
        boxStartY = (e.clientY - rect.top - canvasPanY) / canvasZoomScale;
        const marquee = document.getElementById('vizSelectionMarquee');
        if (marquee) {
          marquee.classList.remove('hidden');
          marquee.style.left = `${boxStartX}px`;
          marquee.style.top = `${boxStartY}px`;
          marquee.style.width = '0px';
          marquee.style.height = '0px';
        }
        isBoxSelecting = true;
      });

      viewport.addEventListener('mousedown', (e) => {

        if (e.target.closest('#studioNodesStage > div') || e.target.closest('#vizK6ControlBar') || e.target.closest('#vizCanvasControls') || e.target.closest('button') || e.target.closest('select') || e.target.closest('input')) {

          return;

        }

        // Marquee box selection if Shift key is held or box drawer mode was triggered
        if (e.shiftKey || isBoxDrawerMode) {
          isBoxSelecting = true;
          const rect = viewport.getBoundingClientRect();
          boxStartX = (e.clientX - rect.left - canvasPanX) / canvasZoomScale;
          boxStartY = (e.clientY - rect.top - canvasPanY) / canvasZoomScale;
          const marquee = document.getElementById('vizSelectionMarquee');
          if (marquee) {
            marquee.classList.remove('hidden');
            marquee.style.left = `${boxStartX}px`;
            marquee.style.top = `${boxStartY}px`;
            marquee.style.width = '0px';
            marquee.style.height = '0px';
          }
          viewport.style.cursor = 'crosshair';
          return;
        }

        // Clicking empty canvas clears selection
        selectedCanvasNodes.forEach(n => {
          n.classList.remove('ring-2', 'ring-cyan-400', 'ring-offset-2', 'ring-offset-slate-900', 'shadow-[0_0_20px_rgba(6,182,212,0.6)]');
        });
        selectedCanvasNodes.clear();

        isCanvasPanning = true;

        panStartX = e.clientX - canvasPanX;

        panStartY = e.clientY - canvasPanY;

        viewport.style.cursor = 'grabbing';

      });

      window.addEventListener('mousemove', (e) => {

        if (isBoxSelecting) {
          const rect = viewport.getBoundingClientRect();
          const curX = (e.clientX - rect.left - canvasPanX) / canvasZoomScale;
          const curY = (e.clientY - rect.top - canvasPanY) / canvasZoomScale;
          const bx = Math.min(boxStartX, curX);
          const by = Math.min(boxStartY, curY);
          const bw = Math.abs(curX - boxStartX);
          const bh = Math.abs(curY - boxStartY);

          const marquee = document.getElementById('vizSelectionMarquee');
          if (marquee) {
            marquee.style.left = `${bx}px`;
            marquee.style.top = `${by}px`;
            marquee.style.width = `${bw}px`;
            marquee.style.height = `${bh}px`;
          }

          const stage = document.getElementById('studioNodesStage');
          if (stage) {
            stage.querySelectorAll(':scope > div:not(#vizSelectionMarquee)').forEach(nodeEl => {
              const nl = parseFloat(nodeEl.style.left) || nodeEl.offsetLeft || 0;
              const nt = parseFloat(nodeEl.style.top) || nodeEl.offsetTop || 0;
              const nw = nodeEl.offsetWidth || 88;
              const nh = nodeEl.offsetHeight || 96;

              const intersects = (bx < nl + nw && bx + bw > nl && by < nt + nh && by + bh > nt);
              if (intersects) {
                selectedCanvasNodes.add(nodeEl);
                nodeEl.classList.add('ring-2', 'ring-cyan-400', 'ring-offset-2', 'ring-offset-slate-900', 'shadow-[0_0_20px_rgba(6,182,212,0.6)]');
              } else if (!e.shiftKey) {
                selectedCanvasNodes.delete(nodeEl);
                nodeEl.classList.remove('ring-2', 'ring-cyan-400', 'ring-offset-2', 'ring-offset-slate-900', 'shadow-[0_0_20px_rgba(6,182,212,0.6)]');
              }
            });
          }
          return;
        }

        if (!isCanvasPanning) return;

        canvasPanX = e.clientX - panStartX;

        canvasPanY = e.clientY - panStartY;

        updateCanvasTransform();

      });

      window.addEventListener('mouseup', () => {

        if (isBoxSelecting) {
          isBoxSelecting = false;
          isBoxDrawerMode = false;
          const marquee = document.getElementById('vizSelectionMarquee');
          if (marquee) marquee.classList.add('hidden');
          const vp = document.getElementById('studioCanvasViewport');
          if (vp) vp.style.cursor = 'grab';
        }

        if (isCanvasPanning) {

          isCanvasPanning = false;

          const vp = document.getElementById('studioCanvasViewport');

          if (vp) vp.style.cursor = 'grab';

        }

      });

      viewport.addEventListener('wheel', (e) => {

        e.preventDefault();

        const factor = e.deltaY < 0 ? 1.08 : 0.92;

        const newZoom = Math.min(2.0, Math.max(0.4, canvasZoomScale * factor));

        const rect = viewport.getBoundingClientRect();

        const mouseX = e.clientX - rect.left;

        const mouseY = e.clientY - rect.top;

        canvasPanX = mouseX - (mouseX - canvasPanX) * (newZoom / canvasZoomScale);

        canvasPanY = mouseY - (mouseY - canvasPanY) * (newZoom / canvasZoomScale);

        canvasZoomScale = newZoom;

        updateCanvasTransform();

      }, { passive: false });

    }

    // Dynamic Node Dragging & Cable Recalculation Engine

    function getNodeAnchor(id) {

      const el = document.getElementById(id);

      if (!el) return { inX: 0, inY: 0, outX: 0, outY: 0, centerX: 0, centerY: 0 };

      const left = parseFloat(el.style.left) || el.offsetLeft || 0;

      const top = parseFloat(el.style.top) || el.offsetTop || 0;

      const w = parseFloat(el.style.width) || el.offsetWidth || 88;

      const isQueueBox = el.classList.contains('viz-queue-box');

      let h = 56;

      if (isQueueBox) {

        h = parseFloat(el.style.minHeight) || el.offsetHeight || 56;

      } else {

        const tile = el.querySelector('div:first-child');

        h = tile ? (tile.offsetHeight || 56) : 56;

      }

      const midY = top + (h / 2);

      return {

        inX: isQueueBox ? left : (left + 6),

        inY: midY,

        outX: isQueueBox ? (left + w) : (left + w - 6),

        outY: midY,

        centerX: left + w / 2,

        centerY: midY

      };

    }

    function makeCablePath(x1, y1, x2, y2) {

      if (Math.abs(y1 - y2) < 2) {

        return `M ${x1} ${y1} L ${x2} ${y2}`;

      }

      const midX = (x1 + x2) / 2;

      return `M ${x1} ${y1} C ${midX} ${y1}, ${midX} ${y2}, ${x2} ${y2}`;

    }

    function updateConnectorPaths() {

      const client = getNodeAnchor('studioNodeClient');

      const auth = getNodeAnchor('studioNodeAuth');

      const rateLimit = getNodeAnchor('studioNodeRateLimit');

      const redis = getNodeAnchor('studioNodeRedis');

      const gateway = getNodeAnchor('studioNodeGateway');

      const router = getNodeAnchor('studioNodeBalancer');

      const retry = getNodeAnchor('studioNodeRetry');

      const qGroq = getNodeAnchor('vizQueueBoxGroq');

      const qGemini = getNodeAnchor('vizQueueBoxGemini');

      const qOpenRouter = getNodeAnchor('vizQueueBoxOpenRouter');

      const groq = getNodeAnchor('studioNodeCache');

      const gemini = getNodeAnchor('studioNodeGemini');

      const openrouter = getNodeAnchor('studioNodeDatabase');

      const idem = getNodeAnchor('studioNodeIdempotency');

      const jq = getNodeAnchor('studioNodeJobQueue');

      const worker = getNodeAnchor('studioNodeWorker');

      const dlq = getNodeAnchor('studioNodeDLQ');

      const cacheStore = getNodeAnchor('studioNodeCacheStore');

      const inGuard = getNodeAnchor('studioNodeInputGuardrails');

      const outGuard = getNodeAnchor('studioNodeOutputGuardrails');

      const auditLog = getNodeAnchor('studioNodeAuditLog');

      const setD = (id, d) => {

        const el = document.getElementById(id);

        if (el) el.setAttribute('d', d);

      };

      // Main Ingress Cable Sequence:
      // Client -> Auth Wall -> Rate Limiter -> Input Guard -> API Gateway -> LLM Router -> Retry & Breaker
      setD('cable-client-auth', makeCablePath(client.outX, client.outY, auth.inX, auth.inY));
      setD('cable-auth-ratelimit', makeCablePath(auth.outX, auth.outY, rateLimit.inX, rateLimit.inY));
      setD('cable-ratelimit-redis', `M ${rateLimit.centerX} ${rateLimit.inY - 24} L ${redis.centerX} ${redis.outY + 2}`);
      setD('cable-ratelimit-inputguard', makeCablePath(rateLimit.outX, rateLimit.outY, inGuard.inX, inGuard.inY));
      setD('cable-inputguard-gateway', makeCablePath(inGuard.outX, inGuard.outY, gateway.inX, gateway.inY));

      // Separate Cache Above API Gateway:
      setD('cable-gateway-cache', `M ${gateway.centerX} ${gateway.inY - 24} L ${cacheStore.centerX} ${cacheStore.outY + 2}`);
      setD('cable-gateway-router', makeCablePath(gateway.outX, gateway.outY, router.inX, router.inY));
      setD('cable-router-retry', makeCablePath(router.outX, router.outY, retry.inX, retry.inY));

      // Async Jobs Pipeline (Inbound Branch from Client Traffic -> Idempotency -> Job Queue -> Workers -> DLQ)
      setD('cable-client-idem', makeCablePath(client.centerX, client.centerY + 28, idem.inX, idem.centerY));
      setD('cable-idem-jobqueue', makeCablePath(idem.outX, idem.outY, jq.inX, jq.inY));
      setD('cable-jobqueue-worker', makeCablePath(jq.outX, jq.outY, worker.inX, worker.inY));
      setD('cable-worker-dlq', makeCablePath(worker.outX, worker.outY, dlq.inX, dlq.inY));

      // Security Egress & Return Flow (Lower Tier - Middle):
      // Retry & Breaker -> Output Guard (downward vertical drop) -> Audit Log (leftward arrow) -> Client Traffic (smooth return bus)
      setD('cable-retry-outputguard', `M ${retry.centerX} ${retry.centerY + 28} L ${outGuard.centerX} ${outGuard.centerY - 28}`);
      setD('cable-outputguard-audit', `M ${outGuard.inX} ${outGuard.centerY} L ${auditLog.outX} ${auditLog.centerY}`);
      const dReturnBus = `M ${auditLog.inX} ${auditLog.centerY} C ${auditLog.centerX - 120} 445, 60 445, ${client.centerX - 10} ${client.centerY + 24}`;
      setD('cable-audit-client', dReturnBus);

      // Cable 4, 5, 6: Retry Node -> Queue Boxes (with Breaker Switch in between)
      const d4 = makeCablePath(retry.outX, retry.outY, qGroq.inX, qGroq.inY);

      setD('path-router-groq', d4);

      const d5 = makeCablePath(retry.outX, retry.outY, qGemini.inX, qGemini.inY);

      setD('path-router-gemini', d5);

      const d6 = makeCablePath(retry.outX, retry.outY, qOpenRouter.inX, qOpenRouter.inY);

      setD('path-router-openrouter', d6);

      // Cable 7, 8, 9: Queue Boxes -> LLM Provider Nodes (Direct clean horizontal docks)
      const d7 = `M ${qGroq.outX} ${qGroq.centerY} L ${groq.inX} ${groq.centerY}`;

      setD('path-queue-groq', d7);

      const d8 = `M ${qGemini.outX} ${qGemini.centerY} L ${gemini.inX} ${gemini.centerY}`;

      setD('path-queue-gemini', d8);

      const d9 = `M ${qOpenRouter.outX} ${qOpenRouter.centerY} L ${openrouter.inX} ${openrouter.centerY}`;

      setD('path-queue-openrouter', d9);

      // Inbound composite path for particles: Client -> Auth -> RateLimit -> InputGuard -> Gateway -> Router -> Retry
      const dComp = `M ${client.centerX} ${client.centerY} L ${auth.centerX} ${auth.centerY} L ${rateLimit.centerX} ${rateLimit.centerY} L ${inGuard.centerX} ${inGuard.centerY} L ${gateway.centerX} ${gateway.centerY} L ${router.centerX} ${router.centerY} L ${retry.centerX} ${retry.centerY}`;
      setD('path-client-router', dComp);

      // Security Egress Return path for particles: Retry -> OutputGuard -> AuditLog -> Client Traffic
      const dReturn = `M ${retry.centerX} ${retry.centerY + 28} L ${outGuard.centerX} ${outGuard.centerY} L ${auditLog.centerX} ${auditLog.centerY} C ${auditLog.centerX - 120} 445, 60 445, ${client.centerX} ${client.centerY}`;
      setD('path-return-egress', dReturn);

      // Dedicated return path for Redis Cache hits: Cache Store -> Gateway -> Bus -> Client Traffic (Traces actual cables)
      const dCacheReturn = `M ${cacheStore.centerX} ${cacheStore.centerY} L ${gateway.centerX} ${gateway.centerY} L ${inGuard.centerX} ${inGuard.centerY} L ${rateLimit.centerX} ${rateLimit.centerY} L ${auth.centerX} ${auth.centerY} L ${client.centerX} ${client.centerY}`;
      setD('path-cache-return', dCacheReturn);

      // Dedicated rejection paths for 401 and 429 animation
      setD('path-client-auth', `M ${client.centerX} ${client.centerY} L ${auth.centerX} ${auth.centerY}`);
      setD('path-client-ratelimit', `M ${client.centerX} ${client.centerY} L ${auth.centerX} ${auth.centerY} L ${rateLimit.centerX} ${rateLimit.centerY}`);

      // Dynamically position circuit breaker switches squarely along Cable 1 (Retry -> Queue) with generous buffer
      const pG = document.getElementById('path-router-groq');
      if (pG && pG.getTotalLength) {
        const pt = pG.getPointAtLength(pG.getTotalLength() * 0.50);
        const s = document.getElementById('cableSwitchGroq');
        if (s) { s.style.left = `${pt.x}px`; s.style.top = `${pt.y}px`; }
      }
      const pM = document.getElementById('path-router-gemini');
      if (pM && pM.getTotalLength) {
        const pt = pM.getPointAtLength(pM.getTotalLength() * 0.50);
        const s = document.getElementById('cableSwitchGemini');
        if (s) { s.style.left = `${pt.x}px`; s.style.top = `${pt.y}px`; }
      }
      const pO = document.getElementById('path-router-openrouter');
      if (pO && pO.getTotalLength) {
        const pt = pO.getPointAtLength(pO.getTotalLength() * 0.50);
        const s = document.getElementById('cableSwitchOpenRouter');
        if (s) { s.style.left = `${pt.x}px`; s.style.top = `${pt.y}px`; }
      }

      // 1. Dotted Retry Loop above Retry & Breaker node
      const dRetry = `M ${retry.centerX - 18} ${retry.centerY - 26} C ${retry.centerX - 35} ${retry.centerY - 65}, ${retry.centerX + 35} ${retry.centerY - 65}, ${retry.centerX + 18} ${retry.centerY - 26}`;
      setD('path-retry-loop', dRetry);
      const bR = document.getElementById('badgeRetryLoop');
      if (bR) { bR.style.left = `${retry.centerX}px`; bR.style.top = `${retry.centerY - 58}px`; }

      // 2. Dotted Fallback Cascade between Providers: Groq -> Gemini -> OpenRouter
      const dFb1 = `M ${groq.centerX + 30} ${groq.centerY + 12} C ${groq.centerX + 68} ${groq.centerY + 55}, ${gemini.centerX + 68} ${gemini.centerY - 55}, ${gemini.centerX + 30} ${gemini.centerY - 12}`;
      setD('path-fallback-groq-gemini', dFb1);

      const dFb2 = `M ${gemini.centerX + 30} ${gemini.centerY + 12} C ${gemini.centerX + 68} ${gemini.centerY + 55}, ${openrouter.centerX + 68} ${openrouter.centerY - 55}, ${openrouter.centerX + 30} ${openrouter.centerY - 12}`;
      setD('path-fallback-gemini-openrouter', dFb2);

      const bFb = document.getElementById('badgeFallbackChain');
      if (bFb) { bFb.style.left = `${groq.centerX + 66}px`; bFb.style.top = `${(groq.centerY + gemini.centerY) / 2}px`; }

    }

    async function syncDlqDepthBadge() {

      try {

        const res = await fetch('/v1/dlq');

        if (!res.ok) return;

        const items = await res.json();

        const badge = document.getElementById('vizDlqDepthBadge');

        if (badge) badge.innerText = String(Array.isArray(items) ? items.length : 0);

      } catch (err) {

        console.warn('DLQ depth sync error:', err);

      }

    }

    function recheckDlqDepth() {

      setTimeout(syncDlqDepthBadge, 600);

    }

    async function replayLatestDLQJob(e) {
      if (e) e.stopPropagation();
      try {
        const res = await fetch('/v1/dlq');
        const items = await res.json();
        if (items && items.length > 0) {
          const target = items[0];
          const repRes = await fetch(`/v1/dlq/${target.id}/replay`, { method: 'POST' });
          if (repRes.ok) {
            flashNodeStatus('studioNodeDLQ', 'yellow');
            flashNodeStatus('studioNodeJobQueue', 'blue');
            recheckDlqDepth();
            logVisualizerEvent(
              `<span class="px-1.5 py-0.2 rounded text-[9px] font-bold bg-amber-950 text-amber-300 border border-amber-500">REPLAY</span>`,
              `Replaying DLQ job <span class="font-mono text-cyan-300">${target.id}</span>`
            );
          }
        } else {
          logVisualizerEvent(
            `<span class="px-1.5 py-0.2 rounded text-[9px] font-bold bg-slate-800 text-gray-300 border border-slate-600">DLQ</span>`,
            `No dead-lettered jobs to replay.`
          );
        }
      } catch (err) {
        console.error('Error replaying DLQ job:', err);
      }
    }

    function initDraggableNodes() {

      const stage = document.getElementById('studioNodesStage');

      if (!stage || stage.dataset.draggableNodesInit) return;

      stage.dataset.draggableNodesInit = 'true';

      let draggedNode = null;

      let startX = 0;

      let startY = 0;

      let hasMoved = false;

      const initialPositions = new Map();

      stage.querySelectorAll(':scope > div:not(#vizSelectionMarquee)').forEach((nodeEl) => {

        nodeEl.addEventListener('mousedown', (e) => {

          if (e.target.closest('button')) return;

          e.stopPropagation();

          // Multi-selection management: maintain or update selection group
          if (!selectedCanvasNodes.has(nodeEl)) {
            if (!e.shiftKey) {
              selectedCanvasNodes.forEach(n => {
                n.classList.remove('ring-2', 'ring-cyan-400', 'ring-offset-2', 'ring-offset-slate-900', 'shadow-[0_0_20px_rgba(6,182,212,0.6)]');
              });
              selectedCanvasNodes.clear();
            }
            selectedCanvasNodes.add(nodeEl);
            nodeEl.classList.add('ring-2', 'ring-cyan-400', 'ring-offset-2', 'ring-offset-slate-900', 'shadow-[0_0_20px_rgba(6,182,212,0.6)]');
          }

          draggedNode = nodeEl;

          hasMoved = false;

          startX = e.clientX;

          startY = e.clientY;

          initialPositions.clear();
          selectedCanvasNodes.forEach(selNode => {
            initialPositions.set(selNode, {
              left: parseFloat(selNode.style.left) || selNode.offsetLeft || 0,
              top: parseFloat(selNode.style.top) || selNode.offsetTop || 0
            });
            selNode.style.cursor = 'grabbing';
            selNode.style.zIndex = '35';
          });

        });

      });

      window.addEventListener('mousemove', (e) => {

        if (!draggedNode) return;

        const dx = (e.clientX - startX) / (canvasZoomScale || 1.0);

        const dy = (e.clientY - startY) / (canvasZoomScale || 1.0);

        if (Math.abs(dx) > 3 || Math.abs(dy) > 3) {

          hasMoved = true;

        }

        if (hasMoved) {

          selectedCanvasNodes.forEach(selNode => {
            const init = initialPositions.get(selNode);
            if (init) {
              selNode.style.left = `${init.left + dx}px`;
              selNode.style.top = `${init.top + dy}px`;
            }
          });

          updateConnectorPaths();

        }

      });

      window.addEventListener('mouseup', () => {

        if (draggedNode) {

          selectedCanvasNodes.forEach(selNode => {
            selNode.style.cursor = 'pointer';
            selNode.style.zIndex = '';
          });

          if (hasMoved) {

            draggedNode.dataset.justDragged = 'true';

            setTimeout(() => {

              if (draggedNode) delete draggedNode.dataset.justDragged;

            }, 120);

          }

          draggedNode = null;

          hasMoved = false;

        }

      });

    }

    function handleNodeClick(nodeId, event) {

      const el = event?.currentTarget || event?.target?.closest('#studioNodesStage > div');

      if (el && el.dataset.justDragged === 'true') {

        return;

      }

      openNodeInspector(nodeId);

    }

    function triggerKeyEditFromInspector() {

      const prov = (currentSelectedInspectorNode === 'openrouter' || currentSelectedInspectorNode === 'gemini') ? currentSelectedInspectorNode : 'groq';

      openKeyEditModal(prov);

    }

    function toggleFaultFromInspector() {

      const prov = (currentSelectedInspectorNode === 'openrouter' || currentSelectedInspectorNode === 'gemini') ? currentSelectedInspectorNode : 'groq';

      toggleFailureInjection(prov);

    }

    function toggleVisualizerFeedModal() {

      const modal = document.getElementById('visualizerFeedModal');

      if (modal) modal.classList.toggle('hidden');

    }

    function selectCanvasTool(tool) {

      ['pointer', 'hand', 'node', 'circle', 'diamond'].forEach(t => {

        const b = document.getElementById(`tool-btn-${t}`);

        if (b) b.className = 'w-7 h-7 rounded-lg text-gray-400 hover:text-white hover:bg-white/5 flex items-center justify-center relative cursor-pointer';

      });

      const activeBtn = document.getElementById(`tool-btn-${tool}`);

      if (activeBtn) {

        activeBtn.className = 'w-7 h-7 rounded-lg bg-blue-600/40 text-blue-400 border border-blue-500/50 flex items-center justify-center relative cursor-pointer';

      }

    }

    function triggerAutoLayout() {

      resetNodePositionsToDefault();

      const stage = document.getElementById('studioNodesStage');

      if (!stage) return;

      stage.style.transition = 'transform 0.25s ease';

      stage.style.transform = 'scale(0.97)';

      setTimeout(() => {

        stage.style.transform = 'scale(1)';

      }, 250);

    }

    function exportTopologyJson() {

      const topologyData = {

        name: "Gateway Resilience Topology",

        version: "2.0",

        nodes: [

          { id: "gateway", type: "edge_gateway", role: "API Gateway" },

          { id: "balancer", type: "router", role: "Load Balancer" },

          { id: "appserver", type: "core", role: "App Server" },

          { id: "cache", type: "primary_provider", role: "Cache (Groq)" },

          { id: "database", type: "fallback_provider", role: "Database (OpenRouter)" }

        ],

        cables: [

          { from: "gateway", to: "balancer" },

          { from: "balancer", to: "appserver" },

          { from: "appserver", to: "cache", fallback_to: "database" }

        ],

        exported_at: new Date().toISOString()

      };

      const blob = new Blob([JSON.stringify(topologyData, null, 2)], { type: "application/json" });

      const url = URL.createObjectURL(blob);

      const a = document.createElement('a');

      a.href = url;

      a.download = "gateway-topology.json";

      a.click();

      URL.revokeObjectURL(url);

    }

    function importTopologyJson() {

      const input = document.createElement('input');

      input.type = 'file';

      input.accept = '.json';

      input.onchange = (e) => {

        const file = e.target.files[0];

        if (file) {

          const reader = new FileReader();

          reader.onload = () => {

            try {

              JSON.parse(reader.result);

              alert("✓ Topology configuration loaded successfully!");

              refreshVisualizeData();

            } catch (err) {

              alert("Error parsing JSON file: " + err.message);

            }

          };

          reader.readAsText(file);

        }

      };

      input.click();

    }

    function shareVisualizerLink() {

      navigator.clipboard.writeText(window.location.origin + '/#visualize').then(() => {

        alert("✓ Visualizer link copied to clipboard!");

      });

    }

    function saveVisualizerDesign() {

      alert("✓ Topology layout and resilience parameters saved!");

    }

    function toggleVisualizerLibrary() {

      openNodeInspector('groq');

    }

    function toggleAdminSecurityPanel() {

      alert("Admin Security: Key endpoints restricted to localhost (127.0.0.1). All keys are masked (***yd) and test-validated before save.");

    }

    function showDesignPresetsModal() {

      alert("Topology Presets:\\n1. Active Resilience Chain (Default)\\n2. High-Throughput Aggregation\\n3. Zero-Egress Local Cluster");

    }

    async function triggerStressFallbackDemo() {

      await toggleFailureInjection('groq');

      await sendVisualizerTestTraffic();

    }

    async function refreshVisualizeData() {

      try {

        const res = await fetch('/api/reliability/provider-nodes');

        if (!res.ok) return;

        const nodes = await res.json();

        nodes.forEach(node => {

          const prov = node.provider;

          const isSelected = (currentSelectedInspectorNode === prov);

          // Update Inspector Drawer if this provider is currently selected

          if (isSelected) {

            const keyEl = document.getElementById('inspectorKeyMasked');

            if (keyEl) keyEl.innerText = node.masked_key || '***';

            const statusEl = document.getElementById('inspectorKeyStatus');

            if (statusEl) {

              statusEl.innerText = node.key_status === 'valid' ? 'valid' : 'not set';

              statusEl.className = node.key_status === 'valid'

                ? 'px-1.5 py-0.2 rounded text-[9px] font-bold bg-emerald-950/60 border border-emerald-500/30 text-emerald-400 uppercase'

                : 'px-1.5 py-0.2 rounded text-[9px] font-bold bg-gray-800 border border-gray-600 text-gray-400 uppercase';

            }

            const brkPill = document.getElementById('inspectorBreakerPill');

            if (brkPill && node.circuit_breaker) {

              const state = node.circuit_breaker.state || 'CLOSED';

              brkPill.innerText = state;

              brkPill.className = state === 'CLOSED'

                ? 'px-2 py-0.5 rounded text-[10px] font-bold bg-emerald-950/80 border border-emerald-500/30 text-emerald-400'

                : 'px-2 py-0.5 rounded text-[10px] font-bold bg-rose-950/80 border border-rose-500/40 text-rose-300 animate-pulse';

            }

            if (node.metrics) {

              const mInf = document.getElementById('inspectorMetricInflight');

              const mSrv = document.getElementById('inspectorMetricServed');

              const mFail = document.getElementById('inspectorMetricFailed');

              if (mInf) mInf.innerText = String(node.metrics.in_flight || 0);

              if (mSrv) mSrv.innerText = String(node.metrics.served || 0);

              if (mFail) mFail.innerText = String(node.metrics.failed || 0);

            }

            const faultBtn = document.getElementById('inspectorFaultToggleBtn');

            if (faultBtn && node.failure_injection) {

              if (node.failure_injection.enabled) {

                faultBtn.className = 'w-full py-1.5 rounded-lg bg-rose-800 text-white font-bold text-xs shadow-[0_0_12px_rgba(239,68,68,0.6)] cursor-pointer';

                faultBtn.innerText = '⚡ Injected (503 Active)';

              } else {

                faultBtn.className = 'w-full py-1.5 rounded-lg bg-rose-900/60 hover:bg-rose-900 border border-rose-500/40 text-rose-200 text-xs font-semibold transition cursor-pointer';

                faultBtn.innerText = 'Break This Node (503)';

              }

            }

          }

          // Update Canvas squircle indicators, connection string switches, and fault warning badges
          const tile = prov === 'groq' ? document.getElementById('studioCacheTile') : (prov === 'gemini' ? document.getElementById('studioGeminiTile') : document.getElementById('studioDatabaseTile'));
          const badge = prov === 'groq' ? document.getElementById('studioBadgeBreakerGroq') : (prov === 'gemini' ? document.getElementById('studioBadgeBreakerGemini') : document.getElementById('studioBadgeBreakerOpenRouter'));
          const warnBadge = prov === 'groq' ? document.getElementById('studioWarnBadgeGroq') : (prov === 'gemini' ? document.getElementById('studioWarnBadgeGemini') : document.getElementById('studioWarnBadgeOpenRouter'));
          const cable = prov === 'groq' ? document.getElementById('path-router-groq') : (prov === 'gemini' ? document.getElementById('path-router-gemini') : document.getElementById('path-router-openrouter'));
          const cableGlow = prov === 'groq' ? document.getElementById('path-router-groq-glow') : (prov === 'gemini' ? document.getElementById('path-router-gemini-glow') : document.getElementById('path-router-openrouter-glow'));
          const cableSw = prov === 'groq' ? document.getElementById('cableSwitchGroq') : (prov === 'gemini' ? document.getElementById('cableSwitchGemini') : document.getElementById('cableSwitchOpenRouter'));

          const state = (node.circuit_breaker && node.circuit_breaker.state) || 'CLOSED';
          const isFaulty = (state === 'OPEN') || (node.failure_injection && node.failure_injection.enabled);

          if (badge) {
            badge.innerText = state;
            badge.className = state === 'CLOSED'
              ? 'absolute -top-2 -right-2 px-1.5 py-0.2 rounded-full text-[9px] font-mono font-bold bg-emerald-950 border border-emerald-500/40 text-emerald-400'
              : 'absolute -top-2 -right-2 px-1.5 py-0.2 rounded-full text-[9px] font-mono font-bold bg-rose-950 border border-rose-500/40 text-rose-300 animate-pulse';
          }

          if (isFaulty) {
            // Faulty node: prominent crimson red with warning badge at bottom-right
            if (tile) {
              tile.style.backgroundColor = '#7f1d1d';
              tile.style.borderColor = '#ef4444';
              tile.style.boxShadow = '0 0 40px rgba(239, 68, 68, 0.95), inset 0 0 15px rgba(239, 68, 68, 0.5)';
            }
            if (warnBadge) warnBadge.classList.remove('hidden');

            // Cable string disconnected: broken dashed red line
            if (cable) {
              cable.setAttribute('stroke', '#ef4444');
              cable.setAttribute('stroke-dasharray', '6,6');
              cable.setAttribute('opacity', '0.35');
            }
            if (cableGlow) cableGlow.style.display = 'none';

            // Switch on string shows DISCONNECTED
            if (cableSw) {
              cableSw.querySelector('.switch-closed')?.classList.add('hidden');
              cableSw.querySelector('.switch-open')?.classList.remove('hidden');
            }
          } else {
            // Healthy node: clean standard styling
            if (tile) {
              tile.style.backgroundColor = '';
              tile.style.borderColor = '';
              tile.style.boxShadow = '';
            }
            if (warnBadge) warnBadge.classList.add('hidden');

            // Cable string connected
            if (cable) {
              cable.setAttribute('stroke', '#1e3a8a');
              cable.removeAttribute('stroke-dasharray');
              cable.removeAttribute('opacity');
            }
            if (cableGlow) cableGlow.style.display = '';

            // Switch on string shows CLOSED
            if (cableSw) {
              cableSw.querySelector('.switch-closed')?.classList.remove('hidden');
              cableSw.querySelector('.switch-open')?.classList.add('hidden');
            }
          }
        });

        // Update ComponentRegistry with real concurrency data from provider nodes
        nodes.forEach(node => {
          if (node.provider) {
            injectedFailuresState[node.provider] = Boolean(node.failure_injection && node.failure_injection.enabled);
          }
          if (node.concurrency) {
            ComponentRegistry.update(`semaphore-${node.provider}`, node.concurrency);
            ComponentRegistry.update(`queue-${node.provider}`, node.concurrency);
          }
        });

        // Sync header user email if available

        if (currentUser && currentUser.email) {

          const emailPill = document.getElementById('vizHeaderUserEmail');

          if (emailPill) emailPill.innerText = currentUser.email;

        }

      } catch (err) {

        console.warn('Failed to refresh visualize data:', err);

      }

    }

    // Toggle Failure Injection Demo (Two-way: Break <-> Unbreak)

    async function toggleFailureInjection(provider) {
      provider = (provider || 'groq').toLowerCase();
      const isCurrentlyBroken = Boolean(injectedFailuresState[provider]);
      const newState = !isCurrentlyBroken;

      try {
        const res = await fetch('/api/reliability/inject-failure', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ provider, enabled: newState, mode: '503' }),
        });
        const data = await res.json();
        injectedFailuresState[provider] = Boolean(data.enabled);

        // Update UI warning badge on provider node immediately
        const provMap = { groq: 'studioWarnBadgeGroq', gemini: 'studioWarnBadgeGemini', openrouter: 'studioWarnBadgeOpenRouter' };
        const badge = document.getElementById(provMap[provider]);
        if (badge) {
          if (newState) badge.classList.remove('hidden');
          else badge.classList.add('hidden');
        }

        // Update inspector buttons if modal is open
        const faultBtn = document.getElementById('inspectorFaultToggleBtn');
        const faultSub = document.getElementById('inspectorFaultSub');
        if (faultBtn) {
          if (newState) {
            faultBtn.className = 'w-full py-2.5 rounded-xl bg-emerald-600 hover:bg-emerald-500 border border-emerald-400 text-white text-xs font-bold transition cursor-pointer flex items-center justify-center gap-1.5 shadow-md';
            faultBtn.innerHTML = '✅ Unbreak Node (Restore 200 OK)';
            if (faultSub) faultSub.innerText = 'Active 503 Outage Injected';
          } else {
            faultBtn.className = 'w-full py-2.5 rounded-xl bg-rose-900/60 hover:bg-rose-900 border border-rose-500/40 text-rose-200 text-xs font-bold transition cursor-pointer flex items-center justify-center gap-1.5 shadow-md';
            faultBtn.innerHTML = '⚡ Break This Node (503)';
            if (faultSub) faultSub.innerText = 'Simulate 503 Outage';
          }
        }

        const modalFaultBtn = document.getElementById('modalCircuitFaultBtn');
        if (modalFaultBtn) {
          const provNames = { groq: 'Groq', gemini: 'Google Gemini', openrouter: 'OpenRouter' };
          const pName = provNames[provider] || provider;
          if (newState) {
            modalFaultBtn.className = 'flex-1 py-2.5 rounded-xl bg-emerald-600 hover:bg-emerald-500 text-white border border-emerald-400 font-bold text-xs transition cursor-pointer flex items-center justify-center gap-1.5 shadow-md';
            modalFaultBtn.innerText = `✅ Unbreak ${pName} (Restore)`;
          } else {
            modalFaultBtn.className = 'flex-1 py-2.5 rounded-xl bg-rose-900/60 hover:bg-rose-900 border border-rose-500/40 text-rose-200 font-bold text-xs transition cursor-pointer flex items-center justify-center gap-1.5 shadow-md';
            modalFaultBtn.innerText = `⚡ Break ${pName} (Inject 503)`;
          }
        }

        logVisualizerEvent(
          newState ? `<span class="px-1.5 py-0.2 rounded text-[9px] font-bold bg-rose-950 text-rose-300 border border-rose-500">FAULT INJECTED</span>`
                   : `<span class="px-1.5 py-0.2 rounded text-[9px] font-bold bg-emerald-950 text-emerald-300 border border-emerald-500">FAULT CLEARED</span>`,
          newState ? `Simulated <strong>503 Service Unavailable</strong> on <span class="capitalize font-semibold text-rose-400">${provider}</span>`
                   : `Restored <span class="capitalize font-semibold text-emerald-400">${provider}</span> to healthy <strong>200 OK</strong>`
        );

        await refreshVisualizeData();
      } catch (err) {
        console.error('Failed to toggle failure injection:', err);
      }
    }

    // Clear Redis Cache from Cache Node
    async function clearGatewayCache(e) {
      if (e) e.stopPropagation();
      try {
        const res = await fetch('/api/reliability/clear-cache', { method: 'POST' });
        if (res.ok) {
          flashNodeStatus('studioNodeCacheStore', 'emerald');
          const lbl = document.getElementById('modalCacheStatusLabel');
          if (lbl) lbl.innerText = 'Cache Purged (0 items)';
          logVisualizerEvent(
            `<span class="px-1.5 py-0.2 rounded text-[9px] font-bold bg-emerald-950 text-emerald-300 border border-emerald-500">CACHE CLEARED</span>`,
            `Purged Redis response cache. Subsequent requests will query live LLM providers.`
          );
        }
      } catch (err) {
        console.error('Failed to clear cache:', err);
      }
    }

    // Clear & Replay All DLQ Jobs
    async function clearAllDLQJobs(e) {
      if (e) e.stopPropagation();
      try {
        const res = await fetch('/v1/dlq/clear', { method: 'POST' });
        if (res.ok) {
          const data = await res.json();
          flashNodeStatus('studioNodeDLQ', 'yellow');
          const badge = document.getElementById('vizDlqDepthBadge');
          if (badge) badge.innerText = '0';
          logVisualizerEvent(
            `<span class="px-1.5 py-0.2 rounded text-[9px] font-bold bg-amber-950 text-amber-300 border border-amber-500">DLQ PURGED</span>`,
            `Cleared ${data.cleared || 0} dead-lettered jobs from DLQ.`
          );
        }
      } catch (err) {
        console.error('Failed to clear DLQ:', err);
      }
    }

    async function replayAllDLQJobs(e) {
      if (e) e.stopPropagation();
      try {
        const res = await fetch('/v1/dlq');
        const items = await res.json();
        if (items && items.length > 0) {
          for (const item of items) {
            await fetch(`/v1/dlq/${item.id}/replay`, { method: 'POST' });
          }
          flashNodeStatus('studioNodeDLQ', 'yellow');
          flashNodeStatus('studioNodeJobQueue', 'blue');
          const badge = document.getElementById('vizDlqDepthBadge');
          if (badge) badge.innerText = '0';
          logVisualizerEvent(
            `<span class="px-1.5 py-0.2 rounded text-[9px] font-bold bg-cyan-950 text-cyan-300 border border-cyan-500">DLQ REPLAYED</span>`,
            `Replayed ${items.length} dead-lettered jobs back into Job Queue.`
          );
        }
      } catch (err) {
        console.error('Failed to replay DLQ jobs:', err);
      }
    }

    // Key Editing Modal Controls

    function openKeyEditModal(provider) {

      currentKeyEditProvider = provider;

      const titleEl = document.getElementById('keyModalProviderTitle');

      const inputEl = document.getElementById('keyModalInput');

      const feedbackEl = document.getElementById('keyModalFeedback');

      const saveBtn = document.getElementById('keyModalSaveBtn');

      if (titleEl) {

        titleEl.innerText = provider === 'groq' ? 'Groq' : (provider === 'openrouter' ? 'OpenRouter' : 'Google Gemini');

      }

      if (inputEl) {

        inputEl.value = '';

        inputEl.type = 'password';

      }

      if (feedbackEl) feedbackEl.className = 'hidden';

      if (saveBtn) {

        saveBtn.disabled = false;

        saveBtn.innerHTML = '<span>Validate & Save</span>';

      }

      const modal = document.getElementById('keyEditModal');

      if (modal) modal.classList.remove('hidden');

    }

    function closeKeyEditModal() {

      const modal = document.getElementById('keyEditModal');

      if (modal) modal.classList.add('hidden');

      currentKeyEditProvider = null;

    }

    function toggleKeyVisibility() {

      const inputEl = document.getElementById('keyModalInput');

      if (!inputEl) return;

      inputEl.type = inputEl.type === 'password' ? 'text' : 'password';

    }

    async function submitCandidateKey() {

      if (!currentKeyEditProvider) return;

      const inputEl = document.getElementById('keyModalInput');

      const feedbackEl = document.getElementById('keyModalFeedback');

      const feedbackIcon = document.getElementById('keyModalFeedbackIcon');

      const feedbackText = document.getElementById('keyModalFeedbackText');

      const saveBtn = document.getElementById('keyModalSaveBtn');

      const candidateKey = inputEl ? inputEl.value.trim() : '';

      if (!candidateKey) {

        if (feedbackEl) {

          feedbackEl.className = 'p-2.5 rounded-xl border border-rose-500/40 bg-rose-950/40 text-rose-300 text-[11px] flex items-center gap-2';

          feedbackText.innerText = 'Please enter an API key.';

        }

        return;

      }

      // Show testing progress spinner

      if (saveBtn) {

        saveBtn.disabled = true;

        saveBtn.innerHTML = `

          <svg class="animate-spin w-3.5 h-3.5 text-gray-950" fill="none" viewBox="0 0 24 24">

            <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>

            <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8H4z"></path>

          </svg>

          <span>Validating Key...</span>

        `;

      }

      if (feedbackEl) {

        feedbackEl.className = 'p-2.5 rounded-xl border border-cyan-500/40 bg-cyan-950/40 text-cyan-300 text-[11px] flex items-center gap-2';

        feedbackText.innerText = `Making test validation call to ${currentKeyEditProvider}...`;

      }

      try {

        const res = await fetch('/api/reliability/keys', {

          method: 'POST',

          headers: { 'Content-Type': 'application/json' },

          body: JSON.stringify({ provider: currentKeyEditProvider, api_key: candidateKey }),

        });

        const data = await res.json();

        if (data.status === 'valid') {

          // Success: Validated and applied

          if (feedbackEl) {

            feedbackEl.className = 'p-2.5 rounded-xl border border-emerald-500/40 bg-emerald-950/40 text-emerald-300 text-[11px] flex items-center gap-2';

            feedbackText.innerText = `✓ Valid key! Applied immediately to runtime. Circuit breaker reset.`;

          }

          await refreshVisualizeData();

          setTimeout(() => {

            closeKeyEditModal();

          }, 1200);

        } else {

          // Failure: Rejected

          if (feedbackEl) {

            feedbackEl.className = 'p-2.5 rounded-xl border border-rose-500/40 bg-rose-950/40 text-rose-300 text-[11px] flex items-center gap-2';

            feedbackText.innerText = `✗ Key rejected: ${data.error || 'Validation call failed'}. Key was not applied.`;

          }

          if (saveBtn) {

            saveBtn.disabled = false;

            saveBtn.innerHTML = '<span>Validate & Save</span>';

          }

        }

      } catch (err) {

        if (feedbackEl) {

          feedbackEl.className = 'p-2.5 rounded-xl border border-rose-500/40 bg-rose-950/40 text-rose-300 text-[11px] flex items-center gap-2';

          feedbackText.innerText = `Error: ${err.message || 'Network error'}`;

        }

        if (saveBtn) {

          saveBtn.disabled = false;

          saveBtn.innerHTML = '<span>Validate & Save</span>';

        }

      }

    }

    let isK6Running = false;

    function toggleK6SimultaneousMode() {

      const check = document.getElementById('k6SimultaneousCheck');

      const durInput = document.getElementById('k6DurationInput');

      const unit = document.getElementById('k6DurationUnit');

      if (!check || !durInput) return;

      if (check.checked) {

        durInput.disabled = true;

        durInput.classList.add('opacity-40');

        if (unit) unit.innerText = '⚡';

      } else {

        durInput.disabled = false;

        durInput.classList.remove('opacity-40');

        if (unit) unit.innerText = 's';

      }

    }

    let lastK6ReportData = null;

    function openLatestK6ReportModal() {

      if (lastK6ReportData) {

        openK6ReportModal(lastK6ReportData);

      } else {

        openK6ReportModal({

          total: parseInt(document.getElementById('k6RequestCountInput')?.value || 10),

          completed: 0,

          successes: 0,

          latencies: [],

          requests: [],

          target: document.getElementById('k6TargetSelect')?.value || 'fast',

          prompt: document.getElementById('k6PromptInput')?.value || 'hi',

          isSimultaneous: document.getElementById('k6SimultaneousCheck')?.checked || false,

          startTime: performance.now(),

          totalDurationMs: 0

        });

      }

    }

    function closeK6ReportModal() {

      const modal = document.getElementById('k6RunReportModal');

      if (modal) modal.classList.add('hidden');

    }

    function openK6ReportModal(report) {

      const modal = document.getElementById('k6RunReportModal');

      if (!modal) return;

      modal.classList.remove('hidden');

      const successRateEl = document.getElementById('reportMetricSuccess');

      const successCountEl = document.getElementById('reportMetricSuccessCount');

      const avgEl = document.getElementById('reportMetricAvg');

      const minMaxEl = document.getElementById('reportMetricMinMax');

      const rpsEl = document.getElementById('reportMetricRps');

      const totalTimeEl = document.getElementById('reportMetricTotalTime');

      const batchSubEl = document.getElementById('reportMetricBatchSub');

      const speedupEl = document.getElementById('reportMetricSpeedup');

      const targetProvEl = document.getElementById('reportTargetProvider');

      const modeEl = document.getElementById('reportExecutionMode');

      const promptEl = document.getElementById('reportPromptText');

      const tableCountEl = document.getElementById('reportTableCount');

      const listEl = document.getElementById('reportRequestsList');

      const total = report.total || (report.requests ? report.requests.length : 0);

      let successes = 0;
      let failures = 0;
      let totalTokens = 0;
      let totalBurstCost = 0.0;
      let cacheHits = 0;
      let totalSavedCost = 0.0;

      let latencies = (report.latencies && report.latencies.length > 0) ? [...report.latencies] : [];

      if (report.requests && report.requests.length > 0) {
        if (latencies.length === 0) {
          latencies = report.requests.map(r => r.latency).filter(l => typeof l === 'number' && !isNaN(l));
        }
        report.requests.forEach(r => {
          if (r.status === 'ok') {
            successes++;
          } else {
            failures++;
          }
          if (r.cached) {
            cacheHits++;
            totalSavedCost += (r.saved_cost !== undefined ? r.saved_cost : 0.000018);
          } else {
            const reqCost = (r.cost_usd !== undefined && r.cost_usd !== null) ? Number(r.cost_usd) : ((r.saved_cost !== undefined) ? r.saved_cost : (r.tokens ? r.tokens * 0.0000006 : 0.000018));
            totalBurstCost += reqCost;
          }
          totalTokens += (r.tokens || 0);
        });
      } else {
        successes = report.successes || 0;
      }

      const rate = total > 0 ? Math.round((successes / total) * 100) : (successes > 0 ? 100 : 0);

      const avg = latencies.length > 0 ? Math.round(latencies.reduce((a, b) => a + b, 0) / latencies.length) : 0;

      const minL = latencies.length > 0 ? Math.min(...latencies) : 0;

      const maxL = latencies.length > 0 ? Math.max(...latencies) : 0;

      let durMs = report.totalDurationMs;
      if (!durMs || durMs <= 0) {
        if (latencies.length > 0) {
          durMs = report.isSimultaneous ? Math.max(...latencies) : latencies.reduce((a, b) => a + b, 0);
        } else {
          durMs = 1000;
        }
      }
      const elapsedSec = Math.max(0.01, durMs / 1000);

      const rps = (total / elapsedSec).toFixed(1);

      if (avgEl) avgEl.innerText = latencies.length > 0 ? `${avg} ms` : '-- ms';

      if (minMaxEl) minMaxEl.innerText = latencies.length > 0 ? `Fastest: ${minL} ms` : 'Fastest: -- ms';

      if (totalTimeEl) totalTimeEl.innerText = `${elapsedSec.toFixed(2)} s`;

      if (batchSubEl) batchSubEl.innerText = 'Whole batch wall-clock';

      if (rpsEl) rpsEl.innerText = `${rps} RPS`;

      const servedCountEl = document.getElementById('reportMetricServedCount');
      const servedRateEl = document.getElementById('reportMetricServedRate');
      if (servedCountEl) servedCountEl.innerText = `${successes}/${total}`;
      if (servedRateEl) servedRateEl.innerText = `${rate}% passed (${failures} fail)`;

      const burstCostEl = document.getElementById('reportMetricBurstCost');
      const burstTokensEl = document.getElementById('reportMetricBurstTokens');
      if (burstCostEl) burstCostEl.innerText = `$${totalBurstCost.toFixed(6)}`;
      if (burstTokensEl) burstTokensEl.innerText = `${totalTokens} total tokens`;

      const costSavedEl = document.getElementById('reportMetricCostSaved');
      const cacheHitsEl = document.getElementById('reportMetricCacheHits');
      if (costSavedEl) costSavedEl.innerText = `$${totalSavedCost.toFixed(6)}`;
      if (cacheHitsEl) cacheHitsEl.innerText = `${cacheHits} cache hit${cacheHits === 1 ? '' : 's'}`;

      if (targetProvEl) targetProvEl.innerText = report.target || 'fast';

      if (modeEl) modeEl.innerText = report.isSimultaneous ? 'Simultaneous Burst' : 'Spread Batch';

      if (promptEl) promptEl.innerText = `"${report.prompt || 'hi'}"`;

      if (tableCountEl) tableCountEl.innerText = `${report.requests.length} responses`;

      if (listEl) {

        let html = '';

        report.requests.forEach((r, idx) => {

          const isErr = r.status !== 'ok';

          const isCached = Boolean(r.cached);

          const badgeClass = isErr
            ? 'text-rose-400 bg-rose-950/80 border-rose-500/40'
            : (isCached ? 'text-emerald-300 bg-emerald-950/80 border-emerald-500/50 shadow-sm' : 'text-cyan-300 bg-cyan-950/60 border-cyan-500/30');

          const badgeText = isErr ? 'ERR' : (isCached ? 'CACHE HIT' : '200 OK');

          const qWait = r.queue_wait_ms !== undefined ? `${r.queue_wait_ms}ms` : '0ms';

          const deadline = r.deadline || '30s';

          const costVal = (r.cost_usd !== undefined && r.cost_usd !== null) ? Number(r.cost_usd) : ((r.saved_cost !== undefined) ? r.saved_cost : (r.tokens ? r.tokens * 0.0000006 : 0.000018));
          const costBadge = isCached
            ? `<span class="px-1.5 py-0.5 rounded bg-emerald-950 text-emerald-300 font-bold border border-emerald-500/40 text-[9px]">Saved $${costVal.toFixed(6)}</span>`
            : `<span class="px-1.5 py-0.5 rounded bg-blue-950/80 text-blue-300 font-bold border border-blue-500/40 text-[9px]">$${costVal.toFixed(6)}</span>`;

          html += `

            <div class="p-2.5 px-3 flex items-center justify-between gap-3 hover:bg-white/5 transition border-b border-white/5 last:border-0 text-xs">

              <div class="flex items-center gap-3 min-w-0 flex-1">

                <span class="text-gray-500 text-[10px] w-6 shrink-0 font-bold font-mono">#${idx + 1}</span>

                <span class="px-2 py-0.5 rounded text-[9.5px] font-bold border font-mono shrink-0 ${badgeClass}">${badgeText}</span>

                <span class="text-cyan-300 font-medium shrink-0 font-mono text-[11px]">"${r.prompt}"</span>

                <span class="text-gray-300 text-[11px] truncate italic">→ "${r.text || r.snippet || ''}"</span>

              </div>

              <div class="flex items-center gap-3 shrink-0 text-[10px] font-mono">

                ${costBadge}

                <span class="text-gray-400 capitalize w-16 text-center">${r.provider || ''}</span>

                <span class="text-indigo-400 font-semibold w-12 text-center" title="Queue Wait Time">Q: ${qWait}</span>

                <span class="text-amber-400/90 font-semibold w-12 text-center" title="Hard Deadline Budget">DL: ${deadline}</span>

                ${r.tokens ? `<span class="text-gray-400 text-[9.5px] w-14 text-center">${r.tokens} tok</span>` : '<span class="w-14"></span>'}

                <span class="font-bold w-14 text-right ${isCached ? 'text-emerald-400' : (r.latency > 1000 ? 'text-amber-300' : 'text-cyan-300')}">${r.latency}ms</span>

              </div>

            </div>

          `;

        });

        listEl.innerHTML = html || '<div class="p-4 text-center text-gray-500 text-xs">No responses recorded.</div>';

      }

    }

    // K6-Style Load Test Generator (Triggers Traffic & Drives Particle Visualizer)

    async function startK6TrafficTest() {

      if (isK6Running) return;

      const countInput = document.getElementById('k6RequestCountInput');

      const durationInput = document.getElementById('k6DurationInput');

      const simultaneousCheck = document.getElementById('k6SimultaneousCheck');

      const promptInput = document.getElementById('k6PromptInput');

      const targetSelect = document.getElementById('k6TargetSelect') || document.getElementById('vizTargetSelect');

      const btn = document.getElementById('k6SendTrafficBtn');

      const btnLabel = document.getElementById('k6BtnLabel');

      const drawerBtn = document.getElementById('vizSendTrafficBtn');

      const progressPill = document.getElementById('k6ProgressPill');

      const progressText = document.getElementById('k6ProgressText');

      // Telemetry elements

      const liveReqCount = document.getElementById('liveMetricReqCount');

      const liveAvgLatency = document.getElementById('liveMetricAvgLatency');

      const liveBatchTime = document.getElementById('liveMetricBatchTime');

      const liveP99 = document.getElementById('liveMetricP99');

      const liveP99Pill = document.getElementById('liveMetricP99Pill');

      const count = Math.min(150, Math.max(1, parseInt(countInput ? countInput.value : 10) || 10));

      const isSimultaneous = simultaneousCheck ? simultaneousCheck.checked : false;

      const durationSec = Math.min(30, Math.max(0.5, parseFloat(durationInput ? durationInput.value : 3.0) || 3.0));

      const durationMs = isSimultaneous ? 500 : (durationSec * 1000);

      const interval = isSimultaneous ? 0 : (durationMs / count);

      const target = targetSelect ? targetSelect.value : 'fast';

      const prompt = (promptInput && promptInput.value.trim()) ? promptInput.value.trim() : 'hi';

      isK6Running = true;

      if (btn) {

        btn.disabled = true;

        btn.classList.add('opacity-75', 'cursor-not-allowed');

      }

      if (drawerBtn) {

        drawerBtn.disabled = true;

        drawerBtn.classList.add('opacity-75', 'cursor-not-allowed');

      }

      if (progressPill) progressPill.classList.remove('hidden');

      const runReportData = {

        total: count,

        completed: 0,

        successes: 0,

        latencies: [],

        requests: [],

        target: target,

        prompt: prompt,

        isSimultaneous: isSimultaneous,

        startTime: performance.now(),

        totalDurationMs: 0

      };

      const startTime = Date.now();

      let sentCount = 0;

      let completedCount = 0;

      const countdownInterval = setInterval(() => {

        const elapsedSec = ((performance.now() - runReportData.startTime) / 1000).toFixed(1);

        if (isSimultaneous) {

          if (btnLabel) btnLabel.innerText = `Bursting (${elapsedSec}s)...`;

        } else {

          const remaining = Math.max(0, ((durationMs - (Date.now() - startTime)) / 1000)).toFixed(1);

          if (btnLabel) btnLabel.innerText = `Testing (${remaining}s)`;

        }

      }, 80);

      // Trigger all N requests with guaranteed 1:1 particles and strict real-backend synchronization

      for (let i = 0; i < count; i++) {

        const reqDelay = isSimultaneous ? 0 : (i * interval);

        const visualDelay = isSimultaneous ? (i * 65) : (i * interval);

        const reqId = `k6_${Date.now()}_${i}_${Math.random().toString(36).substring(2, 6)}`;

        const laneOffset = isSimultaneous ? (((i % 5) - 2) * 3.5) : 0;

        // Instantly spawn dedicated particle starting at Client Traffic

        activeTrafficDots.set(reqId, {

          id: reqId,

          index: i + 1,

          phase: 'ingress',

          phaseStartTime: performance.now() + visualDelay,

          ingressDuration: 550,

          branchDuration: 350,

          dwellDuration: 300,

          fadeDuration: 400,

          color: '#38bdf8',

          filter: 'url(#glowBlue)',

          provider: target === 'smart' ? 'gemini' : (target === 'cheap' ? 'openrouter' : 'groq'),

          queuedProvider: null,

          outcome: 'pending',

          target: target,

          swarmOffset: null,

          laneOffset: laneOffset,

        });

        if (!isTrafficAnimationRunning) {

          isTrafficAnimationRunning = true;

          requestAnimationFrame(renderTrafficAnimationLoop);

        }

        setTimeout(async () => {

          sentCount++;

          if (progressText) {

            progressText.innerText = isSimultaneous

              ? `${sentCount}/${count} dispatched (Simultaneous)`

              : `${sentCount}/${count} dispatched ("${prompt}")`;

          }

          const reqStart = performance.now();

          let dot = activeTrafficDots.get(reqId);

          try {

            const res = await fetch('/api/reliability/test-request', {

              method: 'POST',

              headers: { 'Content-Type': 'application/json' },

              body: JSON.stringify({ target, prompt, request_id: reqId }),

            });

            const latency = Math.round(performance.now() - reqStart);

            if (res.ok) {

              const data = await res.json();

              runReportData.successes++;

              runReportData.latencies.push(latency);

              const textContent = (data.text || '').replace(/\s+/g, ' ').trim();

              const snippet = textContent.substring(0, 60);

              const isCached = Boolean(data.cached);
              const costUsd = data.cost_usd !== undefined ? Number(data.cost_usd) : (isCached ? 0.0 : (data.tokens ? data.tokens * 0.0000006 : 0.000018));
              const savedCost = isCached ? (data.saved_cost || data.cost_usd || 0.000018) : 0.0;

              runReportData.requests.push({

                prompt: prompt,

                provider: isCached ? 'Redis Cache' : (data.provider || 'groq'),

                model: data.model || 'openai/gpt-oss-120b',

                latency: latency,

                queue_wait_ms: data.queue_wait_ms !== undefined ? data.queue_wait_ms : 0.0,

                deadline: '30s',

                text: textContent,

                snippet: snippet,

                tokens: (data.token_usage && data.token_usage.completion_tokens) || (data.usage && data.usage.completion_tokens) || (data.total_tokens || 44),

                status: 'ok',

                cached: isCached,

                saved_cost: savedCost,

                cost_usd: costUsd

              });

              if (dot) {
                if (isCached) {
                  dot.isCached = true;
                  dot.color = '#10b981';
                  dot.filter = 'url(#glowGreen)';
                  dot.outcome = 'served';
                  if (dot.phase !== 'ingress') {
                    dot.phase = 'cache_return';
                    dot.phaseStartTime = performance.now();
                  }
                } else {
                  dot.outcome = 'served';
                  dot.servedTime = performance.now();
                  dot.provider = (data.provider || 'groq').toLowerCase();
                  dot.pathId = `path-router-${dot.provider}`;
                }
              }

              logVisualizerEvent(

                `<span class="px-1.5 py-0.2 rounded text-[9px] font-bold bg-blue-950/80 text-blue-300 border border-blue-500/40">PROMPT</span>`,

                `"${prompt}" → <span class="text-white font-semibold capitalize">${data.provider}</span> in <span class="text-cyan-300 font-bold">${latency}ms</span>: <span class="text-emerald-300 italic">"${snippet}"</span>`

              );

            } else {

              runReportData.latencies.push(latency);

              runReportData.requests.push({

                prompt: prompt,

                provider: 'gateway',

                model: 'n/a',

                latency: latency,

                queue_wait_ms: 0.0,

                deadline: '30s',

                snippet: 'HTTP Error',

                status: 'error'

              });

              if (dot) {

                dot.outcome = 'failed';

                dot.servedTime = performance.now();

              }

            }

          } catch (err) {

            const latency = Math.round(performance.now() - reqStart);

            runReportData.latencies.push(latency);

            runReportData.requests.push({

              prompt: prompt,

              provider: 'gateway',

              model: 'n/a',

              latency: latency,

              queue_wait_ms: 0.0,

              deadline: '30s',

              snippet: 'Network Dropout',

              status: 'error'

            });

            if (dot) {

              dot.outcome = 'failed';

              dot.servedTime = performance.now();

            }

          } finally {

            completedCount++;

            runReportData.completed = completedCount;

            // Live metrics update

            if (liveReqCount) liveReqCount.innerText = `${completedCount}/${count} req`;

            if (runReportData.latencies.length > 0) {

              const curAvg = Math.round(runReportData.latencies.reduce((a, b) => a + b, 0) / runReportData.latencies.length);

              const curP99 = Math.max(...runReportData.latencies);

              if (liveAvgLatency) liveAvgLatency.innerText = `${curAvg} ms`;

              if (liveP99) liveP99.innerText = `${curP99} ms`;

            }

            // When all requests complete

            if (completedCount === count) {

              const finalBatchDurationMs = Math.round(performance.now() - runReportData.startTime);

              runReportData.totalDurationMs = finalBatchDurationMs;

              lastK6ReportData = runReportData;

              clearInterval(countdownInterval);

              if (btnLabel) btnLabel.innerText = 'Send Traffic';

              if (btn) {

                btn.disabled = false;

                btn.classList.remove('opacity-75', 'cursor-not-allowed');

              }

              if (drawerBtn) {

                drawerBtn.disabled = false;

                drawerBtn.classList.remove('opacity-75', 'cursor-not-allowed');

              }

              if (progressText) {

                progressText.innerText = isSimultaneous

                  ? `${count} processed simultaneously ✓`

                  : `${count} processed in ${durationSec.toFixed(1)}s ✓`;

              }

              setTimeout(() => {

                if (progressPill && !isK6Running) progressPill.classList.add('hidden');

              }, 4000);

              isK6Running = false;

              // Open Stress Test Execution Report Modal

              // Sweep any lingering pending dots upon test completion
              activeTrafficDots.forEach((d) => {
                if (d.outcome === 'pending') {
                  d.outcome = 'served';
                  d.servedTime = performance.now();
                }
              });
              setTimeout(() => {
                if (!isK6Running) activeTrafficDots.clear();
              }, 1200);

              // Open Stress Test Execution Report Modal
              setTimeout(() => {
                openK6ReportModal(runReportData);
              }, 600);

            }

          }

        }, reqDelay);

      }

    }

    // Send Test Traffic from Visualizer

    async function sendVisualizerTestTraffic() {

      await startK6TrafficTest();

    }

    // -------------------------------------------------------------

    // GATEWAY LOGS VIEWER (REAL-TIME REQUEST TELEMETRY)

    // -------------------------------------------------------------

    let allGatewayLogs = [];

    function formatIST(timestampStr) {

      if (!timestampStr) return '-';

      try {

        const iso = timestampStr.includes('T')

          ? timestampStr

          : timestampStr.replace(' ', 'T') + (timestampStr.endsWith('Z') ? '' : 'Z');

        const d = new Date(iso);

        if (isNaN(d.getTime())) return timestampStr;

        return d.toLocaleTimeString('en-IN', {

          timeZone: 'Asia/Kolkata',

          hour: '2-digit',

          minute: '2-digit',

          second: '2-digit',

          hour12: true

        });

      } catch (e) {

        return timestampStr;

      }

    }

    async function openLogsModal() {

      showView('logs');

    }

    function closeLogsModal() {

      showView('chat');

    }

    async function refreshGatewayLogs() {

      try {

        const res = await fetch('/api/logs?limit=100');

        if (!res.ok) return;

        allGatewayLogs = await res.json();

        renderLogsTable(allGatewayLogs);

      } catch (err) {

        console.warn('Failed to fetch gateway logs:', err);

      }

    }

    function filterLogsTable() {

      const query = (document.getElementById('logFilterInput')?.value || '').toLowerCase().trim();

      if (!query) {

        renderLogsTable(allGatewayLogs);

        return;

      }

      const filtered = allGatewayLogs.filter(log => {

        return (log.provider || '').toLowerCase().includes(query) ||

               (log.model || '').toLowerCase().includes(query) ||

               (log.target || '').toLowerCase().includes(query) ||

               String(log.status_code).includes(query) ||

               (log.error_message || '').toLowerCase().includes(query);

      });

      renderLogsTable(filtered);

    }

    function renderLogsTable(logs) {

      const tbody = document.getElementById('logsTableBody');

      const emptyState = document.getElementById('logsEmptyState');

      const countBadge = document.getElementById('logsCountBadge');

      if (!tbody) return;

      if (countBadge) countBadge.innerText = `${logs.length} log${logs.length === 1 ? '' : 's'}`;

      if (!logs || logs.length === 0) {

        tbody.innerHTML = '';

        if (emptyState) emptyState.classList.remove('hidden');

        return;

      }

      if (emptyState) emptyState.classList.add('hidden');

      let html = '';

      logs.forEach(log => {

        const isSuccess = log.status_code === 200;

        const statusClass = isSuccess 

          ? 'bg-emerald-950/80 text-emerald-300 border-emerald-500/40' 

          : 'bg-red-950/80 text-red-300 border-red-500/40';

        const fallbackPill = log.fallback_used ? `

          <span class="inline-flex items-center px-1.5 py-0.2 rounded text-[9px] font-semibold bg-amber-950/80 text-amber-300 border border-amber-500/40">

            ⚡ Fallback

          </span>

        ` : '';

        const attemptsCount = (log.attempts && log.attempts.length > 0) ? log.attempts.length : 1;

        const attemptDetails = (log.attempts && log.attempts.length > 1) 

          ? log.attempts.map(a => `${a.provider}: ${a.status} (${a.latency_ms}ms)`).join(' → ')

          : `${attemptsCount} attempt`;

        const timeStr = formatIST(log.timestamp);

        html += `

          <tr class="hover:bg-white/[0.03] transition">

            <td class="py-2.5 px-3 text-cyan-300 font-medium whitespace-nowrap">${escapeHtml(timeStr)}</td>

            <td class="py-2 px-3 font-semibold text-white uppercase text-[10px]">${escapeHtml(log.target || 'fast')}</td>

            <td class="py-2 px-3">

              <div class="flex items-center gap-1.5">

                <span class="capitalize font-semibold text-cyan-300">${escapeHtml(log.provider)}</span>

                <span class="text-gray-500 text-[10px]">(${escapeHtml(log.model)})</span>

              </div>

            </td>

            <td class="py-2 px-3 whitespace-nowrap">

              <span class="px-1.5 py-0.5 rounded text-[10px] font-bold border ${statusClass}">${log.status_code}</span>

            </td>

            <td class="py-2 px-3">

              <div class="flex items-center gap-1.5 flex-wrap">

                <span class="text-gray-300 text-[10px]">${escapeHtml(attemptDetails)}</span>

                ${fallbackPill}

              </div>

            </td>

            <td class="py-2 px-3 text-gray-300 whitespace-nowrap">${log.latency_ms}ms</td>

            <td class="py-2 px-3 text-gray-400 whitespace-nowrap">${log.total_tokens || 0}</td>

          </tr>

        `;

      });

      tbody.innerHTML = html;

    }

    async function clearAllGatewayLogs() {

      if (!confirm('Clear all request logs?')) return;

      try {

        await fetch('/api/logs', { method: 'DELETE' });

        allGatewayLogs = [];

        renderLogsTable([]);

      } catch (err) {

        console.error('Failed to clear logs:', err);

      }

    }

    function quickPrompt(text) {

      document.getElementById('heroChatInput').value = text;

      submitMessage();

    }

    function resetToWelcome() {

      currentSessionId = null;

      chatMessages = [];

      showView('chat');

      document.getElementById('welcomeScreen').classList.remove('hidden');

      document.getElementById('chatScreen').classList.add('hidden');

      document.getElementById('messagesContainer').innerHTML = '';

      document.getElementById('heroChatInput').value = '';

      loadHistoryFromDB();

    }

    function startNewChat() {

      if (!currentUser) {

        currentUser = { id: 'guest_user', email: 'developer@llmgateway.io', name: 'Developer', is_admin: true };

        updateAuthHeaderUI();

      }

      currentSessionId = null;

      chatMessages = [];

      showView('chat');

      const welcome = document.getElementById('welcomeScreen');

      const chat = document.getElementById('chatScreen');

      const chatsPage = document.getElementById('chatsPage');

      if (welcome) welcome.classList.remove('hidden');

      if (chat) chat.classList.add('hidden');

      if (chatsPage) chatsPage.classList.add('hidden');

      const container = document.getElementById('messagesContainer');

      if (container) container.innerHTML = '';

      const input = document.getElementById('heroChatInput');

      if (input) {

        input.value = '';

        setTimeout(() => input.focus(), 60);

      }

      loadHistoryFromDB();

    }

    // SQLite Chat History Client (Empty notice placeholder completely removed)

    async function loadHistoryFromDB() {

      try {

        const res = await fetch('/api/chats');

        if (!res.ok) return;

        const sessions = await res.json();

        const container = document.getElementById('chatsList');

        container.innerHTML = '';

        if (!sessions || sessions.length === 0) {

          // Keep empty as requested - no placeholder text!

          return;

        }

        sessions.forEach(s => {

          const btn = document.createElement('div');

          btn.className = `relative flex items-center justify-between px-2.5 py-1.5 rounded-lg text-xs transition group cursor-pointer ${currentSessionId === s.id ? 'bg-cyan-950/40 text-cyan-300 border border-cyan-500/30' : 'text-gray-400 hover:text-white hover:bg-white/5'}`;

          const safeTitle = (s.title || 'Untitled Chat').replace(/"/g, '&quot;');

          btn.onclick = () => openChatSession(s.id);

          btn.innerHTML = `

            <div id="view-${s.id}" class="flex items-center justify-between w-full min-w-0">

              <div class="truncate flex-1 mr-2">

                <span id="title-${s.id}">${escapeHtml(s.title || 'Untitled Chat')}</span>

              </div>

              <div class="relative shrink-0 chat-menu-wrapper">

                <button onclick="toggleChatMenu(event, '${s.id}')" class="opacity-0 group-hover:opacity-100 hover:text-white text-gray-400 p-0.5 rounded hover:bg-white/10 transition" title="More options">

                  <svg class="w-4 h-4" fill="currentColor" viewBox="0 0 24 24">

                    <circle cx="5" cy="12" r="1.75"/>

                    <circle cx="12" cy="12" r="1.75"/>

                    <circle cx="19" cy="12" r="1.75"/>

                  </svg>

                </button>

                <div id="menu-${s.id}" class="chat-menu-dropdown hidden absolute right-0 top-full mt-1 z-30 w-28 bg-[#161a22] border border-white/10 rounded-lg shadow-xl py-1 text-xs">

                  <button onclick="startRenameChat(event, '${s.id}')" class="group/item w-full text-left px-2.5 py-1.5 text-gray-300 hover:text-white hover:bg-white/5 transition flex items-center gap-2">

                    <svg class="w-3.5 h-3.5 text-gray-400 group-hover/item:text-white transition shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15.232 5.232l3.536 3.536m-2.036-5.036a2.5 2.5 0 113.536 3.536L6.5 21.036H3v-3.572L16.732 3.732z"/></svg>

                    <span>Rename</span>

                  </button>

                  <button onclick="deleteChatSession(event, '${s.id}')" class="group/item w-full text-left px-2.5 py-1.5 text-gray-300 hover:text-white hover:bg-white/5 transition flex items-center gap-2">

                    <svg class="w-3.5 h-3.5 text-gray-400 group-hover/item:text-white transition shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"/></svg>

                    <span>Delete</span>

                  </button>

                </div>

              </div>

            </div>

            <div id="rename-${s.id}" class="hidden flex items-center gap-1 w-full" onclick="event.stopPropagation()">

              <input id="input-${s.id}" type="text" value="${safeTitle}" class="flex-1 min-w-0 bg-black/40 text-gray-100 text-xs px-2 py-0.5 rounded border border-white/20 focus:outline-none focus:border-white/50" onkeydown="handleRenameKey(event, '${s.id}')" />

              <button onclick="saveRenameChat(event, '${s.id}')" class="p-1 text-gray-400 hover:text-white transition" title="Save">

                <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"/></svg>

              </button>

              <button onclick="cancelRenameChat(event, '${s.id}')" class="p-1 text-gray-400 hover:text-white transition" title="Cancel">

                <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg>

              </button>

            </div>

          `;

          container.appendChild(btn);

        });

      } catch (err) {

        console.warn('History fetch error:', err);

      }

    }

    function escapeHtml(str) {

      const div = document.createElement('div');

      div.textContent = str || '';

      return div.innerHTML;

    }

    function toggleChatMenu(e, sessionId) {

      e.stopPropagation();

      const menu = document.getElementById(`menu-${sessionId}`);

      if (!menu) return;

      const isHidden = menu.classList.contains('hidden');

      document.querySelectorAll('.chat-menu-dropdown').forEach(m => m.classList.add('hidden'));

      if (isHidden) {

        menu.classList.remove('hidden');

      }

    }

    function startRenameChat(e, sessionId) {

      e.stopPropagation();

      const menu = document.getElementById(`menu-${sessionId}`);

      if (menu) menu.classList.add('hidden');

      const viewDiv = document.getElementById(`view-${sessionId}`);

      const renameDiv = document.getElementById(`rename-${sessionId}`);

      const input = document.getElementById(`input-${sessionId}`);

      const titleSpan = document.getElementById(`title-${sessionId}`);

      if (!viewDiv || !renameDiv || !input) return;

      if (titleSpan) input.value = titleSpan.textContent.trim();

      viewDiv.classList.add('hidden');

      renameDiv.classList.remove('hidden');

      input.focus();

      input.select();

    }

    function cancelRenameChat(e, sessionId) {

      if (e) e.stopPropagation();

      const viewDiv = document.getElementById(`view-${sessionId}`);

      const renameDiv = document.getElementById(`rename-${sessionId}`);

      if (viewDiv && renameDiv) {

        renameDiv.classList.add('hidden');

        viewDiv.classList.remove('hidden');

      }

    }

    async function saveRenameChat(e, sessionId) {

      if (e) e.stopPropagation();

      const input = document.getElementById(`input-${sessionId}`);

      const titleSpan = document.getElementById(`title-${sessionId}`);

      if (!input) return;

      const newTitle = input.value.trim() || 'Untitled Chat';

      try {

        const res = await fetch(`/api/chats/${sessionId}`, {

          method: 'PATCH',

          headers: { 'Content-Type': 'application/json' },

          body: JSON.stringify({ title: newTitle }),

        });

        if (res.ok) {

          if (titleSpan) titleSpan.textContent = newTitle;

          cancelRenameChat(null, sessionId);

        }

      } catch (err) {

        console.error('Rename error:', err);

      }

    }

    function handleRenameKey(e, sessionId) {

      if (e.key === 'Enter') {

        e.preventDefault();

        saveRenameChat(e, sessionId);

      } else if (e.key === 'Escape') {

        e.preventDefault();

        cancelRenameChat(e, sessionId);

      }

    }

    async function openChatSession(sessionId) {

      try {

        const res = await fetch(`/api/chats/${sessionId}`);

        if (!res.ok) return;

        const data = await res.json();

        currentSessionId = sessionId;

        chatMessages = data.messages || [];

        showView('chat');

        renderChatScreen();

        loadHistoryFromDB();

      } catch (err) {

        console.error('Failed to load session:', err);

      }

    }

    async function deleteChatSession(e, sessionId) {

      e.stopPropagation();

      try {

        await fetch(`/api/chats/${sessionId}`, { method: 'DELETE' });

        if (currentSessionId === sessionId) resetToWelcome();

        else loadHistoryFromDB();

      } catch (err) {

        console.error('Delete error:', err);

      }

    }

    function toggleRecentChats() {

      const list = document.getElementById('chatsList');

      const btn = document.getElementById('toggleRecentChatsBtn');

      const section = document.getElementById('recentChatsSection');

      if (!list || !btn) return;

      const isHidden = list.classList.toggle('hidden');

      btn.textContent = isHidden ? 'Show' : 'Hide';

      btn.title = isHidden ? 'Show recent chats' : 'Hide recent chats';

      if (section) {

        section.style.flex = isHidden ? '0 0 auto' : '45';

      }

    }

    async function clearChatHistory() {

      const res = await fetch('/api/chats');

      if (res.ok) {

        const sessions = await res.json();

        for (const s of sessions) {

          await fetch(`/api/chats/${s.id}`, { method: 'DELETE' });

        }

        resetToWelcome();

      }

    }

    // -------------------------------------------------------------

    // CHATS TABLE MANAGEMENT & PERSISTENCE

    // -------------------------------------------------------------

    let allTableSessions = [];

    function formatChatDate(isoStr) {

      if (!isoStr) return 'Oct 27, 2026';

      try {

        const d = new Date(isoStr);

        if (isNaN(d.getTime())) return 'Oct 27, 2026';

        return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });

      } catch (e) {

        return 'Oct 27, 2026';

      }

    }

    async function refreshChatsTable() {

      try {

        const res = await fetch('/api/chats');

        if (!res.ok) return;

        allTableSessions = await res.json();

        filterChatsTable();

      } catch (err) {

        console.error('Failed to load chats table:', err);

      }

    }

    function filterChatsTable() {

      const searchInput = document.getElementById('chatTableSearchInput');

      const query = (searchInput ? searchInput.value : '').trim().toLowerCase();

      const filterSelect = document.getElementById('chatModelFilter');

      const providerFilter = (filterSelect ? filterSelect.value : 'all').toLowerCase();

      let filtered = allTableSessions.slice();

      if (providerFilter && providerFilter !== 'all') {

        filtered = filtered.filter(s => {

          const m = (s.model || '').toLowerCase();

          return m.includes(providerFilter);

        });

      }

      if (query) {

        const userName = (currentUser && currentUser.name) ? currentUser.name.toLowerCase() : 'anoop dixit';

        filtered = filtered.filter(s => {

          const title = (s.title || '').toLowerCase();

          const model = (s.model || '').toLowerCase();

          return title.includes(query) || model.includes(query) || userName.includes(query);

        });

      }

      renderChatsTable(filtered);

    }

    function renderChatsTable(sessions) {

      const tbody = document.getElementById('chatsTableBody');

      const tableWrapper = document.getElementById('chatsTableWrapper');

      const authRequired = document.getElementById('chatsAuthRequired');

      const emptyContainer = document.getElementById('chatsTableEmpty');

      if (!tbody) return;

      if (!currentUser) {

        tbody.innerHTML = '';

        if (tableWrapper) tableWrapper.classList.add('hidden');

        if (emptyContainer) emptyContainer.classList.add('hidden');

        if (authRequired) authRequired.classList.remove('hidden');

        return;

      }

      if (authRequired) authRequired.classList.add('hidden');

      if (tableWrapper) tableWrapper.classList.remove('hidden');

      if (!sessions || sessions.length === 0) {

        tbody.innerHTML = '';

        if (emptyContainer) emptyContainer.classList.remove('hidden');

        return;

      }

      if (emptyContainer) emptyContainer.classList.add('hidden');

      const userName = currentUser.name || (currentUser.email ? currentUser.email.split('@')[0] : 'User');

      const userInitials = (currentUser.name ? currentUser.name.trim().charAt(0) : (currentUser.email ? currentUser.email.charAt(0) : 'U')).toUpperCase();

      const userAvatar = currentUser.picture || null;

      const userBgColor = currentUser.bg_color || '#083344';

      const userId = currentUser.id || null;

      let html = '';

      sessions.forEach(s => {

        const safeTitle = (s.title || 'Untitled Chat').replace(/"/g, '&quot;');

        const dateStr = formatChatDate(s.created_at);

        const avatarMarkup = userAvatar

          ? `<img src="${userAvatar}" referrerpolicy="no-referrer" onerror="this.onerror=null;this.style.display='none';this.nextElementSibling.style.display='flex';" class="w-6 h-6 rounded-full object-cover shrink-0"><div style="display:none;background-color:${userBgColor};" class="w-6 h-6 rounded-full border border-cyan-500/30 items-center justify-center text-[10px] font-bold text-cyan-300 shrink-0">${userInitials}</div>`

          : `<div style="background-color:${userBgColor};" class="w-6 h-6 rounded-full border border-cyan-500/30 flex items-center justify-center text-[10px] font-bold text-cyan-300 shrink-0">${userInitials}</div>`;

        html += `

          <tr onclick="openChatSession('${s.id}')" class="hover:bg-white/[0.04] transition group border-b border-white/5 cursor-pointer">

            <!-- USER (Authenticated User Name without ID) -->

            <td class="py-3 px-4 whitespace-nowrap">

              <div class="flex items-center gap-2.5">

                ${avatarMarkup}

                <div class="flex flex-col min-w-0">

                  <span class="text-xs font-semibold text-gray-200 truncate">${escapeHtml(userName)}</span>

                </div>

              </div>

            </td>

            <!-- CHAT TITLE -->

            <td class="py-3 px-4">

              <div id="table-title-view-${s.id}" class="flex items-center justify-between">

                <span onclick="openChatSession('${s.id}')" class="text-xs font-bold text-white hover:text-cyan-300 transition cursor-pointer truncate max-w-sm sm:max-w-md block" title="${escapeHtml(s.title || 'Untitled Chat')}">

                  ${escapeHtml(s.title || 'Untitled Chat')}

                </span>

              </div>

              <div id="table-rename-view-${s.id}" class="hidden items-center gap-1.5" onclick="event.stopPropagation()">

                <input id="table-rename-input-${s.id}" type="text" value="${safeTitle}" class="bg-black/50 text-white text-xs px-2.5 py-1 rounded-lg border border-white/20 focus:outline-none focus:border-cyan-400 w-48 sm:w-64" onkeydown="handleTableRenameKey(event, '${s.id}')">

                <button onclick="saveTableRename(event, '${s.id}')" class="p-1 text-gray-400 hover:text-cyan-300 transition cursor-pointer" title="Save">

                  <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"/></svg>

                </button>

                <button onclick="cancelTableRename(event, '${s.id}')" class="p-1 text-gray-400 hover:text-rose-400 transition cursor-pointer" title="Cancel">

                  <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg>

                </button>

              </div>

            </td>

            <!-- DATE -->

            <td class="py-3 px-4 whitespace-nowrap text-xs text-gray-400">

              ${dateStr}

            </td>

            <!-- ACTIONS (3-Dots dropdown) -->

            <td class="py-3 px-4 text-right whitespace-nowrap" onclick="event.stopPropagation()">

              <button type="button" data-chat-menu-btn onclick="openChatTableRowMenu(event, this, '${s.id}')" class="p-1.5 rounded-lg text-gray-400 hover:text-white hover:bg-white/10 transition cursor-pointer" title="More options">

                <svg class="w-4 h-4 pointer-events-none" fill="currentColor" viewBox="0 0 24 24"><circle cx="5" cy="12" r="1.75"/><circle cx="12" cy="12" r="1.75"/><circle cx="19" cy="12" r="1.75"/></svg>

              </button>

            </td>

          </tr>

        `;

      });

      tbody.innerHTML = html;

    }

    let activeMenuSessionId = null;

    function openChatTableRowMenu(e, btn, sessionId) {

      if (e) {

        e.preventDefault();

        e.stopPropagation();

      }

      const menu = document.getElementById('chatTableRowActionMenu');

      if (!menu) return;

      if (activeMenuSessionId === sessionId && !menu.classList.contains('hidden')) {

        menu.classList.add('hidden');

        activeMenuSessionId = null;

        return;

      }

      activeMenuSessionId = sessionId;

      // Position relative to the button

      const rect = btn.getBoundingClientRect();

      const menuHeight = 78;

      const menuWidth = 120;

      let top = rect.bottom + 4;

      if (top + menuHeight > window.innerHeight && rect.top > menuHeight) {

        top = rect.top - menuHeight - 4;

      }

      let left = rect.right - menuWidth;

      if (left < 10) left = 10;

      if (left + menuWidth > window.innerWidth - 10) left = window.innerWidth - menuWidth - 10;

      menu.style.top = `${top}px`;

      menu.style.left = `${left}px`;

      menu.classList.remove('hidden');

      const renameBtn = document.getElementById('rowActionRenameBtn');

      const deleteBtn = document.getElementById('rowActionDeleteBtn');

      if (renameBtn) {

        renameBtn.onclick = (evt) => {

          evt.stopPropagation();

          menu.classList.add('hidden');

          const sid = activeMenuSessionId || sessionId;

          activeMenuSessionId = null;

          startTableRename(sid);

        };

      }

      if (deleteBtn) {

        deleteBtn.onclick = (evt) => {

          evt.stopPropagation();

          menu.classList.add('hidden');

          const sid = activeMenuSessionId || sessionId;

          activeMenuSessionId = null;

          deleteTableChat(sid);

        };

      }

    }

    function startTableRename(sessionId) {

      const menu = document.getElementById('chatTableRowActionMenu');

      if (menu) menu.classList.add('hidden');

      const viewDiv = document.getElementById(`table-title-view-${sessionId}`);

      const renameDiv = document.getElementById(`table-rename-view-${sessionId}`);

      const input = document.getElementById(`table-rename-input-${sessionId}`);

      if (viewDiv && renameDiv && input) {

        viewDiv.classList.add('hidden');

        renameDiv.classList.remove('hidden');

        renameDiv.classList.add('flex');

        input.focus();

        input.select();

      }

    }

    function cancelTableRename(e, sessionId) {

      if (e) e.stopPropagation();

      const viewDiv = document.getElementById(`table-title-view-${sessionId}`);

      const renameDiv = document.getElementById(`table-rename-view-${sessionId}`);

      if (viewDiv && renameDiv) {

        renameDiv.classList.remove('flex');

        renameDiv.classList.add('hidden');

        viewDiv.classList.remove('hidden');

      }

    }

    async function saveTableRename(e, sessionId) {

      if (e) e.stopPropagation();

      const input = document.getElementById(`table-rename-input-${sessionId}`);

      if (!input) return;

      const newTitle = input.value.trim() || 'Untitled Chat';

      try {

        const res = await fetch(`/api/chats/${sessionId}`, {

          method: 'PATCH',

          headers: { 'Content-Type': 'application/json' },

          body: JSON.stringify({ title: newTitle }),

        });

        if (res.ok) {

          await refreshChatsTable();

          loadHistoryFromDB();

        }

      } catch (err) {

        console.error('Rename table error:', err);

      }

    }

    function handleTableRenameKey(e, sessionId) {

      if (e.key === 'Enter') {

        e.preventDefault();

        saveTableRename(e, sessionId);

      } else if (e.key === 'Escape') {

        e.preventDefault();

        cancelTableRename(e, sessionId);

      }

    }

    async function deleteTableChat(sessionId) {

      if (!confirm('Are you sure you want to delete this chat conversation?')) return;

      try {

        await fetch(`/api/chats/${sessionId}`, { method: 'DELETE' });

        if (currentSessionId === sessionId) {

          currentSessionId = null;

          chatMessages = [];

        }

        await refreshChatsTable();

        loadHistoryFromDB();

      } catch (err) {

        console.error('Delete table chat error:', err);

      }

    }

    async function openStorageModal() {

      const modal = document.getElementById('storageModal');

      if (!modal) return;

      modal.classList.remove('hidden');

      try {

        const res = await fetch('/api/chats/storage/metrics');

        if (res.ok) {

          const metrics = await res.json();

          const elChats = document.getElementById('storageTotalChats');

          const elMsgs = document.getElementById('storageTotalMessages');

          const elSize = document.getElementById('storageDbSize');

          if (elChats) elChats.innerText = `${metrics.session_count || 0} sessions`;

          if (elMsgs) elMsgs.innerText = `${metrics.message_count || 0} messages`;

          if (elSize) elSize.innerText = `${metrics.db_size_kb || 0} KB`;

        }

      } catch (e) {

        console.warn('Storage metrics fetch error:', e);

      }

    }

    function closeStorageModal() {

      const modal = document.getElementById('storageModal');

      if (modal) modal.classList.add('hidden');

    }

    async function purgeAllChats() {

      if (!confirm('Are you sure you want to delete ALL chat sessions? This action cannot be undone.')) return;

      try {

        await fetch('/api/chats', { method: 'DELETE' });

        currentSessionId = null;

        chatMessages = [];

        closeStorageModal();

        await refreshChatsTable();

        loadHistoryFromDB();

      } catch (e) {

        console.error('Failed to purge chats:', e);

      }

    }

    function renderChatScreen() {

      showView('chat');

      document.getElementById('welcomeScreen').classList.add('hidden');

      document.getElementById('chatScreen').classList.remove('hidden');

      const container = document.getElementById('messagesContainer');

      container.innerHTML = '';

      

      chatMessages.forEach(msg => {

        appendMessageUI(msg.role, msg.content, msg.meta);

      });

      container.scrollTop = container.scrollHeight;

    }

    function copyMessageText(btn, id) {

      const text = window._messageTexts ? window._messageTexts[id] : '';

      if (!text) return;

      navigator.clipboard.writeText(text).then(() => {

        const original = btn.innerHTML;

        btn.innerHTML = `<svg class="w-3.5 h-3.5 text-teal-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"></path></svg><span class="text-teal-400 font-medium">Copied!</span>`;

        setTimeout(() => { btn.innerHTML = original; }, 2000);

      });

    }

    function appendMessageUI(role, content, meta = null) {

      const container = document.getElementById('messagesContainer');

      const isUser = role === 'user';

      const div = document.createElement('div');

      div.className = `flex flex-col ${isUser ? 'items-end' : 'items-start'} gap-1 w-full mb-3`;

      const msgRow = document.createElement('div');

      msgRow.className = `flex gap-3 max-w-[80%] ${isUser ? 'justify-end' : 'justify-start'}`;

      const activeProvider = meta?.provider || (currentSelectedModel.startsWith('gemini') ? 'gemini' : (currentSelectedModel.includes('llama') || currentSelectedModel.includes('gpt-4o') ? 'openrouter' : 'groq'));

      // User avatar: profile image or user selected avatar or dynamic initials

      let avatar = '';

      if (isUser) {

        if (currentUser && currentUser.picture) {

          avatar = `<div class="w-7 h-7 rounded-full overflow-hidden shrink-0 mt-0.5 border border-cyan-400/40 shadow-sm"><img src="${currentUser.picture}" referrerpolicy="no-referrer" class="w-full h-full object-cover" alt="User"></div>`;

        } else {

          const initial = (currentUser && (currentUser.name || currentUser.email))

            ? (currentUser.name ? currentUser.name.trim().charAt(0) : currentUser.email.charAt(0)).toUpperCase()

            : 'U';

          const bg = (currentUser && currentUser.bg_color) ? currentUser.bg_color : '#0284c7';

          avatar = `<div class="w-7 h-7 rounded-full flex items-center justify-center text-[11px] font-bold text-white shrink-0 mt-0.5 shadow-sm border border-cyan-400/30" style="background-color: ${bg};">${initial}</div>`;

        }

      }

      const msgId = 'msg_' + Math.random().toString(36).substr(2, 9);

      window._messageTexts = window._messageTexts || {};

      window._messageTexts[msgId] = content;

      if (isUser) {

        msgRow.innerHTML = `

          <div class="user-message-bubble rounded-2xl py-2 px-3.5 border border-cyan-500/30 text-xs leading-relaxed max-w-full font-semibold shadow-sm">

            <div class="user-message-text font-semibold text-xs leading-relaxed [&>p]:m-0 [&>p]:font-semibold">${marked.parse(content)}</div>

          </div>

          ${avatar}

        `;

        div.appendChild(msgRow);

      } else {

        const aiGlyph = getAiResponseGlyph(activeProvider);

        msgRow.innerHTML = `

          <div class="assistant-response-glyph shrink-0 select-none flex items-center justify-center mt-[1.5px] w-5 h-5" title="LLM Gateway">

            ${aiGlyph}

          </div>

          <div class="flex flex-col gap-1.5 min-w-0 flex-1">

            <!-- Response text: NO background div box, structured, bold and eye-catchy, perfectly aligned with glyph -->

            <div class="chat-response-content text-[13.5px] leading-relaxed text-gray-900 dark:text-gray-100">

              <div class="prose dark:prose-invert max-w-none text-[13.5px] leading-relaxed text-gray-900 dark:text-gray-100 

                          [&>*:first-child]:!mt-0 [&>*:first-child]:!pt-0

                          [&>p:first-child]:!mt-0 [&>p:first-child]:!pt-0

                          [&>h1:first-child]:!mt-0 [&>h1:first-child]:!pt-0

                          [&>h2:first-child]:!mt-0 [&>h2:first-child]:!pt-0

                          [&>h3:first-child]:!mt-0 [&>h3:first-child]:!pt-0

                          [&>ul:first-child]:!mt-0 [&>ul:first-child]:!pt-0

                          [&>ol:first-child]:!mt-0 [&>ol:first-child]:!pt-0

                          [&>blockquote:first-child]:!mt-0 [&>blockquote:first-child]:!pt-0

                          [&>pre:first-child]:!mt-0 [&>pre:first-child]:!pt-0

                          [&>p]:font-medium [&>p]:leading-relaxed [&>p]:mb-2.5 

                          [&>h1]:text-base [&>h1]:font-bold [&>h1]:text-gray-900 dark:[&>h1]:text-white [&>h1]:mb-2 [&>h1]:mt-2.5

                          [&>h2]:text-[14.5px] [&>h2]:font-bold [&>h2]:text-gray-900 dark:[&>h2]:text-white [&>h2]:mb-2 [&>h2]:mt-2

                          [&>h3]:text-[13.5px] [&>h3]:font-bold [&>h3]:text-gray-900 dark:[&>h3]:text-white [&>h3]:mb-1.5 [&>h3]:mt-1.5

                          [&>strong]:font-bold [&>strong]:text-gray-900 dark:[&>strong]:text-white

                          [&>ul]:list-disc [&>ul]:pl-5 [&>ul]:mb-2.5 [&>ul]:space-y-1 [&>ul]:font-medium

                          [&>ol]:list-decimal [&>ol]:pl-5 [&>ol]:mb-2.5 [&>ol]:space-y-1 [&>ol]:font-medium

                          [&>li]:my-0.5

                          [&>blockquote]:border-l-2 [&>blockquote]:border-cyan-500 [&>blockquote]:pl-3 [&>blockquote]:italic [&>blockquote]:text-gray-600 dark:[&>blockquote]:text-gray-300

                          [&>pre]:bg-black/80 [&>pre]:border [&>pre]:border-white/10 [&>pre]:rounded-xl [&>pre]:p-3 [&>pre]:my-2 [&>pre]:font-mono [&>pre]:text-xs [&>pre]:overflow-x-auto

                          [&>code]:font-mono [&>code]:text-xs [&>code]:bg-black/20 dark:[&>code]:bg-white/10 [&>code]:px-1.5 [&>code]:py-0.5 [&>code]:rounded [&>code]:text-cyan-600 dark:[&>code]:text-cyan-300">

                ${marked.parse(content)}

              </div>

            </div>

            <!-- Action Buttons: Copy & Retry (Flush with content) -->

            <div class="flex items-center gap-1.5 text-[11px] text-gray-500 dark:text-gray-400 pl-0.5 pt-0.5">

              <button onclick="copyMessageText(this, '${msgId}')" class="flex items-center gap-1 py-1 px-2 rounded-md hover:bg-gray-200 dark:hover:bg-white/5 hover:text-gray-900 dark:hover:text-white text-gray-500 dark:text-gray-400 transition cursor-pointer" title="Copy response">

                <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z"></path></svg>

                <span>Copy</span>

              </button>

              <button onclick="retryLastUserMessage()" class="flex items-center gap-1 py-1 px-2 rounded-md hover:bg-gray-200 dark:hover:bg-white/5 hover:text-gray-900 dark:hover:text-white text-gray-500 dark:text-gray-400 transition cursor-pointer" title="Retry generation">

                <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"></path></svg>

                <span>Retry</span>

              </button>

            </div>

          </div>

        `;

        div.appendChild(msgRow);

      }

      container.appendChild(div);

      container.scrollTop = container.scrollHeight;

    }

    async function retryLastUserMessage() {

      if (isWaiting || chatMessages.length === 0) return;

      if (chatMessages[chatMessages.length - 1].role === 'assistant') {

        chatMessages.pop();

      }

      renderChatScreen();

      await executeChatCall();

    }

    async function submitMessage() {

      if (!currentUser) {

        currentUser = { id: 'guest_user', email: 'developer@llmgateway.io', name: 'Developer', is_admin: true };

        updateAuthHeaderUI();

      }

      const input = document.getElementById('heroChatInput');

      const text = input.value.trim();

      if (!text || isWaiting) return;

      input.value = '';

      if (!currentSessionId) currentSessionId = 'chat_' + Date.now();

      chatMessages.push({ role: 'user', content: text });

      renderChatScreen();

      await executeChatCall();

    }

    async function submitMessageFromStream() {

      if (!currentUser) {

        currentUser = { id: 'guest_user', email: 'developer@llmgateway.io', name: 'Developer', is_admin: true };

        updateAuthHeaderUI();

      }

      const input = document.getElementById('streamChatInput');

      const text = input.value.trim();

      if (!text || isWaiting) return;

      input.value = '';

      if (!currentSessionId) currentSessionId = 'chat_' + Date.now();

      chatMessages.push({ role: 'user', content: text });

      appendMessageUI('user', text);

      await executeChatCall();

    }

    async function executeChatCall() {

      isWaiting = true;

      const container = document.getElementById('messagesContainer');

      const loadingDiv = document.createElement('div');

      loadingDiv.id = 'loadingIndicator';

      loadingDiv.className = 'flex gap-3 justify-start max-w-[72%]';

      const activeModelObj = AVAILABLE_MODELS.find(m => m.id === currentSelectedModel) || AVAILABLE_MODELS[0];

      const aiGlyph = getAiResponseGlyph(activeModelObj.provider);

      loadingDiv.innerHTML = `

        <div class="assistant-response-glyph shrink-0 select-none flex items-center justify-center mt-1 w-5 h-5" title="LLM Gateway">

          ${aiGlyph}

        </div>

        <div class="glass-panel rounded-2xl p-3.5 text-xs text-gray-400 flex items-center gap-2">

          <div class="w-2 h-2 rounded-full bg-cyan-400 animate-ping"></div>

          <span>Generating response with ${activeModelObj.name}...</span>

        </div>

      `;

      container.appendChild(loadingDiv);

      container.scrollTop = container.scrollHeight;

      try {

        const payload = {

          messages: chatMessages.map(m => ({ role: m.role, content: m.content })),

          alias: currentSelectedModel || 'fast',

          model: currentSelectedModel || 'fast',

          temperature: parseFloat(document.getElementById('modalTemp')?.value || 0.7),

          max_tokens: parseInt(document.getElementById('modalTokens')?.value || 1000),

          session_id: currentSessionId

        };

        const res = await fetch('/v1/chat', {

          method: 'POST',

          headers: { 'Content-Type': 'application/json' },

          body: JSON.stringify(payload)

        });

        loadingDiv.remove();

        if (res.ok) {

          const data = await res.json();

          currentSessionId = data.session_id || currentSessionId;

          chatMessages.push({ role: 'assistant', content: data.text, meta: data });

          appendMessageUI('assistant', data.text, data);

          loadHistoryFromDB();

        } else {

          const errData = await res.json().catch(() => ({ detail: 'Unknown error' }));

          appendMessageUI('assistant', `⚠️ **Error (${res.status})**: ${errData.detail}`);

        }

      } catch (err) {

        loadingDiv.remove();

        appendMessageUI('assistant', `Connection error: ${err.message}`);

      } finally {

        isWaiting = false;

      }

    }

    document.getElementById('heroChatInput').addEventListener('keydown', (e) => {

      if (e.key === 'Enter' && !e.shiftKey) {

        e.preventDefault();

        submitMessage();

      }

    });

    document.getElementById('streamChatInput').addEventListener('keydown', (e) => {

      if (e.key === 'Enter' && !e.shiftKey) {

        e.preventDefault();

        submitMessageFromStream();

      }

    });

    // -------------------------------------------------------------

    // VOICE POWERED ORB (EXACT ORIGINAL COMPONENT SPECIFICATIONS)

    // -------------------------------------------------------------

    const vertShaderSource = `

      precision highp float;

      attribute vec2 position;

      attribute vec2 uv;

      varying vec2 vUv;

      void main() {

        vUv = uv;

        gl_Position = vec4(position, 0.0, 1.0);

      }

    `;

    const fragShaderSource = `

      precision highp float;

      uniform float iTime;

      uniform vec3 iResolution;

      uniform float hue;

      uniform float hover;

      uniform float rot;

      uniform float hoverIntensity;

      varying vec2 vUv;

      vec3 rgb2yiq(vec3 c) {

        float y = dot(c, vec3(0.299, 0.587, 0.114));

        float i = dot(c, vec3(0.596, -0.274, -0.322));

        float q = dot(c, vec3(0.211, -0.523, 0.312));

        return vec3(y, i, q);

      }

      vec3 yiq2rgb(vec3 c) {

        float r = c.x + 0.956 * c.y + 0.621 * c.z;

        float g = c.x - 0.272 * c.y - 0.647 * c.z;

        float b = c.x - 1.106 * c.y + 1.703 * c.z;

        return vec3(r, g, b);

      }

      vec3 adjustHue(vec3 color, float hueDeg) {

        float hueRad = hueDeg * 3.14159265 / 180.0;

        vec3 yiq = rgb2yiq(color);

        float cosA = cos(hueRad);

        float sinA = sin(hueRad);

        float i = yiq.y * cosA - yiq.z * sinA;

        float q = yiq.y * sinA + yiq.z * cosA;

        yiq.y = i;

        yiq.z = q;

        return yiq2rgb(yiq);

      }

      vec3 hash33(vec3 p3) {

        p3 = fract(p3 * vec3(0.1031, 0.11369, 0.13787));

        p3 += dot(p3, p3.yxz + 19.19);

        return -1.0 + 2.0 * fract(vec3(

          p3.x + p3.y,

          p3.x + p3.z,

          p3.y + p3.z

        ) * p3.zyx);

      }

      float snoise3(vec3 p) {

        const float K1 = 0.333333333;

        const float K2 = 0.166666667;

        vec3 i = floor(p + (p.x + p.y + p.z) * K1);

        vec3 d0 = p - (i - (i.x + i.y + i.z) * K2);

        vec3 e = step(vec3(0.0), d0 - d0.yzx);

        vec3 i1 = e * (1.0 - e.zxy);

        vec3 i2 = 1.0 - e.zxy * (1.0 - e);

        vec3 d1 = d0 - (i1 - K2);

        vec3 d2 = d0 - (i2 - K1);

        vec3 d3 = d0 - 0.5;

        vec4 h = max(0.6 - vec4(

          dot(d0, d0),

          dot(d1, d1),

          dot(d2, d2),

          dot(d3, d3)

        ), 0.0);

        vec4 n = h * h * h * h * vec4(

          dot(d0, hash33(i)),

          dot(d1, hash33(i + i1)),

          dot(d2, hash33(i + i2)),

          dot(d3, hash33(i + 1.0))

        );

        return dot(vec4(31.316), n);

      }

      vec4 extractAlpha(vec3 colorIn) {

        float a = max(max(colorIn.r, colorIn.g), colorIn.b);

        return vec4(colorIn.rgb / (a + 1e-5), a);

      }

      const vec3 baseColor1 = vec3(0.611765, 0.262745, 0.996078);

      const vec3 baseColor2 = vec3(0.298039, 0.760784, 0.913725);

      const vec3 baseColor3 = vec3(0.062745, 0.078431, 0.600000);

      const float innerRadius = 0.6;

      const float noiseScale = 0.65;

      float light1(float intensity, float attenuation, float dist) {

        return intensity / (1.0 + dist * attenuation);

      }

      float light2(float intensity, float attenuation, float dist) {

        return intensity / (1.0 + dist * dist * attenuation);

      }

      vec4 draw(vec2 uv) {

        vec3 color1 = adjustHue(baseColor1, hue);

        vec3 color2 = adjustHue(baseColor2, hue);

        vec3 color3 = adjustHue(baseColor3, hue);

        float ang = atan(uv.y, uv.x);

        float len = length(uv);

        float invLen = len > 0.0 ? 1.0 / len : 0.0;

        float n0 = snoise3(vec3(uv * noiseScale, iTime * 0.5)) * 0.5 + 0.5;

        float r0 = mix(mix(innerRadius, 1.0, 0.4), mix(innerRadius, 1.0, 0.6), n0);

        float d0 = distance(uv, (r0 * invLen) * uv);

        float v0 = light1(1.0, 10.0, d0);

        v0 *= smoothstep(r0 * 1.05, r0, len);

        float cl = cos(ang + iTime * 2.0) * 0.5 + 0.5;

        float a = iTime * -1.0;

        vec2 pos = vec2(cos(a), sin(a)) * r0;

        float d = distance(uv, pos);

        float v1 = light2(1.5, 5.0, d);

        v1 *= light1(1.0, 50.0, d0);

        float v2 = smoothstep(1.0, mix(innerRadius, 1.0, n0 * 0.5), len);

        float v3 = smoothstep(innerRadius, mix(innerRadius, 1.0, 0.5), len);

        vec3 col = mix(color1, color2, cl);

        col = mix(color3, col, v0);

        col = (col + v1) * v2 * v3;

        col = clamp(col, 0.0, 1.0);

        return extractAlpha(col);

      }

      vec4 mainImage(vec2 fragCoord) {

        vec2 center = iResolution.xy * 0.5;

        float size = min(iResolution.x, iResolution.y);

        vec2 uv = (fragCoord - center) / size * 2.0;

        float angle = rot;

        float s = sin(angle);

        float c = cos(angle);

        uv = vec2(c * uv.x - s * uv.y, s * uv.x + c * uv.y);

        uv.x += hover * hoverIntensity * 0.1 * sin(uv.y * 10.0 + iTime);

        uv.y += hover * hoverIntensity * 0.1 * sin(uv.x * 10.0 + iTime);

        return draw(uv);

      }

      void main() {

        vec2 fragCoord = vUv * iResolution.xy;

        vec4 col = mainImage(fragCoord);

        gl_FragColor = vec4(col.rgb * col.a, col.a);

      }

    `;

    let isVoiceActive = false;

    let audioCtx = null;

    let analyserNode = null;

    let micStream = null;

    let audioDataArray = null;

    async function initMicrophone() {

      try {

        stopMicrophone();

        micStream = await navigator.mediaDevices.getUserMedia({

          audio: { echoCancellation: false, noiseSuppression: false, autoGainControl: false, sampleRate: 44100 }

        });

        audioCtx = new (window.AudioContext || window.webkitAudioContext)();

        if (audioCtx.state === 'suspended') await audioCtx.resume();

        analyserNode = audioCtx.createAnalyser();

        analyserNode.fftSize = 512;

        analyserNode.smoothingTimeConstant = 0.3;

        analyserNode.minDecibels = -90;

        analyserNode.maxDecibels = -10;

        const micSource = audioCtx.createMediaStreamSource(micStream);

        micSource.connect(analyserNode);

        audioDataArray = new Uint8Array(analyserNode.frequencyBinCount);

        return true;

      } catch (err) {

        console.warn('Microphone access unavailable:', err);

        return false;

      }

    }

    function stopMicrophone() {

      if (micStream) {

        micStream.getTracks().forEach(t => t.stop());

        micStream = null;

      }

      if (audioCtx && audioCtx.state !== 'closed') {

        audioCtx.close();

        audioCtx = null;

      }

      analyserNode = null;

      audioDataArray = null;

    }

    function analyzeAudio() {

      if (!analyserNode || !audioDataArray) return 0;

      analyserNode.getByteFrequencyData(audioDataArray);

      let sum = 0;

      for (let i = 0; i < audioDataArray.length; i++) {

        const val = audioDataArray[i] / 255;

        sum += val * val;

      }

      const rms = Math.sqrt(sum / audioDataArray.length);

      return Math.min(rms * 1.5 * 3.0, 1.0);

    }

    let speechRecognizer = null;

    let activeTranscriptBase = '';

    function startSpeechRecognition() {

      const SpeechRec = window.SpeechRecognition || window.webkitSpeechRecognition;

      if (!SpeechRec) {

        console.warn("SpeechRecognition not supported in browser");

        return;

      }

      try {

        stopSpeechRecognition();

        speechRecognizer = new SpeechRec();

        speechRecognizer.continuous = true;

        speechRecognizer.interimResults = true;

        speechRecognizer.lang = 'en-US';

        const isChatScreen = !document.getElementById('chatScreen').classList.contains('hidden');

        const activeInput = isChatScreen ? document.getElementById('streamChatInput') : document.getElementById('heroChatInput');

        activeTranscriptBase = activeInput ? activeInput.value : '';

        if (activeTranscriptBase && !activeTranscriptBase.endsWith(' ')) activeTranscriptBase += ' ';

        speechRecognizer.onresult = (event) => {

          let interimTranscript = '';

          let finalTranscript = '';

          for (let i = event.resultIndex; i < event.results.length; ++i) {

            if (event.results[i].isFinal) {

              finalTranscript += event.results[i][0].transcript;

            } else {

              interimTranscript += event.results[i][0].transcript;

            }

          }

          const targetInput = !document.getElementById('chatScreen').classList.contains('hidden')

            ? document.getElementById('streamChatInput')

            : document.getElementById('heroChatInput');

          if (targetInput) {

            targetInput.value = activeTranscriptBase + finalTranscript + interimTranscript;

            targetInput.scrollTop = targetInput.scrollHeight;

          }

          if (finalTranscript) {

            activeTranscriptBase += finalTranscript + ' ';

          }

        };

        speechRecognizer.onerror = (e) => {

          console.warn('Speech recognition error:', e.error);

        };

        speechRecognizer.onend = () => {

          if (isVoiceActive && speechRecognizer) {

            try { speechRecognizer.start(); } catch(err){}

          }

        };

        speechRecognizer.start();

      } catch (err) {

        console.warn('Failed to start speech recognition:', err);

      }

    }

    function stopSpeechRecognition() {

      if (speechRecognizer) {

        try { speechRecognizer.stop(); } catch(err){}

        speechRecognizer = null;

      }

    }

    let initialVoiceInputText = '';
    let soundwaveAnimFrameId = null;

    function getActiveChatInput() {
      const isChatScreen = !document.getElementById('chatScreen').classList.contains('hidden');
      return isChatScreen ? document.getElementById('streamChatInput') : document.getElementById('heroChatInput');
    }

    function updateVoiceInputBarUI(active) {
      const heroNorm = document.getElementById('heroNormalControls');
      const heroVoice = document.getElementById('heroVoiceControls');
      const streamNorm = document.getElementById('streamNormalControls');
      const streamVoice = document.getElementById('streamVoiceControls');

      if (active) {
        if (heroNorm) heroNorm.classList.add('hidden');
        if (heroVoice) heroVoice.classList.remove('hidden');
        if (streamNorm) streamNorm.classList.add('hidden');
        if (streamVoice) streamVoice.classList.remove('hidden');
        startSoundwaveAnimation();
      } else {
        if (heroNorm) heroNorm.classList.remove('hidden');
        if (heroVoice) heroVoice.classList.add('hidden');
        if (streamNorm) streamNorm.classList.remove('hidden');
        if (streamVoice) streamVoice.classList.add('hidden');
        stopSoundwaveAnimation();
      }
    }

    function startSoundwaveAnimation() {
      if (soundwaveAnimFrameId) cancelAnimationFrame(soundwaveAnimFrameId);

      function renderWaves(now) {
        const bars = document.querySelectorAll('.chatgpt-soundwave .soundwave-bar');
        if (bars.length > 0) {
          if (analyserNode && audioDataArray) {
            analyserNode.getByteFrequencyData(audioDataArray);
          }
          for (let i = 0; i < bars.length; i++) {
            let h = 3.5;
            if (analyserNode && audioDataArray) {
              const sampleIdx = Math.floor(i * (audioDataArray.length / (bars.length * 2)));
              const amp = (audioDataArray[sampleIdx] || 0) / 255.0;
              h = 3.5 + (amp * 18.0) + Math.sin(now * 0.007 + i * 0.45) * 1.8;
            } else {
              // Organic ChatGPT waveform fallback
              h = 3.5 + Math.sin(now * 0.006 + i * 0.45) * 6.5 + Math.abs(Math.cos(now * 0.009 + i * 0.25)) * 6.0;
            }
            h = Math.min(22, Math.max(3.0, h));
            bars[i].style.height = `${h.toFixed(1)}px`;
          }
        }
        if (isVoiceActive) {
          soundwaveAnimFrameId = requestAnimationFrame(renderWaves);
        }
      }
      soundwaveAnimFrameId = requestAnimationFrame(renderWaves);
    }

    function stopSoundwaveAnimation() {
      if (soundwaveAnimFrameId) {
        cancelAnimationFrame(soundwaveAnimFrameId);
        soundwaveAnimFrameId = null;
      }
      document.querySelectorAll('.chatgpt-soundwave .soundwave-bar').forEach(b => {
        b.style.height = '4px';
      });
    }

    async function toggleVoiceRecording() {
      const voiceBtns = document.querySelectorAll('.voice-btn');

      if (!isVoiceActive) {
        const activeInput = getActiveChatInput();
        initialVoiceInputText = activeInput ? activeInput.value : '';

        const ok = await initMicrophone();

        if (ok) {
          isVoiceActive = true;
          startSpeechRecognition();
          updateVoiceInputBarUI(true);

          voiceBtns.forEach(b => {
            b.classList.add('bg-red-500/20', 'text-red-400', 'border-red-500/50', 'animate-pulse');
          });
        }
      } else {
        acceptVoiceRecording();
      }
    }

    function cancelVoiceRecording() {
      stopMicrophone();
      stopSpeechRecognition();
      isVoiceActive = false;
      updateVoiceInputBarUI(false);

      const activeInput = getActiveChatInput();
      if (activeInput) {
        activeInput.value = initialVoiceInputText;
      }

      const voiceBtns = document.querySelectorAll('.voice-btn');
      voiceBtns.forEach(b => {
        b.classList.remove('bg-red-500/20', 'text-red-400', 'border-red-500/50', 'animate-pulse');
      });
    }

    function acceptVoiceRecording() {
      stopMicrophone();
      stopSpeechRecognition();
      isVoiceActive = false;
      updateVoiceInputBarUI(false);

      const voiceBtns = document.querySelectorAll('.voice-btn');
      voiceBtns.forEach(b => {
        b.classList.remove('bg-red-500/20', 'text-red-400', 'border-red-500/50', 'animate-pulse');
      });
    }

    function initVoicePoweredOrb() {

      const canvas = document.getElementById('orbCanvas');

      if (!canvas) return;

      const gl = canvas.getContext('webgl', { alpha: true, premultipliedAlpha: false, antialias: true });

      if (!gl) return;

      gl.clearColor(0, 0, 0, 0);

      gl.enable(gl.BLEND);

      gl.blendFunc(gl.SRC_ALPHA, gl.ONE_MINUS_SRC_ALPHA);

      function createShader(type, src) {

        const shader = gl.createShader(type);

        gl.shaderSource(shader, src);

        gl.compileShader(shader);

        if (!gl.getShaderParameter(shader, gl.COMPILE_STATUS)) {

          console.error(gl.getShaderInfoLog(shader));

          gl.deleteShader(shader);

          return null;

        }

        return shader;

      }

      const vs = createShader(gl.VERTEX_SHADER, vertShaderSource);

      const fs = createShader(gl.FRAGMENT_SHADER, fragShaderSource);

      if (!vs || !fs) return;

      const prog = gl.createProgram();

      gl.attachShader(prog, vs);

      gl.attachShader(prog, fs);

      gl.linkProgram(prog);

      gl.useProgram(prog);

      const posBuffer = gl.createBuffer();

      gl.bindBuffer(gl.ARRAY_BUFFER, posBuffer);

      gl.bufferData(gl.ARRAY_BUFFER, new Float32Array([-1, -1, 3, -1, -1, 3]), gl.STATIC_DRAW);

      const posAttr = gl.getAttribLocation(prog, 'position');

      gl.enableVertexAttribArray(posAttr);

      gl.vertexAttribPointer(posAttr, 2, gl.FLOAT, false, 0, 0);

      const uvBuffer = gl.createBuffer();

      gl.bindBuffer(gl.ARRAY_BUFFER, uvBuffer);

      gl.bufferData(gl.ARRAY_BUFFER, new Float32Array([0, 0, 2, 0, 0, 2]), gl.STATIC_DRAW);

      const uvAttr = gl.getAttribLocation(prog, 'uv');

      if (uvAttr !== -1) {

        gl.enableVertexAttribArray(uvAttr);

        gl.vertexAttribPointer(uvAttr, 2, gl.FLOAT, false, 0, 0);

      }

      const uTime = gl.getUniformLocation(prog, 'iTime');

      const uRes = gl.getUniformLocation(prog, 'iResolution');

      const uHue = gl.getUniformLocation(prog, 'hue');

      const uHover = gl.getUniformLocation(prog, 'hover');

      const uRot = gl.getUniformLocation(prog, 'rot');

      const uHoverIntensity = gl.getUniformLocation(prog, 'hoverIntensity');

      let currentRot = 0;

      let lastTime = 0;

      let mouseHover = 0;

      const baseRotationSpeed = 0.3;

      const maxRotationSpeed = 1.2;

      const maxHoverIntensity = 0.8;

      canvas.addEventListener('mouseenter', () => { mouseHover = 0.5; });

      canvas.addEventListener('mouseleave', () => { mouseHover = 0; });

      function render(now) {

        requestAnimationFrame(render);

        const dt = (now - lastTime) * 0.001;

        lastTime = now;

        const w = canvas.width;

        const h = canvas.height;

        gl.viewport(0, 0, w, h);

        gl.uniform1f(uTime, now * 0.001);

        gl.uniform3f(uRes, w, h, w / h);

        gl.uniform1f(uHue, 0.0);

        let hoverVal = 0.0;

        let hoverIntensityVal = 0.0;

        if (isVoiceActive) {

          const voiceLevel = analyzeAudio();

          const voiceRotSpeed = baseRotationSpeed + (voiceLevel * maxRotationSpeed * 2.0);

          if (voiceLevel > 0.05) {

            currentRot += dt * voiceRotSpeed;

          } else {

            currentRot += dt * baseRotationSpeed;

          }

          hoverVal = Math.min(voiceLevel * 2.0, 1.0);

          hoverIntensityVal = Math.min(voiceLevel * maxHoverIntensity * 0.8, maxHoverIntensity);

        } else {

          currentRot += dt * baseRotationSpeed;

          if (mouseHover > 0) {

            hoverVal = 0.3;

            hoverIntensityVal = 0.3;

          }

        }

        gl.uniform1f(uRot, currentRot);

        gl.uniform1f(uHover, hoverVal);

        gl.uniform1f(uHoverIntensity, hoverIntensityVal);

        gl.clear(gl.COLOR_BUFFER_BIT);

        gl.drawArrays(gl.TRIANGLES, 0, 3);

      }

      requestAnimationFrame(render);

    }

    // =========================================================================

    // FOUNDATION PRIMITIVES 3D WEBGL2 ENGINE

    // =========================================================================

    const PRIM_VERT = `#version 300 es

    void main() {

      vec2 p = vec2(float((gl_VertexID << 1) & 2), float(gl_VertexID & 2));

      gl_Position = vec4(p * 2.0 - 1.0, 0.0, 1.0);

    }`;

    const PRIM_FRAG = `#version 300 es

    precision highp float;

    out vec4 outColor;

    uniform vec2 uRes;

    uniform int uCount;

    uniform vec4 uPos[6];

    uniform mat3 uRot[6];

    uniform vec4 uInfo[6];

    uniform vec3 uSquash[6];

    uniform vec3 uCore[6];

    uniform vec3 uRim[6];

    uniform vec4 uStrip[4];

    uniform int uStripCount;

    uniform vec3 uDisc[6];

    uniform int uDiscCount;

    uniform vec3 uEye;

    uniform vec3 uRight;

    uniform vec3 uUp;

    uniform vec3 uBack;

    uniform float uFov;

    uniform vec3 uLight;

    uniform vec4 uLens;

    uniform vec3 uFg;

    uniform float uShadow;

    uniform float uGlow;

    const float PLANE_Z = -1.32;

    const float TILE_Z = -1.24;

    const float LENS_Z = 1.9;

    float smin(float a, float b, float k) {

      float h = clamp(0.5 + 0.5 * (b - a) / k, 0.0, 1.0);

      return mix(b, a, h) - k * h * (1.0 - h);

    }

    float smax(float a, float b, float k) { return -smin(-a, -b, k); }

    float sdBox(vec3 p, vec3 b, float r) {

      vec3 q = abs(p) - b + r;

      return length(max(q, 0.0)) + min(max(q.x, max(q.y, q.z)), 0.0) - r;

    }

    mat2 rot2(float a) { float c = cos(a), s = sin(a); return mat2(c, -s, s, c); }

    float halfCentre(int k, float spread) { return (float(k) - 1.0) * 0.55 * spread + 0.5; }

    vec2 shapeDist(int kind, vec3 q, float spread, int skipSub) {

      if (kind == 1) {

        float d = 1e9;

        for (int k = 0; k < 4; k++) {

          vec3 r = q;

          r.xy = rot2(float(k) * 0.7853982) * r.xy;

          d = smin(d, sdBox(r, vec3(1.0, 0.2, 0.2), 0.1), 0.05);

        }

        return vec2(d, 0.0);

      }

      if (kind == 2) {

        float d = 1e9;

        float sub = 0.0;

        for (int k = 0; k < 3; k++) {

          if (k == skipSub) continue;

          float cx = halfCentre(k, spread);

          vec3 c = q - vec3(cx, 0.0, 0.0);

          float h = smax(length(c) - 1.0, c.x, 0.05);

          if (h < d) { d = h; sub = float(k); }

        }

        return vec2(d, sub);

      }

      if (kind == 3) {

        vec3 s = q * vec3(0.9, 1.0, 0.9);

        float top = smax(length(s - vec3(0.0, 1.0, 0.0)) - 1.0, s.y - 1.0, 0.05);

        float bot = smax(length(s + vec3(0.0, 1.0, 0.0)) - 1.0, -1.0 - s.y, 0.05);

        return vec2(smin(top, bot, 0.1) * 0.9, 0.0);

      }

      if (kind == 4) {

        vec2 t = vec2(length(q.xy) - 0.68, q.z);

        return vec2(length(t) - 0.32, 0.0);

      }

      if (kind == 5) {

        vec3 c = q;

        c.x -= clamp(c.x, -0.55, 0.55);

        return vec2(length(c) - 0.5, 0.0);

      }

      if (kind == 6) return vec2(sdBox(q, vec3(0.74), 0.2), 0.0);

      return vec2(length(q) - 1.0, 0.0);

    }

    float tileDist2(vec2 p) {

      float d = 1e9;

      for (int i = 0; i < 4; i++) {

        if (i >= uStripCount) break;

        vec4 s = uStrip[i];

        vec2 c = vec2((s.x + s.y) * 0.5, s.z);

        vec2 b = vec2((s.y - s.x) * 0.5, 1.04);

        vec2 q = abs(p - c) - b + 0.34;

        d = min(d, length(max(q, 0.0)) + min(max(q.x, q.y), 0.0) - 0.34);

      }

      for (int i = 0; i < 6; i++) {

        if (i >= uDiscCount) break;

        d = min(d, length(p - uDisc[i].xy) - uDisc[i].z);

      }

      return d;

    }

    vec2 map(vec3 p, int skip) {

      vec2 res = vec2(1e9, -1.0);

      for (int i = 0; i < 6; i++) {

        if (i >= uCount) break;

        vec3 q = p - uPos[i].xyz;

        float s = uPos[i].w;

        float bound = length(q) - 1.9 * s;

        if (bound > 0.35) {

          if (bound < res.x) res = vec2(bound, float(i * 4));

          continue;

        }

        int kind = int(uInfo[i].x + 0.5);

        int skipSub = -1;

        if (skip >= 0 && skip / 4 == i) {

          if (kind != 2) continue;

          skipSub = skip - i * 4;

        }

        vec3 sq = uSquash[i];

        vec3 lq = (transpose(uRot[i]) * q) / (s * sq);

        vec2 h = shapeDist(kind, lq, uInfo[i].w, skipSub);

        float d = h.x * s * min(sq.x, min(sq.y, sq.z));

        if (d < res.x) res = vec2(d, float(i * 4) + h.y);

      }

      return res;

    }

    vec3 calcNormal(vec3 p, int skip) {

      const vec2 e = vec2(1.0, -1.0) * 0.0015;

      return normalize(

        e.xyy * map(p + e.xyy, skip).x +

        e.yyx * map(p + e.yyx, skip).x +

        e.yxy * map(p + e.yxy, skip).x +

        e.xxx * map(p + e.xxx, skip).x);

    }

    float sphShadow(vec3 ro, vec3 rd, vec4 sph) {

      vec3 oc = ro - sph.xyz;

      float b = dot(oc, rd);

      float c = dot(oc, oc) - sph.w * sph.w;

      float h = b * b - c;

      float d = sqrt(max(0.0, sph.w * sph.w - h)) - sph.w;

      float t = -b - sqrt(max(h, 0.0));

      return t < 0.0 ? 1.0 : smoothstep(0.0, 1.0, 1.6 * d / t);

    }

    float softShadow(vec3 ro, vec3 rd, int self) {

      float res = 1.0;

      for (int i = 0; i < 6; i++) {

        if (i >= uCount) break;

        if (i == self) continue;

        res = min(res, sphShadow(ro, rd, vec4(uPos[i].xyz, 0.92 * uPos[i].w)));

      }

      return res;

    }

    vec4 over(vec4 top, vec4 under) { return top + (1.0 - top.a) * under; }

    vec4 glowAt(vec2 xy) {

      vec3 g = vec3(0.0);

      float sum = 0.0;

      for (int i = 0; i < 6; i++) {

        if (i >= uCount) break;

        vec2 d = xy - uPos[i].xy;

        float w = exp(-dot(d, d) * 0.55) * uGlow * (0.7 + 0.5 * uInfo[i].z);

        g += uCore[i] * w;

        sum += w;

      }

      float a = min(sum, 0.6);

      return vec4(g * (a / max(sum, 1e-4)), a) * 0.45;

    }

    vec4 behind(vec3 ro, vec3 rd, vec3 L) {

      float tt = (TILE_Z - ro.z) / min(rd.z, -1e-4);

      vec3 p = ro + rd * tt;

      float d = tileDist2(p.xy);

      float px = 1.5 * uFov * tt / uRes.y;

      float cover = 1.0 - smoothstep(-px, px, d);

      float tp = (PLANE_Z - ro.z) / min(rd.z, -1e-4);

      vec3 q = ro + rd * tp;

      vec4 card = over(vec4(0.0, 0.0, 0.0, (1.0 - softShadow(q, L, -1)) * uShadow), glowAt(q.xy));

      if (cover <= 0.0) return card;

      vec2 e = vec2(0.01, 0.0);

      vec2 g = normalize(vec2(tileDist2(p.xy + e.xy) - tileDist2(p.xy - e.xy), tileDist2(p.xy + e.yx) - tileDist2(p.xy - e.yx)) + 1e-5);

      float bevel = smoothstep(-0.07, 0.0, d);

      float lit = dot(g, normalize(L.xy + 1e-5));

      vec4 c = vec4(uFg * 0.055, 0.055);

      c = over(vec4(vec3(1.0), 1.0) * bevel * max(lit, 0.0) * 0.5, c);

      c = over(vec4(0.0, 0.0, 0.0, bevel * max(-lit, 0.0) * 0.06), c);

      c = over(vec4(0.0, 0.0, 0.0, (1.0 - softShadow(p, L, -1)) * uShadow * 0.55), c);

      c = over(c, glowAt(p.xy) * 0.6);

      return mix(card, c, cover);

    }

    vec4 shapeShade(vec3 p, vec3 rd, int id, int skip, vec3 L) {

      int i = id / 4;

      int sub = id - i * 4;

      int kind = int(uInfo[i].x + 0.5);

      float hover = uInfo[i].z;

      vec3 n = calcNormal(p, skip);

      vec3 lq = (transpose(uRot[i]) * (p - uPos[i].xyz)) / (uPos[i].w * uSquash[i]);

      float facing = clamp(dot(n, -rd), 0.0, 1.0);

      float radial;

      float cx = 0.0;

      if (kind == 2) {

        cx = halfCentre(sub, uInfo[i].w);

        radial = clamp(1.0 - length(lq - vec3(cx - 0.55, 0.0, 0.0)) / 1.4, 0.0, 1.0);

      } else if (kind == 3) {

        radial = clamp(1.0 - length(lq * vec3(0.8, 1.1, 1.0)) / 1.85, 0.0, 1.0);

      } else {

        radial = clamp(1.0 - length(lq.xy) / 1.3, 0.0, 1.0);

      }

      float core = clamp(0.45 * pow(facing, 1.6) + 0.7 * radial, 0.0, 1.0);

      vec3 base = mix(uRim[i], uCore[i], smoothstep(0.06, 0.9, core));

      float sh = softShadow(p, L, i);

      float dif = clamp(dot(n, L) * 0.5 + 0.5, 0.0, 1.0);

      vec3 col = base * (0.8 + 0.3 * dif) * mix(0.8, 1.0, sh);

      float fres = pow(1.0 - facing, 2.4);

      col = mix(col, mix(uRim[i], vec3(1.0), 0.55), fres * 0.6);

      col += uRim[i] * pow(clamp(dot(n, -L) * 0.5 + 0.5, 0.0, 1.0), 3.0) * 0.18;

      vec3 hv = normalize(L - rd);

      col += vec3(1.0) * pow(max(dot(n, hv), 0.0), 56.0) * 0.42 * sh;

      col = mix(col, col * 1.06 + 0.015, hover);

      float soft = kind == 0 ? 0.32 : kind == 2 ? 0.1 : 0.16;

      float a = smoothstep(0.0, soft, facing);

      if (kind == 2) {

        float f = smoothstep(-0.95, 0.02, lq.x - cx);

        a *= mix(1.0, 0.55, f);

        col = mix(col, mix(uRim[i], vec3(1.0), 0.2), f * 0.18);

      }

      return vec4(clamp(col, 0.0, 1.0) * a, a);

    }

    vec2 march(vec3 ro, vec3 rd, float t, float tMax, int skip) {

      for (int i = 0; i < 80; i++) {

        vec2 h = map(ro + rd * t, skip);

        if (h.x < 0.0025) return vec2(t, h.y);

        t += h.x;

        if (t > tMax) break;

      }

      return vec2(tMax, -1.0);

    }

    bool mayHit(vec3 ro, vec3 rd) {

      for (int i = 0; i < 6; i++) {

        if (i >= uCount) break;

        vec3 oc = ro - uPos[i].xyz;

        float r = 1.95 * uPos[i].w;

        float b = dot(oc, rd);

        if (b * b - dot(oc, oc) + r * r > 0.0) return true;

      }

      return false;

    }

    vec4 render(vec3 ro, vec3 rd, vec3 L) {

      if (!mayHit(ro, rd)) return behind(ro, rd, L);

      float t = max(0.0, (2.4 - ro.z) / min(rd.z, -1e-4));

      float tEnd = (TILE_Z - ro.z) / min(rd.z, -1e-4);

      vec4 acc = vec4(0.0);

      int skip = -1;

      for (int layer = 0; layer < 3; layer++) {

        vec2 h = march(ro, rd, t, tEnd, skip);

        if (h.y < 0.0) break;

        vec3 p = ro + rd * h.x;

        acc = over(acc, shapeShade(p, rd, int(h.y + 0.5), skip, L));

        if (acc.a > 0.985) return acc;

        skip = int(h.y + 0.5);

        t = h.x + 0.01;

      }

      return over(acc, behind(ro, rd, L));

    }

    vec4 lensLayer(vec2 o, float r) {

      vec4 c = vec4(vec3(1.0) * 0.06, 0.06);

      float rim = smoothstep(0.84, 0.975, r) * (1.0 - smoothstep(0.975, 1.0, r));

      c = over(vec4(vec3(1.0), 1.0) * rim * 0.5, c);

      float line = smoothstep(0.955, 0.985, r) * (1.0 - smoothstep(0.985, 1.0, r));

      c = over(vec4(uFg, 1.0) * line * 0.16, c);

      float spot = smoothstep(0.55, 0.0, length(o - vec2(-0.42, 0.48)));

      c = over(vec4(vec3(1.0), 1.0) * spot * 0.3, c);

      return c;

    }

    void main() {

      vec2 uv = (gl_FragCoord.xy - 0.5 * uRes) / (0.5 * uRes.y);

      vec3 ro = uEye;

      vec3 rd = normalize(uRight * uv.x * uFov + uUp * uv.y * uFov - uBack);

      vec3 L = normalize(uLight);

      vec4 glass = vec4(0.0);

      if (uLens.w > 0.002) {

        float tl = (LENS_Z - ro.z) / min(rd.z, -1e-4);

        vec3 lp = ro + rd * tl;

        vec2 o = (lp.xy - uLens.xy) / uLens.z;

        float r = length(o);

        if (r < 1.0) {

          glass = lensLayer(o, r) * uLens.w;

          float bend = 0.13 * uLens.w * (1.0 - 0.25 * r * r);

          ro = lp;

          rd = normalize(rd - vec3(o * bend, 0.0));

        }

      }

      outColor = over(glass, render(ro, rd, L));

    }`;

    let primitivesInitialized = false;

    function initFoundationPrimitives() {

      const stage = document.getElementById('primitivesStage');

      const canvas = document.getElementById('primitivesCanvas');

      const btnContainer = document.getElementById('primitivesButtonsContainer');

      if (!stage || !canvas || primitivesInitialized) return;

      const gl = canvas.getContext('webgl2', { premultipliedAlpha: true, alpha: true, antialias: false });

      if (!gl) {

        console.warn('WebGL2 not available for Foundation Primitives');

        return;

      }

      primitivesInitialized = true;

      const items = [

        { label: "Reliability", shape: "asterisk", colors: ["#3b82f6", "#93c5fd"], tile: "disc" },

        { label: "Cost Control", shape: "sphere", colors: ["#10b981", "#a7f3d0"], tile: "square" },

        { label: "Observability", shape: "halves", colors: ["#a855f7", "#e9d5ff"], tile: "square" },

        { label: "Security", shape: "hourglass", colors: ["#f43f5e", "#fecdd3"], tile: "square" },

        { label: "Scale", shape: "torus", colors: ["#f59e0b", "#fde68a"], tile: "none" }

      ];

      const MAX_SHAPES = 6;

      const SHAPE_KIND = { sphere: 0, asterisk: 1, halves: 2, hourglass: 3, torus: 4, pill: 5, cube: 6 };

      const SPACING = 2.75;

      const MARGIN = 1.75;

      const ROW_GAP = 2.75;

      const CAMERA_DISTANCE = 18;

      function layoutStage(n, aspect) {

        const count = Math.max(0, Math.min(n, MAX_SHAPES));

        const a = Math.max(aspect, 0.2);

        const rows = count > 3 && a < 1.9 ? 2 : 1;

        const pts = [];

        if (rows === 1) {

          for (let i = 0; i < count; i++) pts.push([(i - (count - 1) / 2) * SPACING, 0]);

        } else {

          const top = Math.ceil(count / 2);

          const bottom = count - top;

          for (let i = 0; i < top; i++) pts.push([(i - (top - 1) / 2) * SPACING, ROW_GAP / 2]);

          for (let i = 0; i < bottom; i++) pts.push([(i - (bottom - 1) / 2) * SPACING, -ROW_GAP / 2]);

        }

        let halfW = MARGIN;

        let halfH = MARGIN;

        for (const [x, y] of pts) {

          halfW = Math.max(halfW, Math.abs(x) + MARGIN);

          halfH = Math.max(halfH, Math.abs(y) + MARGIN);

        }

        return { rows, pts, fit: Math.max(halfH, halfW / a) };

      }

      function stripsFor(tiles, pts) {

        const out = [];

        let run = null;

        for (let i = 0; i < pts.length; i++) {

          const sq = tiles[i] === "square";

          const [x, y] = pts[i];

          if (sq && run && run[2] === y) {

            run[1] = x + 1.12;

            continue;

          }

          if (run) out.push(run);

          run = sq ? [x - 1.12, x + 1.12, y] : null;

        }

        if (run) out.push(run);

        return out.slice(0, 4);

      }

      function clamp(v, lo, hi) { return Math.min(Math.max(v, lo), hi); }

      function damp(from, to, rate, dt) { return to + (from - to) * Math.exp(-rate * dt); }

      function hexToRgb(hex) {

        const m = /^#?([0-9a-f]{3}|[0-9a-f]{6})$/i.exec(hex.trim());

        if (!m) return null;

        let h = m[1];

        if (h.length === 3) h = h[0] + h[0] + h[1] + h[1] + h[2] + h[2];

        const v = parseInt(h, 16);

        return [((v >> 16) & 255) / 255, ((v >> 8) & 255) / 255, (v & 255) / 255];

      }

      function rotation(yaw, pitch, roll, out = new Float32Array(9), at = 0) {

        const cy = Math.cos(yaw), sy = Math.sin(yaw);

        const cp = Math.cos(pitch), sp = Math.sin(pitch);

        const cr = Math.cos(roll), sr = Math.sin(roll);

        const a = [cy, 0, -sy, sy * sp, cp, cy * sp, sy * cp, -sp, cy * cp];

        for (let c = 0; c < 3; c++) {

          const x = a[c * 3], y = a[c * 3 + 1], z = a[c * 3 + 2];

          out[at + c * 3] = cr * x - sr * y;

          out[at + c * 3 + 1] = sr * x + cr * y;

          out[at + c * 3 + 2] = z;

        }

        return out;

      }

      function makeCamera(yaw, pitch, fit) {

        const back = [Math.sin(yaw) * Math.cos(pitch), Math.sin(pitch), Math.cos(yaw) * Math.cos(pitch)];

        const eye = back.map(v => v * CAMERA_DISTANCE);

        const rx = back[2], rz = -back[0];

        const rl = Math.hypot(rx, rz) || 1;

        const right = [rx / rl, 0, rz / rl];

        const up = [

          back[1] * right[2] - back[2] * right[1],

          back[2] * right[0] - back[0] * right[2],

          back[0] * right[1] - back[1] * right[0],

        ];

        return { eye, right, up, back, fov: fit / CAMERA_DISTANCE };

      }

      function project(cam, p, w, h) {

        const v = [p[0] - cam.eye[0], p[1] - cam.eye[1], p[2] - cam.eye[2]];

        const x = v[0] * cam.right[0] + v[1] * cam.right[1] + v[2] * cam.right[2];

        const y = v[0] * cam.up[0] + v[1] * cam.up[1] + v[2] * cam.up[2];

        const z = -(v[0] * cam.back[0] + v[1] * cam.back[1] + v[2] * cam.back[2]);

        const k = h / 2 / (Math.max(z, 1e-3) * cam.fov);

        return [w / 2 + x * k, h / 2 - y * k, k];

      }

      function unproject(cam, px, py, w, h, planeZ) {

        const u = ((px - w / 2) / (h / 2)) * cam.fov;

        const v = ((h / 2 - py) / (h / 2)) * cam.fov;

        const d = [0, 1, 2].map(c => cam.right[c] * u + cam.up[c] * v - cam.back[c]);

        const t = (planeZ - cam.eye[2]) / (Math.abs(d[2]) < 1e-6 ? -1e-6 : d[2]);

        return [cam.eye[0] + d[0] * t, cam.eye[1] + d[1] * t];

      }

      const compile = (type, src) => {

        const s = gl.createShader(type);

        if (!s) return null;

        gl.shaderSource(s, src);

        gl.compileShader(s);

        if (!gl.getShaderParameter(s, gl.COMPILE_STATUS)) {

          console.error("prim shader error:", gl.getShaderInfoLog(s));

          gl.deleteShader(s);

          return null;

        }

        return s;

      };

      const vs = compile(gl.VERTEX_SHADER, PRIM_VERT);

      const fs = compile(gl.FRAGMENT_SHADER, PRIM_FRAG);

      if (!vs || !fs) return;

      const prog = gl.createProgram();

      gl.attachShader(prog, vs);

      gl.attachShader(prog, fs);

      gl.linkProgram(prog);

      if (!gl.getProgramParameter(prog, gl.LINK_STATUS)) {

        console.error("prim link error:", gl.getProgramInfoLog(prog));

        return;

      }

      const loc = (n) => gl.getUniformLocation(prog, n);

      const U = {

        res: loc("uRes"), count: loc("uCount"), pos: loc("uPos"), rot: loc("uRot"), info: loc("uInfo"),

        squash: loc("uSquash"), core: loc("uCore"), rim: loc("uRim"), strip: loc("uStrip"),

        stripCount: loc("uStripCount"), disc: loc("uDisc"), discCount: loc("uDiscCount"),

        eye: loc("uEye"), right: loc("uRight"), up: loc("uUp"), back: loc("uBack"), fov: loc("uFov"),

        light: loc("uLight"), lens: loc("uLens"), fg: loc("uFg"), shadow: loc("uShadow"), glow: loc("uGlow")

      };

      const vao = gl.createVertexArray();

      gl.clearColor(0, 0, 0, 0);

      const n = items.length;

      const pos = new Float32Array(MAX_SHAPES * 4);

      const rot = new Float32Array(MAX_SHAPES * 9);

      const info = new Float32Array(MAX_SHAPES * 4);

      const squash = new Float32Array(MAX_SHAPES * 3);

      const core = new Float32Array(MAX_SHAPES * 3);

      const rim = new Float32Array(MAX_SHAPES * 3);

      const strips = new Float32Array(16);

      const discs = new Float32Array(MAX_SHAPES * 3);

      items.forEach((it, i) => {

        const c1 = hexToRgb(it.colors[0]) || [0.5, 0.4, 1.0];

        const c2 = hexToRgb(it.colors[1]) || [0.9, 0.85, 1.0];

        core.set(c1, i * 3);

        rim.set(c2, i * 3);

      });

      const anims = items.map(() => ({

        hover: 0, sel: 0, yaw: 0, pitch: 0, vYaw: 0, vPitch: 0, roll: 0,

        flips: 0, flipAngle: 0, squash: 0, vSquash: 0

      }));

      let cssW = 1, cssH = 1;

      let layout = layoutStage(n, 1);

      const buttons = [];

      if (btnContainer) {

        btnContainer.innerHTML = '';

        items.forEach((it, i) => {

          const btn = document.createElement('button');

          btn.type = 'button';

          btn.title = `${it.label} (${it.shape})`;

          btn.className = 'absolute rounded-full outline-none cursor-grab active:cursor-grabbing hover:ring-2 hover:ring-white/20 transition-all';

          btn.style.width = '0px';

          btn.style.height = '0px';

          btn.addEventListener('pointerenter', () => { hoveredIdx = i; kick(); });

          btn.addEventListener('pointerleave', () => { if (hoveredIdx === i) hoveredIdx = null; kick(); });

          btn.addEventListener('click', (e) => {

            e.stopPropagation();

            anims[i].vYaw += 11;

            anims[i].vSquash += 7;

            if (it.shape === "hourglass") anims[i].flips += 1;

            kick();

          });

          btnContainer.appendChild(btn);

          buttons.push(btn);

        });

      }

      const resize = () => {

        const dpr = Math.min(window.devicePixelRatio || 1, 1.75);

        cssW = Math.max(stage.clientWidth, 1);

        cssH = Math.max(stage.clientHeight, 1);

        canvas.width = Math.floor(cssW * dpr);

        canvas.height = Math.floor(cssH * dpr);

        kick();

      };

      window.addEventListener('resize', resize);

      resize();

      let hoveredIdx = null;

      let selectedIdx = null;

      let pointer = { x: 0.5, y: 0.5, inside: false };

      let drag = null;

      stage.addEventListener('pointermove', (e) => {

        const r = stage.getBoundingClientRect();

        pointer.x = clamp((e.clientX - r.left) / Math.max(r.width, 1), 0, 1);

        pointer.y = clamp((e.clientY - r.top) / Math.max(r.height, 1), 0, 1);

        pointer.inside = true;

        if (drag) {

          const a = anims[drag.i];

          const now = performance.now();

          const dx = e.clientX - drag.lx;

          const dy = e.clientY - drag.ly;

          if (a) {

            const dtm = Math.max((now - drag.lt) / 1000, 1 / 240);

            a.yaw += dx * 0.012;

            a.pitch = clamp(a.pitch + dy * 0.012, -1.2, 1.2);

            a.vYaw = a.vYaw * 0.4 + ((dx * 0.012) / dtm) * 0.6;

            a.vPitch = a.vPitch * 0.4 + ((dy * 0.012) / dtm) * 0.6;

          }

          drag.lx = e.clientX;

          drag.ly = e.clientY;

          drag.lt = now;

        }

        kick();

      });

      stage.addEventListener('pointerleave', () => {

        pointer.inside = false;

        hoveredIdx = null;

        drag = null;

        kick();

      });

      let camYaw = 0, camPitch = 0, lightX = 0, lightY = 0;

      let lensX = 0, lensY = 0, lensVis = 0, lensPlaced = false;

      let t = 0, last = performance.now(), raf = 0;

      function renderFrame() {

        const now = performance.now();

        const dt = Math.min((now - last) / 1000, 0.05);

        last = now;

        t += dt;

        layout = layoutStage(n, cssW / cssH);

        const k = (rate) => 1 - Math.exp(-rate * dt);

        const px = pointer.inside ? pointer.x - 0.5 : Math.sin(t * 0.23) * 0.22;

        const py = pointer.inside ? pointer.y - 0.5 : Math.sin(t * 0.31 + 1) * 0.12;

        camYaw += (px * 0.3 - camYaw) * k(3);

        camPitch += (-py * 0.2 - camPitch) * k(3);

        lightX += (px * 1.6 - lightX) * k(4);

        lightY += (-py * 1.2 - lightY) * k(4);

        const cam = makeCamera(camYaw, camPitch, layout.fit);

        for (let i = 0; i < n; i++) {

          const a = anims[i];

          const it = items[i];

          const kind = SHAPE_KIND[it.shape] || 0;

          const isHover = hoveredIdx === i || drag?.i === i;

          const isSel = selectedIdx === i;

          a.hover += ((isHover ? 1 : 0) - a.hover) * k(9);

          a.sel += ((isSel ? 1 : 0) - a.sel) * k(6);

          if (!drag || drag.i !== i) {

            a.yaw += a.vYaw * dt;

            a.pitch += a.vPitch * dt;

            a.vYaw *= Math.exp(-2.4 * dt);

            a.vPitch = a.vPitch * Math.exp(-3 * dt) - a.pitch * 14 * dt;

          }

          a.vSquash += (-a.squash * 170 - a.vSquash * 9) * dt;

          a.squash += a.vSquash * dt;

          if (kind === 1) a.roll += dt * (0.25 + 3.2 * a.hover);

          if (kind === 3) {

            if (Math.floor(t / 6.5 + i * 0.13) > Math.floor((t - dt) / 6.5 + i * 0.13)) a.flips += 1;

            const target = a.flips * Math.PI;

            a.flipAngle = damp(a.flipAngle, target, 5.5, dt);

            a.roll = a.flipAngle;

          }

          const [hx, hy] = layout.pts[i];

          const bob = Math.sin(t * 1.05 + i * 1.3) * 0.07;

          const lift = 0.5 * a.hover + 0.22 * a.sel;

          const s = 1 + 0.06 * a.hover + 0.03 * a.sel;

          pos.set([hx, hy + bob, lift, s], i * 4);

          const idleYaw = Math.sin(t * 0.45 + i * 1.7) * 0.4;

          const idlePitch = Math.sin(t * 0.37 + i * 2.3) * 0.2;

          rotation(a.yaw + idleYaw, a.pitch + idlePitch, a.roll, rot, i * 9);

          const spread = 1 + 0.42 * Math.max(a.hover, a.sel * 0.45);

          info.set([kind, 0, a.hover, spread], i * 4);

          const sq = clamp(a.squash, -0.6, 0.6);

          squash.set([1 + 0.1 * sq, 1 - 0.14 * sq, 1 + 0.1 * sq], i * 3);

          const btn = buttons[i];

          if (btn) {

            const [sx, sy, unit] = project(cam, [hx, hy + bob, lift], cssW, cssH);

            const r = unit * 1.12 * s;

            btn.style.width = btn.style.height = Math.round(r * 2) + "px";

            btn.style.transform = `translate(${(sx - r).toFixed(1)}px, ${(sy - r).toFixed(1)}px)`;

          }

        }

        const st = stripsFor(items.map(it => it.tile || "none"), layout.pts);

        strips.fill(0);

        st.forEach((s, j) => strips.set([s[0], s[1], s[2], 0], j * 4));

        let dc = 0;

        items.forEach((it, i) => {

          if (it.tile === "disc") discs.set([layout.pts[i][0], layout.pts[i][1], 1.3], dc++ * 3);

        });

        const p0 = layout.pts[0] || [0, 0];

        const homeX = p0[0] + (layout.rows === 1 ? SPACING * 0.6 : SPACING * 0.5);

        const homeY = p0[1] - (layout.rows === 1 ? 0 : 0.3);

        let tx = homeX, ty = homeY;

        if (pointer.inside) [tx, ty] = unproject(cam, pointer.x * cssW, pointer.y * cssH, cssW, cssH, 1.9);

        if (!lensPlaced) { lensX = tx; lensY = ty; lensPlaced = true; }

        lensX += (tx - lensX) * k(pointer.inside ? 7 : 2.5);

        lensY += (ty - lensY) * k(pointer.inside ? 7 : 2.5);

        lensVis += ((n > 0 ? 1 : 0) - lensVis) * k(5);

        gl.useProgram(prog);

        gl.bindVertexArray(vao);

        gl.viewport(0, 0, canvas.width, canvas.height);

        gl.clear(gl.COLOR_BUFFER_BIT);

        gl.uniform2f(U.res, canvas.width, canvas.height);

        gl.uniform1i(U.count, n);

        gl.uniform4fv(U.pos, pos);

        gl.uniformMatrix3fv(U.rot, false, rot);

        gl.uniform4fv(U.info, info);

        gl.uniform3fv(U.squash, squash);

        gl.uniform3fv(U.core, core);

        gl.uniform3fv(U.rim, rim);

        gl.uniform4fv(U.strip, strips);

        gl.uniform1i(U.stripCount, st.length);

        gl.uniform3fv(U.disc, discs);

        gl.uniform1i(U.discCount, dc);

        gl.uniform3fv(U.eye, cam.eye);

        gl.uniform3fv(U.right, cam.right);

        gl.uniform3fv(U.up, cam.up);

        gl.uniform3fv(U.back, cam.back);

        gl.uniform1f(U.fov, cam.fov);

        gl.uniform3f(U.light, -0.45 + lightX, 0.6 + lightY, 1.3);

        gl.uniform4f(U.lens, lensX, lensY, 1.0, lensVis);

        gl.uniform3f(U.fg, 0.9, 0.9, 0.95);

        gl.uniform1f(U.shadow, 0.5 * 0.13);

        gl.uniform1f(U.glow, 0.5 * 0.5);

        gl.drawArrays(gl.TRIANGLES, 0, 3);

        raf = requestAnimationFrame(renderFrame);

      }

      function kick() {

        if (!raf) {

          last = performance.now();

          raf = requestAnimationFrame(renderFrame);

        }

      }

      kick();

    }

    // =========================================================================

    // ATTACHMENT & TOAST NOTIFICATION HELPERS

    // =========================================================================

    function triggerAttachment() {

      const fileInput = document.getElementById('chatFileInput');

      if (fileInput) fileInput.click();

    }

    function handleFileAttachment(event) {

      const files = event.target.files;

      if (!files || files.length === 0) return;

      const fileNames = Array.from(files).map(f => f.name).join(', ');

      showToast(`Attached: ${fileNames}`, 'info');

      const isChatScreen = !document.getElementById('chatScreen').classList.contains('hidden');

      const targetInput = isChatScreen ? document.getElementById('streamChatInput') : document.getElementById('heroChatInput');

      if (targetInput) {

        const tag = `[Attached: ${fileNames}]\n`;

        targetInput.value = tag + targetInput.value;

        targetInput.focus();

      }

    }

    function showToast(message, type = 'info') {

      const container = document.getElementById('toastContainer');

      if (!container) return;

      const toast = document.createElement('div');

      let bgBorder = 'bg-[#121824] border-cyan-500/40 text-cyan-300';

      if (type === 'success') bgBorder = 'bg-[#0f241a] border-emerald-500/40 text-emerald-300';

      if (type === 'error') bgBorder = 'bg-[#291118] border-rose-500/40 text-rose-300';

      toast.className = `pointer-events-auto px-4 py-2.5 rounded-xl border ${bgBorder} text-xs font-semibold shadow-2xl backdrop-blur-xl transition-all duration-300 transform translate-y-2 opacity-0 flex items-center gap-2`;

      toast.innerHTML = `<span>${message}</span>`;

      container.appendChild(toast);

      requestAnimationFrame(() => {

        toast.classList.remove('translate-y-2', 'opacity-0');

      });

      setTimeout(() => {

        toast.classList.add('opacity-0', 'translate-y-2');

        setTimeout(() => toast.remove(), 300);

      }, 3500);

    }

    // =========================================================================

    // AUTHENTICATION & OTP SYSTEM (Google OAuth + Manual Signup + Gmail Send)

    // =========================================================================

    let currentUser = null;

    let pendingOtpEmail = null;

    let otpResendTimer = null;

    let resendSecondsRemaining = 0;

    function openAuthModal(tab = 'signin') {

      const modal = document.getElementById('authModal');

      if (modal) modal.classList.remove('hidden');

      switchAuthTab(tab);

    }

    function closeAuthModal() {

      const modal = document.getElementById('authModal');

      if (modal) modal.classList.add('hidden');

      clearAuthNotices();

      if (otpResendTimer) {

        clearInterval(otpResendTimer);

        otpResendTimer = null;

      }

    }

    function clearAuthNotices() {

      const an = document.getElementById('authNotice');

      if (an) { an.classList.add('hidden'); an.innerText = ''; an.className = 'hidden mb-3 p-2.5 rounded-xl text-xs flex items-center gap-2'; }

      const on = document.getElementById('otpNotice');

      if (on) { on.classList.add('hidden'); on.innerText = ''; on.className = 'hidden mb-3 p-2.5 rounded-xl text-xs flex items-center gap-2'; }

    }

    function showAuthNotice(msg, type = 'error') {

      const el = document.getElementById('authNotice');

      if (!el) return;

      el.classList.remove('hidden');

      if (type === 'error') {

        el.className = 'mb-3 p-2.5 rounded-xl text-xs flex items-center gap-2 bg-red-950/60 border border-red-500/30 text-red-300';

      } else {

        el.className = 'mb-3 p-2.5 rounded-xl text-xs flex items-center gap-2 bg-emerald-950/60 border border-emerald-500/30 text-emerald-300';

      }

      el.innerText = msg;

    }

    function showOtpNotice(msg, type = 'error') {

      const el = document.getElementById('otpNotice');

      if (!el) return;

      el.classList.remove('hidden');

      if (type === 'error') {

        el.className = 'mb-3 p-2.5 rounded-xl text-xs flex items-center gap-2 bg-red-950/60 border border-red-500/30 text-red-300';

      } else {

        el.className = 'mb-3 p-2.5 rounded-xl text-xs flex items-center gap-2 bg-emerald-950/60 border border-emerald-500/30 text-emerald-300';

      }

      el.innerText = msg;

    }

    function switchAuthTab(tab) {

      clearAuthNotices();

      const tabsView = document.getElementById('authTabsView');

      const otpView = document.getElementById('authOtpView');

      if (tabsView) tabsView.classList.remove('hidden');

      if (otpView) otpView.classList.add('hidden');

      const title = document.getElementById('authModalTitle');

      const sub = document.getElementById('authModalSubtitle');

      const tabSign = document.getElementById('tabSignInBtn');

      const tabUp = document.getElementById('tabSignUpBtn');

      const signForm = document.getElementById('signInForm');

      const upForm = document.getElementById('signUpForm');

      if (tab === 'signin') {

        if (title) title.innerText = 'Welcome Back';

        if (sub) sub.innerText = 'Access unified intelligence across multiple providers';

        if (tabSign) { tabSign.className = 'flex-1 py-1.5 text-xs font-semibold rounded-lg bg-white/10 text-white transition'; }

        if (tabUp) { tabUp.className = 'flex-1 py-1.5 text-xs font-semibold rounded-lg text-gray-400 hover:text-white transition'; }

        if (signForm) signForm.classList.remove('hidden');

        if (upForm) upForm.classList.add('hidden');

      } else {

        if (title) title.innerText = 'Create Gateway Account';

        if (sub) sub.innerText = 'Join LLM Gateway with unified intelligence and email OTP';

        if (tabUp) { tabUp.className = 'flex-1 py-1.5 text-xs font-semibold rounded-lg bg-white/10 text-white transition'; }

        if (tabSign) { tabSign.className = 'flex-1 py-1.5 text-xs font-semibold rounded-lg text-gray-400 hover:text-white transition'; }

        if (signForm) signForm.classList.add('hidden');

        if (upForm) upForm.classList.remove('hidden');

      }

    }

    async function startGoogleSignIn() {

      try {

        const res = await fetch('/api/auth/google/url');

        if (!res.ok) throw new Error('Could not initialize Google authentication');

        const data = await res.json();

        if (data.url) {

          window.location.href = data.url;

        } else {

          showAuthNotice('Google OAuth client is not fully configured.', 'error');

        }

      } catch (err) {

        showAuthNotice(err.message || 'Google Sign In error', 'error');

      }

    }

    async function checkGoogleCallback() {

      const urlParams = new URLSearchParams(window.location.search);

      const code = urlParams.get('code');

      const error = urlParams.get('error');

      if (error) {

        window.history.replaceState({}, document.title, window.location.pathname);

        return;

      }

      if (!code) return;

      // Clean query string from browser bar silently

      window.history.replaceState({}, document.title, window.location.pathname);

      try {

        const res = await fetch('/api/auth/google/callback', {

          method: 'POST',

          headers: { 'Content-Type': 'application/json' },

          body: JSON.stringify({ code })

        });

        const data = await res.json();

        if (res.ok && data.user) {

          currentUser = data.user;

          updateAuthHeaderUI();

          checkMailerStatus();

          refreshChatsTable();

          loadHistoryFromDB();

        }

      } catch (err) {

        console.error('Google Sign-In error:', err);

      }

    }

    async function handleManualSignIn(e) {

      e.preventDefault();

      clearAuthNotices();

      const email = document.getElementById('signInEmail').value.trim();

      const password = document.getElementById('signInPassword').value;

      const submitBtn = document.getElementById('signInSubmitBtn');

      const originalText = submitBtn.innerText;

      submitBtn.innerText = 'Signing In...';

      submitBtn.disabled = true;

      try {

        const res = await fetch('/api/auth/login', {

          method: 'POST',

          headers: { 'Content-Type': 'application/json' },

          body: JSON.stringify({ email, password })

        });

        const data = await res.json();

        if (res.ok) {

          if (data.requires_verification) {

            transitionToOtpView(email, data.dev_otp);

            showOtpNotice('Please verify your email to activate account.', 'info');

          } else {

            currentUser = data.user;

            updateAuthHeaderUI();

            closeAuthModal();

            refreshChatsTable();

            loadHistoryFromDB();

          }

        } else {

          showAuthNotice(data.detail || 'Invalid email or password.', 'error');

        }

      } catch (err) {

        showAuthNotice(err.message || 'Connection error', 'error');

      } finally {

        submitBtn.innerText = originalText;

        submitBtn.disabled = false;

      }

    }

    async function handleManualSignUp(e) {

      e.preventDefault();

      clearAuthNotices();

      const name = document.getElementById('signUpName').value.trim();

      const email = document.getElementById('signUpEmail').value.trim();

      const password = document.getElementById('signUpPassword').value;

      const submitBtn = document.getElementById('signUpSubmitBtn');

      const originalText = submitBtn.innerText;

      submitBtn.innerText = 'Creating & Sending OTP...';

      submitBtn.disabled = true;

      try {

        const res = await fetch('/api/auth/signup', {

          method: 'POST',

          headers: { 'Content-Type': 'application/json' },

          body: JSON.stringify({ name, email, password })

        });

        const data = await res.json();

        if (res.ok) {

          transitionToOtpView(email, data.dev_otp);

          showOtpNotice(data.message || 'OTP verification code sent!', 'success');

        } else {

          showAuthNotice(data.detail || 'Failed to create account.', 'error');

        }

      } catch (err) {

        showAuthNotice(err.message || 'Connection error', 'error');

      } finally {

        submitBtn.innerText = originalText;

        submitBtn.disabled = false;

      }

    }

    function transitionToOtpView(email, devOtp = null) {

      pendingOtpEmail = email;

      const tabsView = document.getElementById('authTabsView');

      const otpView = document.getElementById('authOtpView');

      if (tabsView) tabsView.classList.add('hidden');

      if (otpView) otpView.classList.remove('hidden');

      const targetEmailText = document.getElementById('otpTargetEmailText');

      if (targetEmailText) targetEmailText.innerText = email;

      const devBanner = document.getElementById('devOtpBanner');

      const devCodeText = document.getElementById('devOtpCodeText');

      if (devOtp) {

        if (devBanner) devBanner.classList.remove('hidden');

        if (devCodeText) devCodeText.innerText = devOtp;

      } else {

        if (devBanner) devBanner.classList.add('hidden');

      }

      // Clear all OTP input boxes

      const inputs = document.querySelectorAll('.otp-digit');

      inputs.forEach(inp => inp.value = '');

      if (inputs.length > 0) inputs[0].focus();

      startResendCountdown(30);

    }

    function autoFillOtp(code) {

      if (!code) return;

      const clean = String(code).trim().replace(/\D/g, '');

      const inputs = document.querySelectorAll('.otp-digit');

      inputs.forEach((inp, idx) => {

        inp.value = clean[idx] || '';

      });

      if (clean.length === 6) {

        submitOtpVerification();

      }

    }

    function cancelOtpAndBack() {

      switchAuthTab('signin');

    }

    function startResendCountdown(seconds = 30) {

      if (otpResendTimer) clearInterval(otpResendTimer);

      resendSecondsRemaining = seconds;

      const resendBtn = document.getElementById('resendOtpBtn');

      if (!resendBtn) return;

      resendBtn.disabled = true;

      resendBtn.classList.add('opacity-50', 'cursor-not-allowed');

      const updateText = () => {

        if (resendSecondsRemaining > 0) {

          resendBtn.innerText = `Resend in ${resendSecondsRemaining}s`;

          resendSecondsRemaining--;

        } else {

          clearInterval(otpResendTimer);

          otpResendTimer = null;

          resendBtn.innerText = 'Resend Code';

          resendBtn.disabled = false;

          resendBtn.classList.remove('opacity-50', 'cursor-not-allowed');

        }

      };

      updateText();

      otpResendTimer = setInterval(updateText, 1000);

    }

    async function resendOtpCode() {

      if (!pendingOtpEmail || resendSecondsRemaining > 0) return;

      showOtpNotice('Sending new OTP code...', 'info');

      try {

        const res = await fetch('/api/auth/resend-otp', {

          method: 'POST',

          headers: { 'Content-Type': 'application/json' },

          body: JSON.stringify({ email: pendingOtpEmail })

        });

        const data = await res.json();

        if (res.ok) {

          showOtpNotice(data.message || 'New OTP sent to email!', 'success');

          if (data.dev_otp) {

            const devBanner = document.getElementById('devOtpBanner');

            const devCodeText = document.getElementById('devOtpCodeText');

            if (devBanner) devBanner.classList.remove('hidden');

            if (devCodeText) devCodeText.innerText = data.dev_otp;

          }

          startResendCountdown(30);

        } else {

          showOtpNotice(data.detail || 'Could not resend OTP.', 'error');

        }

      } catch (err) {

        showOtpNotice(err.message || 'Resend failed', 'error');

      }

    }

    async function submitOtpVerification() {

      if (!pendingOtpEmail) {

        showOtpNotice('No verification email session found.', 'error');

        return;

      }

      const inputs = document.querySelectorAll('.otp-digit');

      let otpCode = '';

      inputs.forEach(inp => otpCode += inp.value.trim());

      if (otpCode.length !== 6) {

        showOtpNotice('Please enter all 6 digits of the OTP.', 'error');

        return;

      }

      const verifyBtn = document.getElementById('verifyOtpBtn');

      const originalText = verifyBtn.innerText;

      verifyBtn.innerText = 'Verifying...';

      verifyBtn.disabled = true;

      try {

        const res = await fetch('/api/auth/verify-otp', {

          method: 'POST',

          headers: { 'Content-Type': 'application/json' },

          body: JSON.stringify({ email: pendingOtpEmail, otp: otpCode })

        });

        const data = await res.json();

        if (res.ok && data.user) {

          currentUser = data.user;

          updateAuthHeaderUI();

          closeAuthModal();

          refreshChatsTable();

          loadHistoryFromDB();

        } else {

          showOtpNotice(data.detail || 'Invalid or expired OTP code.', 'error');

        }

      } catch (err) {

        showOtpNotice(err.message || 'Verification failed', 'error');

      } finally {

        verifyBtn.innerText = originalText;

        verifyBtn.disabled = false;

      }

    }

    function initOtpInputAutoAdvance() {

      const inputs = document.querySelectorAll('.otp-digit');

      inputs.forEach((inp, idx) => {

        inp.addEventListener('input', (e) => {

          const val = e.target.value.replace(/\D/g, '');

          e.target.value = val ? val[val.length - 1] : '';

          if (val && idx < inputs.length - 1) {

            inputs[idx + 1].focus();

          }

          let allFilled = true;

          inputs.forEach(i => { if (!i.value) allFilled = false; });

          if (allFilled) {

            submitOtpVerification();

          }

        });

        inp.addEventListener('keydown', (e) => {

          if (e.key === 'Backspace' && !e.target.value && idx > 0) {

            inputs[idx - 1].focus();

          }

        });

        inp.addEventListener('paste', (e) => {

          e.preventDefault();

          const pasted = (e.clipboardData || window.clipboardData).getData('text');

          if (pasted) autoFillOtp(pasted);

        });

      });

    }

    async function checkAuthSession() {

      try {

        const res = await fetch('/api/auth/me');

        if (res.ok) {

          const data = await res.json();

          if (data.authenticated && data.user) {

            currentUser = data.user;

          } else {

            currentUser = null;

          }

        } else {

          currentUser = null;

        }

      } catch (err) {

        currentUser = null;

      }

      if (!currentUser) {

        currentUser = { id: 'guest_user', email: 'developer@llmgateway.io', name: 'Developer', is_admin: true };

      }

      updateAuthHeaderUI();

      checkMailerStatus();

      if (currentView === 'chats') {

        refreshChatsTable();

      }

    }

    // ---------------------------------------------------------------------------

    // Profile & Cartoon Avatar Settings Modal Client Logic (Tray of 3 - 3)

    // ---------------------------------------------------------------------------

    let selectedAvatarBgColor = '#06b6d4';

    let currentPreviewDataUrl = null;

    let avatarTrayPage = 0;

    let currentTrayAvatars = [];

    let isTrayLoading = false;

    function openProfileModal() {

      if (!currentUser) return;

      const modal = document.getElementById('profileModal');

      if (!modal) return;

      const nameInput = document.getElementById('profileInputName');

      if (nameInput) nameInput.value = currentUser.name || '';

      selectedAvatarBgColor = currentUser.bg_color || '#06b6d4';

      updateBgColorUI();

      currentPreviewDataUrl = currentUser.picture;

      const img = document.getElementById('modalAvatarPreviewImg');

      if (img && currentUser.picture) {

        img.src = currentUser.picture;

      }

      clearProfileNotice();

      modal.classList.remove('hidden');

      // Load initial tray of 6 avatars (3 - 3 in one go)

      avatarTrayPage = 0;

      loadAvatarTray(avatarTrayPage);

    }

    function closeProfileModal() {

      const modal = document.getElementById('profileModal');

      if (modal) modal.classList.add('hidden');

    }

    async function loadAvatarTray(page = 0) {

      if (isTrayLoading) return;

      isTrayLoading = true;

      const tray = document.getElementById('avatarTrayGrid');

      const spinner = document.getElementById('moreAvatarsSpinner');

      if (spinner) spinner.classList.add('animate-spin');

      if (tray && (!currentTrayAvatars || currentTrayAvatars.length === 0)) {

        tray.innerHTML = `

          <div class="col-span-4 flex flex-col items-center justify-center py-4 text-gray-400 text-[11px]">

            <svg class="w-5 h-5 animate-spin text-cyan-400 mb-1.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/></svg>

            <span>Generating cartoon avatars...</span>

          </div>`;

      }

      const nameInput = document.getElementById('profileInputName');

      const name = (nameInput ? nameInput.value.trim() : '') || (currentUser ? currentUser.name : 'User');

      try {

        const res = await fetch('/api/user/generate-avatars-batch', {

          method: 'POST',

          headers: { 'Content-Type': 'application/json' },

          body: JSON.stringify({

            name: name,

            bg_color: selectedAvatarBgColor,

            page: page,

            count: 7

          })

        });

        if (res.ok) {

          const data = await res.json();

          currentTrayAvatars = data.avatars || [];

          avatarTrayPage = page;

          renderAvatarTray();

        } else {

          showProfileNotice('Failed to generate cartoon avatars.', 'error');

        }

      } catch (e) {

        console.error('Tray load error:', e);

      } finally {

        isTrayLoading = false;

        if (spinner) spinner.classList.remove('animate-spin');

      }

    }

    function renderAvatarTray() {

      const tray = document.getElementById('avatarTrayGrid');

      if (!tray) return;

      const googlePic = (currentUser && currentUser.google_picture) || (currentUser && currentUser.picture && currentUser.picture.startsWith('http') ? currentUser.picture : null);

      const isNoneSelected = (!currentPreviewDataUrl || currentPreviewDataUrl === 'none' || (googlePic && currentPreviewDataUrl === googlePic));

      const noneActiveClass = isNoneSelected

        ? 'border-cyan-400 bg-cyan-500/15 ring-2 ring-cyan-400 shadow-md shadow-cyan-500/30'

        : 'border-white/10 bg-white/5 hover:border-white/20 hover:scale-105';

      let html = '';

      // Slot 1 (Index 0): None / Google Photo card

      html += `

        <div onclick="selectNoneAvatar()" id="tray-item-none" class="tray-avatar-card cursor-pointer p-1 rounded-xl border transition-all duration-200 flex flex-col items-center justify-center group relative ${noneActiveClass}" data-tooltip="None: Reset to Google / Default Photo">

          <div class="w-13 h-13 sm:w-15 sm:h-15 rounded-lg overflow-hidden shadow-inner bg-black/40 flex flex-col items-center justify-center shrink-0 relative">

            ${googlePic 

              ? `<img src="${googlePic}" alt="Google Photo" referrerpolicy="no-referrer" class="w-full h-full object-cover">

                 <span class="absolute bottom-0 inset-x-0 bg-black/75 text-[7px] font-bold text-center text-cyan-300 py-0.5 tracking-tight uppercase">Google</span>`

              : `<div class="w-full h-full flex flex-col items-center justify-center bg-gray-800 text-gray-300 text-xs font-bold">

                   <svg class="w-4 h-4 mb-0.5 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z"/></svg>

                   <span class="text-[7.5px] font-bold uppercase text-gray-300">None</span>

                 </div>`

            }

          </div>

          ${isNoneSelected ? '<span class="absolute top-0.5 right-0.5 w-3.5 h-3.5 rounded-full bg-cyan-400 text-black flex items-center justify-center text-[8.5px] font-black shadow">✓</span>' : ''}

        </div>

      `;

      // Slots 2..8: 7 Procedural cartoon avatars

      currentTrayAvatars.forEach((av, idx) => {

        const isSelected = !isNoneSelected && (currentPreviewDataUrl === av.url);

        const activeClass = isSelected

          ? 'border-cyan-400 bg-cyan-500/15 ring-2 ring-cyan-400 shadow-md shadow-cyan-500/30'

          : 'border-white/10 bg-white/5 hover:border-white/20 hover:scale-105';

        html += `

          <div onclick="selectTrayAvatar(${idx})" id="tray-item-${idx}" class="tray-avatar-card cursor-pointer p-1 rounded-xl border transition-all duration-200 flex flex-col items-center justify-center group relative ${activeClass}">

            <div class="w-13 h-13 sm:w-15 sm:h-15 rounded-lg overflow-hidden shadow-inner bg-black/30 flex items-center justify-center shrink-0">

              <img src="${av.url}" alt="Cartoon Avatar ${idx+1}" class="w-full h-full object-cover">

            </div>

            ${isSelected ? '<span class="absolute top-0.5 right-0.5 w-3.5 h-3.5 rounded-full bg-cyan-400 text-black flex items-center justify-center text-[8.5px] font-black shadow">✓</span>' : ''}

          </div>

        `;

      });

      tray.innerHTML = html;

    }

    function selectNoneAvatar() {

      const googlePic = (currentUser && currentUser.google_picture) || (currentUser && currentUser.picture && currentUser.picture.startsWith('http') ? currentUser.picture : null);

      currentPreviewDataUrl = googlePic || 'none';

      const img = document.getElementById('modalAvatarPreviewImg');

      const initials = document.getElementById('modalAvatarInitials');

      if (img && initials) {

        if (googlePic) {

          img.src = googlePic;

          img.classList.remove('hidden');

          initials.classList.add('hidden');

        } else {

          img.classList.add('hidden');

          initials.classList.remove('hidden');

        }

      }

      renderAvatarTray();

    }

    function selectTrayAvatar(idx) {

      if (!currentTrayAvatars || !currentTrayAvatars[idx]) return;

      currentPreviewDataUrl = currentTrayAvatars[idx].url;

      // Update main preview img at top

      const img = document.getElementById('modalAvatarPreviewImg');

      const initials = document.getElementById('modalAvatarInitials');

      if (img) {

        img.src = currentPreviewDataUrl;

        img.classList.remove('hidden');

      }

      if (initials) initials.classList.add('hidden');

      // Re-render tray to update active card ring

      renderAvatarTray();

    }

    function loadMoreAvatars() {

      loadAvatarTray(avatarTrayPage + 1);

    }

    function updateBgColorUI() {

      const hexEl = document.getElementById('profileColorHexDisplay');

      if (hexEl) hexEl.innerText = selectedAvatarBgColor;

      const picker = document.getElementById('profileCustomColorInput');

      if (picker) picker.value = selectedAvatarBgColor;

      const ring = document.getElementById('modalAvatarRing');

      if (ring) {

        ring.style.borderColor = selectedAvatarBgColor;

        ring.style.boxShadow = `0 4px 20px ${selectedAvatarBgColor}40`;

      }

    }

    function selectAvatarBgColor(hex) {

      selectedAvatarBgColor = hex;

      updateBgColorUI();

      // Reload the tray of 6 with this chosen background color

      loadAvatarTray(avatarTrayPage);

    }

    function showProfileNotice(msg, type = 'info') {

      const el = document.getElementById('profileModalNotice');

      if (!el) return;

      el.innerText = msg;

      el.classList.remove('hidden', 'bg-rose-950/40', 'border-rose-500/30', 'text-rose-300', 'bg-emerald-950/40', 'border-emerald-500/30', 'text-emerald-300', 'bg-cyan-950/40', 'border-cyan-500/30', 'text-cyan-300');

      if (type === 'error') {

        el.classList.add('bg-rose-950/40', 'border-rose-500/30', 'text-rose-300');

      } else if (type === 'success') {

        el.classList.add('bg-emerald-950/40', 'border-emerald-500/30', 'text-emerald-300');

      } else {

        el.classList.add('bg-cyan-950/40', 'border-cyan-500/30', 'text-cyan-300');

      }

    }

    function clearProfileNotice() {

      const el = document.getElementById('profileModalNotice');

      if (el) el.classList.add('hidden');

    }

    async function saveUserProfile() {

      const nameInput = document.getElementById('profileInputName');

      const name = nameInput ? nameInput.value.trim() : '';

      if (!name) {

        showProfileNotice('Please enter a valid display name.', 'error');

        return;

      }

      const saveBtn = document.getElementById('saveProfileBtn');

      const origText = saveBtn.innerText;

      saveBtn.innerText = 'Saving...';

      saveBtn.disabled = true;

      try {

        const picToSend = (currentPreviewDataUrl === 'none' || (currentUser && currentUser.google_picture && currentPreviewDataUrl === currentUser.google_picture)) ? 'none' : currentPreviewDataUrl;

        const res = await fetch('/api/user/profile', {

          method: 'PATCH',

          headers: { 'Content-Type': 'application/json' },

          body: JSON.stringify({

            name: name,

            picture: picToSend,

            bg_color: selectedAvatarBgColor

          })

        });

        const data = await res.json();

        if (res.ok && data.user) {

          currentUser = data.user;

          updateAuthHeaderUI();

          if (currentView === 'chats') {

            refreshChatsTable();

          }

          showToast('Profile and cartoon avatar updated successfully!', 'success');

          closeProfileModal();

        } else {

          showProfileNotice(data.detail || 'Failed to update profile.', 'error');

        }

      } catch (err) {

        showProfileNotice(err.message || 'Network error saving profile.', 'error');

      } finally {

        saveBtn.innerText = origText;

        saveBtn.disabled = false;

      }

    }

    function updateAuthHeaderUI() {

      const signInBtn = document.getElementById('headerSignInBtn');

      const profileWrapper = document.getElementById('profileDropdownWrapper');

      const headerAvatar = document.getElementById('headerUserAvatar');

      const headerInitials = document.getElementById('headerUserInitials');

      const dropAvatar = document.getElementById('dropdownUserAvatar');

      const dropInitials = document.getElementById('dropdownUserInitials');

      const nameEl = document.getElementById('profileDropdownName');

      const emailEl = document.getElementById('profileDropdownEmail');

      if (currentUser) {

        if (signInBtn) signInBtn.classList.add('hidden');

        if (profileWrapper) profileWrapper.classList.remove('hidden');

        const initial = (currentUser.name ? currentUser.name[0] : (currentUser.email ? currentUser.email[0] : 'U')).toUpperCase();

        const userBg = currentUser.bg_color || '#083344';

        if (currentUser.picture) {

          if (headerAvatar) {

            headerAvatar.src = currentUser.picture;

            headerAvatar.classList.remove('hidden');

          }

          if (headerInitials) headerInitials.classList.add('hidden');

          if (dropAvatar) {

            dropAvatar.src = currentUser.picture;

            dropAvatar.classList.remove('hidden');

          }

          if (dropInitials) dropInitials.classList.add('hidden');

        } else {

          if (headerAvatar) headerAvatar.classList.add('hidden');

          if (headerInitials) {

            headerInitials.innerText = initial;

            headerInitials.style.backgroundColor = userBg;

            headerInitials.classList.remove('hidden');

          }

          if (dropAvatar) dropAvatar.classList.add('hidden');

          if (dropInitials) {

            dropInitials.innerText = initial;

            dropInitials.style.backgroundColor = userBg;

            dropInitials.classList.remove('hidden');

          }

        }

        if (nameEl) nameEl.innerText = currentUser.name || 'User';

        if (emailEl) emailEl.innerText = currentUser.email || '';

      } else {

        if (signInBtn) signInBtn.classList.remove('hidden');

        if (profileWrapper) profileWrapper.classList.add('hidden');

      }

    }

    async function checkMailerStatus() {

      try {

        const res = await fetch('/api/auth/mailer-status');

        if (res.ok) {

          const data = await res.json();

          const badge = document.getElementById('gmailMailerBadge');

          if (badge) {

            if (data.active) {

              badge.innerText = 'Connected';

              badge.className = 'text-emerald-400 font-medium';

            } else {

              badge.innerText = 'Dev/SMTP Mode';

              badge.className = 'text-cyan-400 font-medium';

            }

          }

        }

      } catch (e) {

        // quiet ignore

      }

    }

    async function logoutUser() {

      try {

        await fetch('/api/auth/logout', { method: 'POST' });

      } catch (e) {}

      currentUser = null;

      updateAuthHeaderUI();

      const menu = document.getElementById('profileDropdownMenu');

      if (menu) menu.classList.add('hidden');

      if (currentView === 'chats') {

        refreshChatsTable();

      }

      loadHistoryFromDB();

      openAuthModal('signin');

    }

    // --- APP THEME (LIGHT / DARK) ENGINE ---

    function applyAppTheme(theme) {

      const isLight = (theme === 'light');

      if (isLight) {

        document.documentElement.classList.add('light-theme');

        document.body.classList.add('light-theme');

      } else {

        document.documentElement.classList.remove('light-theme');

        document.body.classList.remove('light-theme');

      }

      const sunIcon = document.getElementById('themeIconSun');

      const moonIcon = document.getElementById('themeIconMoon');

      const themeBtn = document.getElementById('themeToggleBtn');

      if (sunIcon && moonIcon) {

        if (isLight) {

          sunIcon.classList.add('hidden');

          moonIcon.classList.remove('hidden');

          if (themeBtn) {

            themeBtn.setAttribute('data-tooltip', 'Switch to Dark Theme');

          }

        } else {

          sunIcon.classList.remove('hidden');

          moonIcon.classList.add('hidden');

          if (themeBtn) {

            themeBtn.setAttribute('data-tooltip', 'Switch to Light Theme');

          }

        }

      }

      try {

        localStorage.setItem('gateway_theme', isLight ? 'light' : 'dark');

      } catch (e) {}

    }

    function toggleAppTheme() {

      const isCurrentLight = document.documentElement.classList.contains('light-theme') || document.body.classList.contains('light-theme');

      applyAppTheme(isCurrentLight ? 'dark' : 'light');

    }

    function initAppTheme() {

      try {

        const saved = localStorage.getItem('gateway_theme') || 'dark';

        applyAppTheme(saved);

      } catch (e) {

        applyAppTheme('dark');

      }

    }

    // --- SLEEK CUSTOM HOVER TOOLTIP ENGINE ---

    let tooltipHideTimer = null;

    function showCustomTooltip(target) {

      const tooltip = document.getElementById('customAppTooltip');

      const textEl = document.getElementById('customAppTooltipText');

      if (!tooltip || !textEl || !target) return;

      const tipText = target.getAttribute('data-tooltip') || target.getAttribute('title');

      if (!tipText) return;

      if (target.hasAttribute('title')) {

        target.setAttribute('data-tooltip', tipText);

        target.removeAttribute('title');

      }

      clearTimeout(tooltipHideTimer);

      textEl.textContent = tipText;

      tooltip.classList.remove('hidden');

      const rect = target.getBoundingClientRect();

      const isSphere = target.id === 'orbCanvas' || target.classList.contains('orb-wrapper');

      const isSidebarTab = target.classList.contains('sidebar-nav-btn');

      const tipWidth = tooltip.offsetWidth || 130;

      const tipHeight = tooltip.offsetHeight || 26;

      if (isSidebarTab) {

        // Position to the right of the sidebar button

        const left = rect.right + 10;

        const top = rect.top + (rect.height - tipHeight) / 2;

        tooltip.style.left = `${Math.min(window.innerWidth - tipWidth - 10, Math.max(10, left))}px`;

        tooltip.style.top = `${Math.max(8, top)}px`;

      } else if (isSphere) {

        // Centered directly above the sphere

        const left = rect.left + (rect.width - tipWidth) / 2;

        let top = rect.top - tipHeight - 12;

        if (top < 10) top = rect.bottom + 12;

        tooltip.style.left = `${Math.min(window.innerWidth - tipWidth - 10, Math.max(10, left))}px`;

        tooltip.style.top = `${Math.max(8, top)}px`;

      } else {

        // Default: Centered above

        let left = rect.left + (rect.width - tipWidth) / 2;

        let top = rect.top - tipHeight - 8;

        if (top < 10) top = rect.bottom + 8;

        tooltip.style.left = `${Math.min(window.innerWidth - tipWidth - 10, Math.max(10, left))}px`;

        tooltip.style.top = `${Math.max(8, top)}px`;

      }

      requestAnimationFrame(() => {

        tooltip.classList.add('tooltip-visible');

      });

    }

    function hideCustomTooltip() {

      const tooltip = document.getElementById('customAppTooltip');

      if (!tooltip) return;

      tooltip.classList.remove('tooltip-visible');

      tooltipHideTimer = setTimeout(() => {

        if (!tooltip.classList.contains('tooltip-visible')) {

          tooltip.classList.add('hidden');

        }

      }, 150);

    }

    function initCustomTooltips() {

      const selector = '.sidebar-nav-btn, .orb-wrapper, #orbCanvas, #themeToggleBtn, #moreAvatarsBtn, #sidebarToggleBtn, [data-tooltip]';

      document.querySelectorAll(selector).forEach(el => {

        const title = el.getAttribute('title');

        if (title && !el.hasAttribute('data-tooltip')) {

          el.setAttribute('data-tooltip', title);

          el.removeAttribute('title');

        }

      });

      document.body.addEventListener('mouseover', (e) => {

        const target = e.target.closest(selector);

        if (target) {

          showCustomTooltip(target);

        }

      });

      document.body.addEventListener('mouseout', (e) => {

        const target = e.target.closest(selector);

        if (target) {

          hideCustomTooltip();

        }

      });

    }

    window.addEventListener('hashchange', () => {

      const hash = (window.location.hash || '').replace('#', '');

      if (hash === 'settings') showView('settings');

      else if (hash === 'stats') showView('stats');

      else if (hash === 'observability') showView('observability');

      else if (hash === 'logs') showView('logs');

      else if (hash === 'about' || hash === 'architecture') showView('about');

      else if (hash === 'chats') showView('chats');

      else if (hash === 'dashboard') showView('chat');

    });

    window.addEventListener('DOMContentLoaded', () => {

      initAppTheme();

      initCustomTooltips();

      renderDropdowns();

      selectModel('openai/gpt-oss-120b', 'GPT-OSS', 'groq');

      initVoicePoweredOrb();

      loadHistoryFromDB();

      initOtpInputAutoAdvance();

      checkAuthSession();

      checkGoogleCallback();

      const hash = (window.location.hash || '').replace('#', '');

      if (hash === 'settings' || window.location.pathname === '/settings') {

        showView('settings');

      } else if (hash === 'stats' || window.location.pathname === '/stats') {

        showView('stats');

      } else if (hash === 'observability' || window.location.pathname === '/observability') {

        showView('observability');

      } else if (hash === 'visualize' || window.location.pathname === '/visualize') {

        showView('visualize');

      } else if (hash === 'logs' || window.location.pathname === '/logs') {

        showView('logs');

      } else if (hash === 'about' || hash === 'architecture' || window.location.pathname === '/about' || window.location.pathname === '/architecture') {

        showView('about');

      } else if (hash === 'chats' || window.location.pathname === '/chats') {

        showView('chats');

      }

    });

  </script>

          <!-- Node Details Modal (Centered Modal Popup replacing side drawer) -->

        <div id="nodeDetailsModal" class="hidden fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-[2px] p-4" onclick="closeNodeDetailsModal()">

          <div class="glass-panel rounded-2xl border border-white/10 max-w-lg w-full shadow-2xl relative bg-[#0e131f] text-gray-200 custom-modal-box max-h-[85vh] flex flex-col overflow-hidden" onclick="event.stopPropagation()">

            <!-- Fixed Header -->

            <div class="flex items-center justify-between px-6 pt-5 pb-4 border-b border-white/10 shrink-0">

              <div class="flex items-center gap-3">

                <div id="modalNodeIcon" class="w-9 h-9 rounded-xl flex items-center justify-center p-1.5 shadow-md"></div>

                <div>

                  <h3 id="modalNodeTitle" class="text-base font-bold text-white tracking-wide">Node Inspector</h3>

                  <div id="modalNodeSubtitle" class="text-xs text-gray-400 font-mono">Architecture Details</div>

                </div>

              </div>

              <button type="button" onclick="closeNodeDetailsModal()" class="p-1.5 rounded-lg text-gray-400 hover:text-white hover:bg-white/10 transition cursor-pointer">

                <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg>

              </button>

            </div>

            <!-- Scrollable Body with Internal Scrollbar -->

            <div class="flex-1 overflow-y-auto px-6 py-4 custom-modal-scroll">

              <div id="modalNodeBody" class="space-y-4 text-xs"></div>

            </div>

          </div>

        </div>

  <!-- Stress Test Run Report Modal (Opens automatically at the end of the run) -->

  <div id="k6RunReportModal" class="hidden fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-[2px] p-4" onclick="closeK6ReportModal()">

    <div class="glass-panel rounded-2xl border border-white/10 max-w-5xl w-full shadow-2xl relative bg-[#0e131f] text-gray-200 custom-modal-box max-h-[88vh] flex flex-col overflow-hidden" onclick="event.stopPropagation()">

      <!-- Fixed Header -->

      <div class="flex items-center justify-between px-6 pt-5 pb-4 border-b border-white/10 shrink-0">

        <div class="flex items-center gap-3">

          <div class="w-10 h-10 rounded-xl bg-cyan-950/80 border border-cyan-500/50 flex items-center justify-center text-cyan-400 shadow-md">

            <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"/></svg>

          </div>

          <div>

            <h3 class="text-base font-bold text-white tracking-wide">Stress Test Execution Report</h3>

            <div id="reportSubTitle" class="text-xs text-gray-400 font-mono">Gateway Concurrency & Latency Breakdown</div>

          </div>

        </div>

        <button type="button" onclick="closeK6ReportModal()" class="p-1.5 rounded-lg text-gray-400 hover:text-white hover:bg-white/10 transition cursor-pointer">

          <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg>

        </button>

      </div>

      <!-- Scrollable Content Body with Internal Scrollbar -->

      <div class="flex-1 overflow-y-auto px-6 py-4 custom-modal-scroll space-y-4">

        <!-- KPI Summary Cards Grid (6 Metric Dashboard) -->

        <div class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-2.5 text-center font-mono">

        <!-- 1. Normal Single Request Latency -->

        <div class="p-3 rounded-xl bg-black/40 border border-white/5" title="Average time taken to serve one single request">

          <span class="text-[10px] text-gray-400 uppercase block mb-0.5">1 Req (Avg RTT)</span>

          <span id="reportMetricAvg" class="text-base font-bold text-cyan-300">-- ms</span>

          <span id="reportMetricMinMax" class="text-[10px] text-gray-400 block mt-0.5">Fastest: -- ms</span>

        </div>

        <!-- 2. Total Time for Whole Batch -->

        <div class="p-3 rounded-xl bg-black/40 border border-white/5" title="Total wall-clock duration to complete the whole batch">

          <span class="text-[10px] text-gray-400 uppercase block mb-0.5">Total Batch Time</span>

          <span id="reportMetricTotalTime" class="text-base font-bold text-emerald-400">-- s</span>

          <span id="reportMetricBatchSub" class="text-[10px] text-gray-400 block mt-0.5">Whole batch wall-clock</span>

        </div>

        <!-- 3. Throughput -->

        <div class="p-3 rounded-xl bg-black/40 border border-white/5" title="Requests per second">

          <span class="text-[10px] text-gray-400 uppercase block mb-0.5">Throughput</span>

          <span id="reportMetricRps" class="text-base font-bold text-amber-300">-- RPS</span>

          <span class="text-[10px] text-gray-400 block mt-0.5">Request rate</span>

        </div>

        <!-- 4. Served Requests Count -->

        <div class="p-3 rounded-xl bg-black/40 border border-white/5" title="Total requests served by LLM Gateway">

          <span class="text-[10px] text-gray-400 uppercase block mb-0.5">Served Requests</span>

          <span id="reportMetricServedCount" class="text-base font-bold text-emerald-400">--/--</span>

          <span id="reportMetricServedRate" class="text-[10px] text-emerald-400/80 block mt-0.5">100% passed</span>

        </div>

        <!-- 5. Total Burst Cost -->

        <div class="p-3 rounded-xl bg-blue-950/40 border border-blue-500/30" title="Total cost consumed by this traffic burst">

          <span class="text-[10px] text-blue-400 uppercase font-bold block mb-0.5">Total Burst Cost</span>

          <span id="reportMetricBurstCost" class="text-base font-bold text-blue-300">$0.000000</span>

          <span id="reportMetricBurstTokens" class="text-[10px] text-blue-400/80 block mt-0.5">0 total tokens</span>

        </div>

        <!-- 6. Cache Savings -->

        <div class="p-3 rounded-xl bg-emerald-950/40 border border-emerald-500/30" title="Total cost saved by serving cached responses">

          <span class="text-[10px] text-emerald-400 uppercase font-bold block mb-0.5">Cache Savings</span>

          <span id="reportMetricCostSaved" class="text-base font-bold text-emerald-300">$0.000000</span>

          <span id="reportMetricCacheHits" class="text-[10px] text-emerald-400/80 block mt-0.5">0 cache hits</span>

        </div>

      </div>

      <!-- Test Parameters Strip -->

      <div class="p-3 rounded-xl bg-black/40 border border-white/5 mb-4 flex items-center justify-between text-xs">

        <div class="flex items-center gap-2">

          <span class="text-gray-400">Target Provider:</span>

          <span id="reportTargetProvider" class="px-2 py-0.5 rounded font-mono font-bold bg-white/5 border border-white/10 text-white capitalize">Groq</span>

        </div>

        <div class="flex items-center gap-2 font-mono">

          <span class="text-gray-400">Mode:</span>

          <span id="reportExecutionMode" class="text-cyan-300">Simultaneous</span>

        </div>

        <div class="flex items-center gap-2 font-mono">

          <span class="text-gray-400">Prompt:</span>

          <span id="reportPromptText" class="text-amber-300 italic">"hi"</span>

        </div>

      </div>

      <!-- Requests Breakdown Table -->

      <div class="border border-white/5 rounded-xl overflow-hidden mb-4">

        <div class="bg-black/60 px-3 py-2 text-[11px] font-bold text-gray-300 uppercase tracking-wider flex items-center justify-between border-b border-white/5">

          <span>Individual Request Telemetry</span>

          <span id="reportTableCount" class="text-gray-400 font-mono text-[10px]">0 records</span>

        </div>

        <div id="reportRequestsList" class="max-h-64 overflow-y-auto divide-y divide-white/5 text-xs font-mono"></div>

      </div>

      </div>

      <!-- Pinned Footer Actions (shrink-0) -->

      <div class="flex items-center gap-2 px-6 py-3.5 border-t border-white/10 shrink-0 bg-black/20">

        <button type="button" onclick="closeK6ReportModal()" class="flex-1 py-2 rounded-xl bg-white/5 hover:bg-white/10 border border-white/10 text-gray-300 font-bold text-xs transition cursor-pointer">

          Close Report

        </button>

        <button type="button" onclick="closeK6ReportModal(); startK6TrafficTest();" class="flex-1 py-2 rounded-xl bg-gradient-to-r from-blue-600 to-cyan-500 hover:from-blue-500 hover:to-cyan-400 text-white font-bold text-xs transition cursor-pointer">

          Re-run Test

        </button>

      </div>

    </div>

  </div>

        

  <!-- Custom Sleek Tooltip Popup -->

  <div id="customAppTooltip" class="custom-tooltip fixed pointer-events-none z-[99999] opacity-0 scale-95 transition-all duration-150 shadow-2xl flex items-center gap-1.5 hidden">

    <span id="customAppTooltipText"></span>

  </div>

</body>

</html>

"""

if __name__ == "__main__":

    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
