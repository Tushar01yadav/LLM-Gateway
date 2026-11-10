import json
import logging
import os
import re
from app.db import sqlite3
import time
import unicodedata
import urllib.parse
from ipaddress import ip_address
from typing import Any, Dict, List, Optional, Tuple

from fastapi import HTTPException, status

logger = logging.getLogger("llm_gateway.guardrails")
logger.setLevel(logging.INFO)
if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setFormatter(
        logging.Formatter("[%(asctime)s] [%(levelname)s] [GUARDRAILS] %(message)s", datefmt="%H:%M:%S")
    )
    logger.addHandler(ch)

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "gateway.db")
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "models.json")


def init_audit_db():
    conn = sqlite3.connect(DB_PATH, timeout=15.0)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS audit_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp REAL NOT NULL,
            key_id TEXT,
            key_name TEXT,
            endpoint TEXT NOT NULL,
            outcome TEXT NOT NULL,
            guardrail_hits TEXT,
            sanitized_summary TEXT,
            ip_address TEXT
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit_logs(timestamp);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_audit_outcome ON audit_logs(outcome);")
    conn.commit()
    conn.close()


init_audit_db()


# ---------------------------------------------------------------------------
# 1. OWASP LLM01: Prompt Injection Patterns
# ---------------------------------------------------------------------------
INJECTION_PATTERNS = [
    re.compile(r"ignore\s+(all\s+)?(previous|prior)\s+(instructions|directives|prompts)", re.IGNORECASE),
    re.compile(r"disregard\s+(all\s+)?(previous|prior)\s+(instructions|directives|prompts)", re.IGNORECASE),
    re.compile(r"bypass\s+(all\s+)?(safety|security|content)\s+(filters|rules|guidelines)", re.IGNORECASE),
    re.compile(r"system\s+override\s*:\s*", re.IGNORECASE),
    re.compile(r"you\s+are\s+now\s+in\s+['\"`]?DAN['\"`]?\s+mode", re.IGNORECASE),
    re.compile(r"jailbreak\s+(mode|prompt)", re.IGNORECASE),
    re.compile(r"act\s+as\s+an\s+unrestricted\s+(ai|model|assistant)", re.IGNORECASE),
    re.compile(r"<\|im_start\|>system", re.IGNORECASE),
    re.compile(r"\[SYSTEM\s+PROMPT\s*:\s*", re.IGNORECASE),
    re.compile(r"reveal\s+(your\s+)?(system\s+prompt|initial\s+instructions)", re.IGNORECASE),
]

# ---------------------------------------------------------------------------
# 2. OWASP LLM02: Sensitive Data & Secret Detection Patterns
# ---------------------------------------------------------------------------
# Hard secrets are redacted when policy allows. Personal identifiers (email,
# phone) are only flagged or blocked; they are never silently rewritten.
SECRET_PATTERNS = {
    "api_key_generic": re.compile(r"(?:sk-[a-zA-Z0-9]{24,}|gsk_[a-zA-Z0-9]{20,}|AIzaSy[0-9A-Za-z-_]{33}|gw_live_[0-9a-zA-Z_-]{20,})"),
    "ssn": re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
    "credit_card": re.compile(r"\b(?:\d{4}[ -]?){3}\d{4}\b"),
    "jwt_token": re.compile(r"\beyJ[a-zA-Z0-9-_]+\.[a-zA-Z0-9-_]+\.[a-zA-Z0-9-_]+\b"),
    "private_key": re.compile(r"-----BEGIN (?:RSA |EC )?PRIVATE KEY-----"),
}

PII_PATTERNS = {
    "email": re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"),
    "phone": re.compile(r"\b(?:\+?1[-. ]?)?\(?([0-9]{3})\)?[-. ]?([0-9]{3})[-. ]?([0-9]{4})\b"),
}

# Zero-width and control characters are invisible in rendered text but break
# naive substring matching, so they are removed before scanning.
_INVISIBLE_RE = re.compile(r"[\u200b-\u200f\u202a-\u202e\u2060-\u2064\ufeff]")
_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


def normalize_for_scan(text: str) -> str:
    """NFKC-normalize, strip invisible/control chars, case-fold for matching."""
    text = unicodedata.normalize("NFKC", text or "")
    text = _INVISIBLE_RE.sub("", text)
    text = _CONTROL_RE.sub("", text)
    return text.casefold()

# ---------------------------------------------------------------------------
# 3. OWASP LLM06: SSRF URL Scanner
# ---------------------------------------------------------------------------
PRIVATE_IP_RANGES = [
    "127.0.0.0/8",
    "10.0.0.0/8",
    "172.16.0.0/12",
    "192.168.0.0/16",
    "169.254.0.0/16",
    "::1/128",
]


def check_ssrf_safety(url_candidate: str) -> Tuple[bool, str]:
    """Scans URL to prevent SSRF against localhost, link-local, and private ranges."""
    try:
        parsed = urllib.parse.urlparse(url_candidate)
        scheme = parsed.scheme.lower()
        if scheme not in ("http", "https"):
            return False, f"Prohibited URL scheme '{scheme}' (SSRF guardrail)"

        host = parsed.hostname or ""
        if host.lower() in ("localhost", "127.0.0.1", "::1", "169.254.169.254", "metadata.google.internal"):
            return False, f"Prohibited loopback/metadata host '{host}' (SSRF guardrail)"

        try:
            ip = ip_address(host)
            if ip.is_private or ip.is_loopback or ip.is_link_local:
                return False, f"Prohibited private/loopback IP destination '{host}' (SSRF guardrail)"
        except ValueError:
            pass  # Hostname (not raw IP)

        return True, ""
    except Exception as e:
        return False, f"Invalid URL for safety check ({e})"


def load_guardrails_config() -> Dict[str, Any]:
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data.get("guardrails", {})
        except Exception:
            pass
    return {}


class GuardrailsEngine:
    def __init__(self):
        cfg = load_guardrails_config()
        self.enabled = cfg.get("enabled", True)
        self.max_body_bytes = cfg.get("max_payload_bytes", 1024 * 1024)  # 1 MB
        self.max_tokens = cfg.get("max_tokens", 32000)
        self.block_on_pii = cfg.get("block_on_pii", False)
        self.system_prompts: List[str] = cfg.get("system_prompts", [])

    def inspect_input(
        self,
        messages: List[Dict[str, str]],
        raw_body_len: Optional[int] = None,
        block_on_pii: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """
        Validates input against OWASP Top 10 LLM risks:
        - Payload size (LLM10)
        - Prompt injection (LLM01)
        - SSRF in URLs (LLM06)
        - Secrets and PII detection (LLM02)
        """
        if not self.enabled:
            return {"safe": True, "hits": [], "sanitized_messages": messages}

        hits = []
        should_block_pii = self.block_on_pii if block_on_pii is None else block_on_pii

        # 1. Payload Size Cap (LLM10)
        if raw_body_len and raw_body_len > self.max_body_bytes:
            hits.append({"type": "oversized_payload", "detail": f"Payload length {raw_body_len} exceeds max 1MB limit"})
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail=f"Payload size {raw_body_len} bytes exceeds maximum allowed limit of 1MB (OWASP LLM10).",
            )

        total_text_len = 0
        sanitized_messages = []

        for msg in messages:
            content = str(msg.get("content", ""))
            total_text_len += len(content)
            scan_content = normalize_for_scan(content)

            # Check prompt injection patterns (LLM01) against the normalized copy.
            for pat in INJECTION_PATTERNS:
                match = pat.search(scan_content)
                if match:
                    match_snippet = match.group(0)
                    hits.append({"type": "prompt_injection", "pattern": match_snippet, "owasp": "LLM01"})
                    logger.warning(f"[GUARDRAILS] Blocked prompt injection pattern: '{match_snippet}'")
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Security guardrail blocked request: Prompt injection pattern detected ('{match_snippet}') (OWASP LLM01).",
                    )

            # Check SSRF URLs (LLM06) after normalization so split/sneaky URLs
            # are scanned in the form a model would actually read.
            urls = re.findall(r'https?://[^\s<>"]+|www\.[^\s<>"]+', scan_content)
            for u in urls:
                safe, reason = check_ssrf_safety(u)
                if not safe:
                    hits.append({"type": "ssrf_violation", "url": u, "reason": reason, "owasp": "LLM06"})
                    logger.warning(f"[GUARDRAILS] Blocked SSRF attempt: {reason}")
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Security guardrail blocked request: {reason} (OWASP LLM06).",
                    )

            # Hard secrets are redacted. Personal identifiers are only flagged
            # (or blocked per block_on_pii policy); we never rewrite them.
            cleaned_content = content
            redactable = _CONTROL_RE.sub("", _INVISIBLE_RE.sub("", content))
            redacted = False
            for sec_name, sec_regex in SECRET_PATTERNS.items():
                if sec_regex.search(scan_content):
                    hits.append({"type": "sensitive_data", "kind": sec_name, "owasp": "LLM02"})
                    if should_block_pii:
                        raise HTTPException(
                            status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f"Security guardrail blocked request: Sensitive data/secret detected ({sec_name}) (OWASP LLM02).",
                        )
                    redactable = sec_regex.sub(f"[REDACTED_{sec_name.upper()}]", redactable)
                    redacted = True

            for pii_name, pii_regex in PII_PATTERNS.items():
                if pii_regex.search(scan_content):
                    hits.append({
                        "type": "sensitive_data",
                        "kind": pii_name,
                        "action": "flagged",
                        "owasp": "LLM02",
                    })
                    if should_block_pii:
                        raise HTTPException(
                            status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f"Security guardrail blocked request: Sensitive data detected ({pii_name}) (OWASP LLM02).",
                        )

            if redacted:
                cleaned_content = redactable
            sanitized_messages.append({"role": msg.get("role", "user"), "content": cleaned_content})

        # Token limit check (estimated 4 chars per token)
        est_tokens = total_text_len // 4
        if est_tokens > self.max_tokens:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Token estimate ({est_tokens}) exceeds maximum context token limit of {self.max_tokens} (OWASP LLM10).",
            )

        return {
            "safe": True,
            "hits": hits,
            "sanitized_messages": sanitized_messages,
        }

    def inspect_output(
        self,
        text: str,
        custom_system_prompts: Optional[List[str]] = None,
    ) -> Tuple[str, List[Dict[str, Any]]]:
        """
        OWASP LLM02, LLM05, & LLM07: Output Guardrail
        Scans model responses before client delivery to redact:
        1. Leaked keys, secrets, tokens, PII (LLM02/LLM05)
        2. System environment API keys
        3. Leaked internal system prompt instructions (LLM07)
        """
        if not self.enabled:
            return text, []

        output_hits = []
        sanitized = text

        # 1. Regex patterns for known secret formats
        for sec_name, sec_regex in SECRET_PATTERNS.items():
            if sec_regex.search(sanitized):
                output_hits.append({"type": "output_leak_prevented", "kind": sec_name, "owasp": "LLM02"})
                sanitized = sec_regex.sub(f"[REDACTED_{sec_name.upper()}]", sanitized)

        # 2. Personal identifiers are flagged for audit, never rewritten.
        scan_sanitized = normalize_for_scan(sanitized)
        for pii_name, pii_regex in PII_PATTERNS.items():
            if pii_regex.search(scan_sanitized):
                output_hits.append({
                    "type": "output_pii_flagged",
                    "kind": pii_name,
                    "action": "flagged",
                    "owasp": "LLM02",
                })

        # 3. Check environment variables to ensure zero provider key leakage
        for env_k, env_v in os.environ.items():
            if ("KEY" in env_k or "TOKEN" in env_k or "SECRET" in env_k) and len(env_v) >= 12:
                if env_v in sanitized:
                    output_hits.append({"type": "output_env_secret_prevented", "kind": env_k, "owasp": "LLM02"})
                    sanitized = sanitized.replace(env_v, "[REDACTED_API_KEY]")

        # 3. System Prompt Leakage Protection (LLM07)
        known_sys_prompts = list(self.system_prompts)
        if custom_system_prompts:
            known_sys_prompts.extend(custom_system_prompts)

        for sys_p in known_sys_prompts:
            if sys_p and len(sys_p) > 10 and sys_p in sanitized:
                output_hits.append({"type": "system_prompt_leak_prevented", "snippet": sys_p[:30], "owasp": "LLM07"})
                sanitized = sanitized.replace(sys_p, "[REDACTED_SYSTEM_PROMPT]")

        return sanitized, output_hits


def record_audit_sync(
    key_id: str,
    key_name: str,
    endpoint: str,
    outcome: str,
    hits: List[Dict[str, Any]],
    sanitized_summary: str,
    ip_address: str,
):
    # Scrub any secrets from sanitized_summary before writing to SQLite
    clean_summary = sanitized_summary
    for sec_name, sec_regex in SECRET_PATTERNS.items():
        clean_summary = sec_regex.sub(f"[REDACTED_{sec_name.upper()}]", clean_summary)

    conn = sqlite3.connect(DB_PATH, timeout=15.0)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO audit_logs (timestamp, key_id, key_name, endpoint, outcome, guardrail_hits, sanitized_summary, ip_address)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (time.time(), str(key_id), key_name, endpoint, outcome, json.dumps(hits), clean_summary[:200], ip_address))
    conn.commit()
    conn.close()


class AuditLogger:
    @staticmethod
    async def log_event(
        key_id: str,
        key_name: str,
        endpoint: str,
        outcome: str,
        hits: Optional[List[Dict[str, Any]]] = None,
        summary: str = "",
        ip_address: str = "127.0.0.1",
    ):
        import asyncio
        await asyncio.to_thread(
            record_audit_sync,
            str(key_id),
            key_name,
            endpoint,
            outcome,
            hits or [],
            summary,
            ip_address,
        )

    @staticmethod
    def list_recent_logs(limit: int = 50) -> List[Dict[str, Any]]:
        conn = sqlite3.connect(DB_PATH, timeout=15.0)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, timestamp, key_id, key_name, endpoint, outcome, guardrail_hits, sanitized_summary, ip_address
            FROM audit_logs ORDER BY id DESC LIMIT ?
        """, (limit,))
        rows = cursor.fetchall()
        conn.close()

        res = []
        for r in rows:
            res.append({
                "id": r[0],
                "timestamp": r[1],
                "key_id": r[2],
                "key_name": r[3],
                "endpoint": r[4],
                "outcome": r[5],
                "hits": json.loads(r[6]) if r[6] else [],
                "summary": r[7],
                "ip": r[8],
            })
        return res


GUARDRAILS = GuardrailsEngine()
AUDIT_LOGGER = AuditLogger()
