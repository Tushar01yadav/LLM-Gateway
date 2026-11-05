# Claims and how to reproduce them

Every claim used in the README or in outreach/LinkedIn posts must have a row here.
Rows are verified against the current commit on the date shown; re-run the command
before reusing a claim. Environment: Windows 10 x64, Python 3.10.11, 8 logical CPUs,
Redis 7.4 in Docker (`llm-gateway-redis`) where noted. Absolute latency numbers are
host-bound; see `docs/benchmarks/phase4-load-before-after.md` for measured host baselines.

## Verified

| Claim | Exact command to reproduce | Last verified result | Date |
|---|---|---|---|
| Full test suite passes | `python -m pytest -q` | 115 passed locally (includes 2 live-provider tests that require real keys); same suite on a clean copy with no `.env`/keys: 113 passed, 2 skipped. Transcript: `scratch/full-pytest-quickwins.txt` | 2026-11-05 |
| Guardrail baseline blocks the tested obfuscated injections, with 2 documented bypass classes | `python scripts/guardrail_matrix.py` | `blocked=10 bypass=2`; PII hits `['email','phone']`, `input_unchanged=True` | 2026-11-05 |
| Guardrail normalization handles NFKC, zero-width, control chars, case-folding | `python -m pytest tests/test_guardrails_normalization.py -q` | 15 passed | 2026-11-05 |
| Hard secrets are redacted; emails/phones are flagged, never rewritten | `python -m pytest tests/test_owasp_guardrails.py tests/test_guardrails_normalization.py -q` | all passed; anonymous chat is asserted as the intentional guest path and invalid/revoked keys still return 401 | 2026-11-05 |
| Async jobs survive a hard worker kill and complete exactly once | `python scripts/verify_jobs_phase4.py` | 30/30 checks; default lease 15 s / heartbeat 5 s; TerminateProcess kill -> job done on attempt 2 in 20.45 s (was 37.53 s at 30 s lease); 1 cost row; no leftover queue/lease/DLQ state | 2026-11-05 |
| Job lease and heartbeat are configurable; a slow-but-alive worker is not duplicated | `python scripts/verify_jobs_phase4.py` | worker with `GATEWAY_JOB_LEASE_SECONDS=6` / `GATEWAY_JOB_HEARTBEAT_SECONDS=2`: lease renewed past the original 6 s expiry while the 12 s job ran, job done with attempts=1, one cost row, no duplicate delivery state | 2026-11-05 |
| DLQ retries up to `max_attempts`, replay resets the attempt budget, `clear_dlq` clears DB + Redis | `python scripts/verify_jobs_phase4.py` | replay accepted, fresh budget exhausted at attempts=3, `db_dlq_rows=0`, `redis_dlq={}` | 2026-11-05 |
| Real server-side p50/p95/p99 (nearest-rank, rolling window, not AVG) are exposed | `python -m pytest tests/test_percentiles.py tests/test_percentiles_api.py -q` | 5 passed; `/api/stats` returned p50/p95/p99 within `p50 <= p95 <= p99 <= max`, both on mock and on real Groq/OpenRouter rows | 2026-11-05 |
| Percentiles are wired to the stats card and visualizer node inspector, with real provider samples | `python scripts/` server on :8060 + 4 real `fast` calls, then open `/visualize` and inspect Groq/OpenRouter | live DOM: Groq inspector `p50 472.12ms · p95 769.4ms · p99 769.4ms`; OpenRouter inspector `p50 4186.75ms · p95 4502.38ms · p99 4502.38ms`; stats card `p50 700.54ms · p95 5540.84ms · p99 16121.59ms`, `1696 samples in last 60m`. Evidence: `docs/benchmarks/phase4-real-percentiles.json` | 2026-11-05 |
| Concurrency control caps provider in-flight and sheds load with 429 + Retry-After | `python scripts/benchmark_load.py --repeats 3` | AFTER median: 84 served / 36 shed, peak mock in-flight 10; BEFORE: 120 served / 0 shed, peak 0 observable | 2026-11-05 |
| Load test is honest about host limits and reports BEFORE vs AFTER medians | `python scripts/benchmark_load.py --repeats 3` | 3 interleaved repeats; Redis RTT 2.07 ms, `/health` 12.41 ms, 60-concurrent `/health` 529.2 ms; medians in `docs/benchmarks/phase4-load-before-after.md` | 2026-11-05 |
| Chaos scenarios are measured, including degradations | `python scripts/chaos_load.py` | provider death: 18.9 % success during fault; latency: 100 % success but ~79 % throughput collapse; Redis kill: 100 % success, 8 ping failures; worker kill: 12/12 jobs, 15 HTTP conn errors. Details in `docs/benchmarks/phase4-chaos.md` | 2026-11-05 |
| A configured fallback chain serves traffic through a provider death | `python scripts/chaos_fallback.py` | mock -> mock2: 100 % success during fault vs 18.9 % without fallback; 80 % of during-fault requests served by the fallback; first fallback response at +2.68 s; primary breaker OPEN at +2.24 s; latency-5000 ms case correctly does not fail over (100 % success, 0 % fallback) | 2026-11-05 |
| Real Groq -> OpenRouter failover works with real adapters | `python scripts/chaos_fallback.py` (real-provider phase) | 6/6 served: 3 real Groq calls, then 3 with Groq injected as 503 all served by real OpenRouter with `fallback_used=true` (first injected call 4.60 s, later 0.59 s once the breaker opened) | 2026-11-05 |
| Overload benchmark: control converts provider 5xx into explicit 429 shedding | `python scripts/benchmark_load.py --profiles overload --repeats 3` | BEFORE (no control): 41.0 % served, 59.0 % 5xx, 0 % shed, served p99 7.44 s. AFTER: 35.0 % served, 0 % 5xx, 65.0 % shed, served p99 8.32 s. Details in `docs/benchmarks/phase4-load-overload-before-after.md` | 2026-11-05 |
| Startup refuses an unsafe proxy configuration in production | `APP_ENV=production TRUSTED_PROXY_COUNT=1 uvicorn app.main:app` (no flag) | exits non-zero with `RuntimeError: Invalid deploy configuration` naming `--no-proxy-headers`; same command with `--no-proxy-headers` starts and `/health` returns 200 | 2026-11-05 |
| Google OAuth uses state + PKCE and stores tokens encrypted | `python -m pytest tests/test_release_hygiene.py -q` | 7 passed: state single-use, expired/unknown state rejected, auth URL carries `state`, `code_challenge`, `code_challenge_method=S256`, callback rejects unknown state with 400, tokens stored as `enc:v1:` and decrypt on read | 2026-11-05 |
| Session cookies carry `Secure` in production mode | `python -m pytest tests/test_release_hygiene.py -q` | login response `Set-Cookie` includes `Secure` when `COOKIE_SECURE` is enabled (production); development leaves it off for http://localhost | 2026-11-05 |
| Docker image builds and runs clean | `docker build -t llm-gateway .` then `docker run -e GATEWAY_ADMIN_TOKEN=... llm-gateway` | image builds; container reports `healthy` and `/health` returns 200; runs as uid 10001; `.dockerignore` excludes `.env`, `*.db`, `client_secret*.json`, `scratch/`, `__pycache__` | 2026-11-05 |
| Dependencies are pinned; repository ships LICENSE | `cat requirements.txt requirements-dev.txt LICENSE` | runtime and dev deps pinned to exact installed versions; MIT LICENSE present | 2026-11-05 |
| `gateway.db` and OAuth client secrets are not in git | `git ls-files | grep -iE "\.db|client_secret|\.env$"` and `git log --all -- gateway.db` | no tracked matches and no history entries; `python scripts/reset_gateway_db.py --yes` clears the local copy before a release | 2026-11-05 |
| Redis-backed rate limiter survives Redis outage per policy | `python scripts/chaos_load.py` (redis_kill scenario) | 56/56 requests served during outage; 8 failed PINGs, restored at +15.2 s | 2026-11-05 |
| Async jobs / DLQ endpoints work over HTTP | `python -m pytest tests/test_async_jobs.py -q` | 5 passed | 2026-11-05 |
| Phase 3 fixes hold (scoped cache, atomic limiter, job idempotency, coalescer) | `python -m pytest tests/test_phase3.py -q` | 18 passed | 2026-11-05 |
| Dashboard JS parses and visualizer route renders | `python -m pytest tests/test_ui_js.py tests/test_visualize.py -q` | 10 passed (`node --check` on extracted dashboard JS) | 2026-11-05 |
| Identity/tenancy and takeover fixes hold (key hashing, owner-scoped jobs, trusted proxy) | `cd scratch/audit-run && python ../phase3_probe.py` (imports phase2, which includes phase1 probes) | 129/129 live probes (85 Phase 1+2, 44 Phase 3), exit 0 | 2026-11-05 |
| Dashboard scripts are pinned and SRI-enforced | `python -m pytest tests/test_dashboard_supply_chain.py -q` | 3 passed; no remote `<script src>`; all three vendored files match their `sha384` integrity attributes | 2026-11-05 |
| Dashboard loads under SRI in a real browser | Open `http://127.0.0.1:<port>/visualize` (vendored assets) and inspect console | `window.tailwind`, `window.marked`, `window.DOMPurify` present; no integrity/CORS errors; only expected 401 for `/api/chats` (guest) | 2026-11-05 |
| Chat responses expose public metadata headers only | `curl -D - -X POST .../v1/chat -d '{"model":"mock",...}'` | `X-Request-Id`, `X-Provider`, `X-Model`, `X-Fallback-Used`, `X-Latency-Ms`, `X-Cost-USD` present; no internal headers exposed (e.g. `X-Latency-Ms: 1533.61`, `X-Cost-USD: 0.00000065`) | 2026-11-05 |
| Visualizer chaos panel works in a live browser (kill, latency, Redis down/up) | Start server with `GATEWAY_ALLOW_CHAOS=1` and open `/visualize` | kill mock -> chat `502` with `server_error_503 x3`; unbreak -> `200`; 1500 ms injection -> chat 1.66 s / `x-latency-ms 1533.61`; reset -> 0.65 s / `525.31` ms; Redis down -> container `exited` while `/health` and chat stay `200`; Redis up -> container `Up`, chat 0.65 s. Unauthenticated chaos calls return 403. Evidence: `docs/benchmarks/quickwins-chaos-ui.json` | 2026-11-05 |
| `docker compose up --build` runs a no-key demo from a clean copy | Copy repo without `.env`/`gateway.db`/`scratch`, then `docker compose up --build --exit-code-from loadgen` | 12/12 `[PASS]`, exit 0; `mock_failover` during injected mock outage served by `mock2` with `X-Fallback-Used: true`; log: `scratch/compose-demo-run.log` | 2026-11-05 |
| CI job steps pass on a clean copy (pytest + `node --check`) | Same commands as `.github/workflows/ci.yml` run in a copy without `.env`: explicit `node --check` on 3 inline script blocks, then `python -m pytest -q` with Redis db 15 | `node --check passed for 3 block(s)`; `113 passed, 2 skipped` (live-provider tests skip without keys) | 2026-11-05 |
| `docs/demo.md` commands produce the stated outputs | Follow `docs/demo.md` against a running server | injection prompt -> `400 Prompt injection pattern detected`; `POST /v1/jobs` -> `{"id":"job_...","status":"queued"}`; poll -> `"status":"done"` with `"text":"Mock response for stress testing"` | 2026-11-05 |

## Not yet verified

| Claim | Why unverified | What would verify it |
|---|---|---|
| Live failover across all three providers with a real upstream outage | Groq -> OpenRouter is verified with real keys (see Verified) but the Groq failure was injected via the admin toggle, and Gemini was not exercised | Take a real Groq/OpenRouter/Gemini outage (or revoke a key) and observe failover without the injection toggle |
| Provider adapters match each vendor's current API | Groq (`openai/gpt-oss-120b`) and OpenRouter (`meta-llama/llama-3.1-8b-instruct`) returned 200 with real keys; Gemini was not called | One real request per provider (including Gemini), assert 200 + token usage |
| "50–100+ simultaneous requests" from the visualizer Burst control | The UI control exists and its JS parses, but the exact burst behavior was not load-verified in a browser this round | Browser test: trigger burst, observe shed/served counts in `/api/concurrency/status` |
| Prometheus metrics export / OpenTelemetry tracing | Listed as **Planned** in the README, not implemented | Implement, then rerun |
| Multi-instance deployment / horizontal scaling of workers | Only 2 local uvicorn workers on one host were tested | Deploy behind a load balancer and rerun job/chaos proofs |
| Cross-language or non-ASCII prompt-injection detection beyond the documented cases | Known limit of the heuristic baseline (homoglyphs, letter-spacing, encodings) | Add a detector and extend `scripts/guardrail_matrix.py` |
| LinkedIn-specific performance/cost numbers | Not part of the repo's measured evidence | Cite only numbers from `docs/benchmarks/` or add a row here |
| Real Google OAuth consent + callback | State/PKCE and encryption are unit-verified against local endpoints; no live Google consent screen was completed in this environment | Complete a real Google sign-in, confirm the callback exchanges the code with `code_verifier`, and check `oauth_tokens` contains `enc:v1:` ciphertext |
| GitHub Actions badge is green on GitHub | The workflow exists and its exact steps pass in a clean local copy, but the repo has not been pushed since the workflow was added, so no GitHub-hosted run exists yet | Push to `main`, open the Actions run for `ci.yml`, confirm both steps are green and the badge renders |

## Previously stale auth tests (reconciled)

- `tests/test_owasp_guardrails.py::test_owasp_auth_bypass_prevention`
- `tests/test_security_wall.py::test_chat_pipeline_auth_rejections`

Both originally asserted 401 for a missing credential. The product decision in
`e74cf56` allows instant anonymous chat under a per-IP guest quota, so they now
assert 200 for the guest path and keep the 401 assertions for invalid and revoked
keys. This matches the code rather than hiding the mismatch.

## Fresh-clone test setup

- `tests/conftest.py` calls `init_db()` once. `ASGITransport` does not run the
  FastAPI lifespan, which is where the SQLite schema is created; without this,
  a clean clone failed 13 tests with `sqlite3.OperationalError: no such table:
  sessions`. The schema is the same one the server creates at startup.
- The two `test_live_*` tests in `tests/test_reliability.py` make real provider
  calls. They run when the relevant keys are present and skip with an explicit
  reason when they are not (e.g. CI). They are skipped, not deleted or weakened.
