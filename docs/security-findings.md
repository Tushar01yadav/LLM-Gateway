# Security Iteration & OWASP Top 10 for LLM Applications

This document details all security findings discovered during the security iteration of the LLM Gateway, mapping each finding to the **OWASP Top 10 for LLM Applications**, analyzing root causes, documenting applied remediation fixes, and cross-referencing automated regression tests.

---

## 🛡️ Summary Matrix

| Vulnerability / Risk | OWASP Category | Root Cause | Fix Applied | Regression Test |
| :--- | :--- | :--- | :--- | :--- |
| **Prompt Injection & DAN Jailbreaks** | **LLM01: Prompt Injection** | Lack of input pattern scanning allowed adversarial system prompt override strings and DAN mode payloads. | Implemented `INJECTION_PATTERNS` regex scanner in `GuardrailsEngine.inspect_input()` returning 400 Bad Request with blocked detail. | `tests/test_owasp_guardrails.py::test_owasp_llm01_prompt_injection` |
| **Sensitive Data & API Key Leakage** | **LLM02: Sensitive Information Disclosure** & **LLM05: Improper Output Handling** | Model completions or errors could echo client API keys, provider env keys, SSNs, credit cards, or internal secrets. | Implemented `GuardrailsEngine.inspect_output()` scanning `SECRET_PATTERNS` and `os.environ` secrets, redacting all matches before client response delivery. | `tests/test_owasp_guardrails.py::test_owasp_llm02_secret_and_key_leakage_redaction` |
| **SSRF via URL Input** | **LLM06: Excessive Agency / SSRF** | Prompts containing URLs referencing cloud metadata or internal private subnets could trigger unauthorized internal queries. | Added `check_ssrf_safety()` to inspect all embedded URLs, disallowing non-HTTP schemes, loopback hostnames, cloud metadata (`169.254.169.254`), and RFC 1918 private ranges. | `tests/test_owasp_guardrails.py::test_owasp_llm06_ssrf_prevention` |
| **System Prompt Leakage** | **LLM07: System Prompt Leakage** | Internal system directives could be extracted through indirect adversarial prompting. | Added system prompt leakage scanner to `inspect_output()` reading confidential directives from `models.json` and redacting them with `[REDACTED_SYSTEM_PROMPT]`. | `tests/test_owasp_guardrails.py::test_owasp_llm07_system_prompt_leakage_redaction` |
| **Oversized Payloads & Context Flooding** | **LLM10: Unbounded Consumption** | Absence of input size and token count guardrails exposed system to memory exhaustion and provider context window crash. | Enforced strict 1MB `Content-Length` / payload cap (HTTP 413) and token count estimator threshold (HTTP 400). | `tests/test_owasp_guardrails.py::test_owasp_llm10_oversized_payload_rejection`, `test_owasp_llm10_context_token_limit` |
| **Rate Limit Bypass on Redis Outage** | **LLM10: Unbounded Consumption** | Redis connectivity failure caused token bucket check to fail open with unthrottled throughput. | Added local in-memory token bucket fallback (`self._mem_buckets`) in `RedisTokenBucketRateLimiter` so rate limiting persists during Redis outages without bypassing. | `tests/test_owasp_guardrails.py::test_owasp_llm10_rate_limit_bypass_prevention` |
| **Daily Budget Depletion** | **LLM10: Unbounded Consumption** | Long-running queries could incur unbounded dollar costs without automated shut-off. | Implemented `BudgetGuard` in `app/cost.py` tracking cumulative daily spend per key and terminating over-budget requests with HTTP 429. | `tests/test_owasp_guardrails.py::test_owasp_llm10_unbounded_consumption_budget` |
| **Audit Trail Secret Persistence** | **LLM02: Sensitive Information Disclosure** | Audit event summaries stored user prompt fragments that could retain raw API keys or PII in `gateway.db`. | Integrated `SECRET_PATTERNS` redaction filter into `record_audit_sync` before SQLite insert. | `tests/test_owasp_guardrails.py::test_owasp_audit_log_tracking` |

---

## 🔍 Detailed Finding Reports

### Finding 1: Unbounded Consumption via Rate-Limit Bypass During Cache/Redis Outage
* **OWASP Category:** LLM10: Unbounded Consumption
* **Severity:** High
* **Description:** When the Redis rate limiting instance was offline or experienced a socket timeout, the token bucket rate limiter logged a warning and defaulted to allowing the request through when configured as `fail_closed=False`. Attackers could exploit transient Redis disconnects or IP spoofing headers (`X-Forwarded-For`) to flood the gateway without restriction.
* **Root Cause:** No local in-memory token bucket fallback was present in `RedisTokenBucketRateLimiter`.
* **Fix Applied:** Built an in-memory token-bucket cache (`self._mem_buckets`) tracking `tokens` and `last_time` per key ID. When Redis is unavailable, the rate limiter falls back to local tracking, enforcing quota RPM and burst capacity without dropping security invariants.
* **Verification:** `pytest tests/test_owasp_guardrails.py::test_owasp_llm10_rate_limit_bypass_prevention` passes with 429 response and `Retry-After` header.

---

### Finding 2: System Prompt & Provider Secret Leakage in Model Completions
* **OWASP Category:** LLM02: Sensitive Information Disclosure & LLM07: System Prompt Leakage
* **Severity:** Critical
* **Description:** Prompt injection attacks attempting to coerce models into reciting system prompts or environment API keys could successfully leak sensitive directives to client responses.
* **Root Cause:** Output processing previously only checked generic API key regexes.
* **Fix Applied:**
  1. Configured system prompt strings in `models.json` under `"guardrails.system_prompts"`.
  2. Enhanced `GuardrailsEngine.inspect_output()` to scan output completions for matching system prompt phrases, replacing them with `[REDACTED_SYSTEM_PROMPT]`.
  3. Scanned running environment variables for provider keys (`GROQ_API_KEY`, etc.), automatically redacting any matched strings with `[REDACTED_API_KEY]`.
* **Verification:** `pytest tests/test_owasp_guardrails.py::test_owasp_llm07_system_prompt_leakage_redaction` and `test_owasp_llm02_secret_and_key_leakage_redaction` pass.

---

### Finding 3: SSRF via Loopback, Private Subnets, and Cloud Metadata URLs
* **OWASP Category:** LLM06: Excessive Agency / SSRF
* **Severity:** High
* **Description:** Input prompts containing URLs could direct agents or tools to fetch internal network assets or AWS/GCP instance metadata (`http://169.254.169.254/latest/meta-data/`).
* **Root Cause:** Input messages were passed to routing without URL safety inspection.
* **Fix Applied:** Integrated `check_ssrf_safety()` into `inspect_input()`. It extracts URLs via regex, validates scheme whitelist (`http`, `https`), verifies destination host is not localhost or metadata domain, and resolves IP addresses to block private (`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`), loopback (`127.0.0.0/8`), or link-local (`169.254.0.0/16`) traffic.
* **Verification:** `pytest tests/test_owasp_guardrails.py::test_owasp_llm06_ssrf_prevention` passes.

---

### Finding 4: Secret Persistence in SQLite Audit Logging Table
* **OWASP Category:** LLM02: Sensitive Information Disclosure
* **Severity:** Medium
* **Description:** When an input was blocked or logged to `audit_logs`, the `sanitized_summary` recorded the raw incoming exception or request snippet. If an attacker sent a prompt containing a real secret or API key, that key would be saved in plain text in `gateway.db`.
* **Root Cause:** Lack of secondary sanitization prior to database insert.
* **Fix Applied:** `record_audit_sync` now passes `sanitized_summary` through `SECRET_PATTERNS` regex replacements, ensuring any tokens, credit cards, or SSNs are converted to `[REDACTED_*]` before execution of the `INSERT` query.
* **Verification:** `pytest tests/test_owasp_guardrails.py::test_owasp_audit_log_tracking` passes.
