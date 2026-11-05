# Phase 4 job recovery proof (configurable lease)

- Generated: 2026-11-05T06:50:15.128658+00:00
- Redis db 14, 2 workers, Python 3.10.11, win32
- Defaults: lease 15.0 s, heartbeat 5.0 s
- Hard-kill recovery (kill -> job done): 20.45 s (previous 30 s-lease run: 37.53 s)
- Slow-but-alive override: lease 6 s, heartbeat 2 s

**30/30 checks passed.**

| Check | Result | Evidence |
|---|---|---|
| two workers started | PASS | `{"pids": [15272, 20484], "redis_db": 14}` |
| default lease/heartbeat are 15s/5s | PASS | `{"lease_seconds": 15.0, "heartbeat_seconds": 5.0}` |
| job picked up and leased | PASS | `{"job_id": "job_fc70647777bf", "lease": "{\"owner\": \"COGLAP021:15272:4af4ee0c\", \"expires\": 1793861353.31401}"}` |
| lease owner is one of the two workers | PASS | `{"victim_pid": 15272, "workers": [15272, 20484]}` |
| victim worker hard-killed (no shutdown hooks) | PASS | `{"pid": 15272, "process": "python3.10.exe", "kill": "TerminateProcess"}` |
| survivor still alive | PASS | `{"pid": 20484}` |
| job remains in processing after kill (no loss) | PASS | `{"lease": "{\"owner\": \"COGLAP021:15272:4af4ee0c\", \"expires\": 1793861353.31401}", "processing": ["job_fc70647777bf"]}` |
| job completed after lease expiry recovery | PASS | `{"status": "done", "attempts": 2, "error": null, "recovery_seconds": 20.45}` |
| retry happened exactly once (attempts==2) | PASS | `{"attempts": 2}` |
| result present | PASS | `{"result_text": "Mock response for stress testing"}` |
| exactly one cost row written | PASS | `{"before": 0, "after": 1}` |
| Redis cost counter incremented once | PASS | `{"delta": 6.5e-07, "job_cost": 6.5e-07}` |
| no leftover queue/lease/delayed/DLQ state | PASS | `{"processing": [], "lease": null, "delayed": null, "dlq": null}` |
| heartbeat job leased | PASS | `{"job_id": "job_7c1142612c49", "lease": "{\"owner\": \"COGLAP021:20484:e1c68467\", \"expires\": 1793861375.0775933}"}` |
| heartbeat extended lease beyond first expiry | PASS | `{"first_expires_in_s": 3.4, "second_expires_in_s": 13.4}` |
| long job completed without spurious retry | PASS | `{"status": "done", "attempts": 1}` |
| job reaches DLQ after max attempts | PASS | `{"status": "dlq", "attempts": 3, "error": "All LLM providers failed. Summary: mock (attempt 1): server_error_503; mock (attempt 2): server_error_503"}` |
| replay accepted | PASS | `{"id": "job_b29aa5aa4efd", "status": "queued", "replayed": true}` |
| replay reset the attempt budget before re-processing | PASS | `{"status": "dlq", "attempts": 3}` |
| replayed job completes after provider heals | PASS | `{"status": "done", "attempts": 1}` |
| cost charged once for replayed job | PASS | `{"before": 0, "after": 1}` |
| guardrail-blocked job goes to DLQ | PASS | `{"status": "dlq", "error": "guardrail: Security guardrail blocked request: Prompt injection pattern detected ('ignore all previous instructions') (OWASP LLM01)."}` |
| clear_dlq reports deleted rows | PASS | `{"status": "ok", "cleared": 2}` |
| DLQ empty in DB and Redis after clear (await bug fixed) | PASS | `{"db_dlq_rows": 0, "redis_dlq": {}}` |
| job leased under the 6s override | PASS | `{"lease": "{\"owner\": \"COGLAP021:23588:7a59d13c\", \"expires\": 1793861409.0008924}"}` |
| slow-but-alive job keeps its lease past the original 6s expiry | PASS | `{"first_expires_in_s": -3.0, "renewed_expires_in_s": 5.1, "worker_alive": true}` |
| lease was renewed (not merely unexpired) | PASS | `{"first_expiry": 1793861409.0008924, "renewed_expiry": 1793861417.134}` |
| slow-but-alive job completes exactly once (attempts==1) | PASS | `{"status": "done", "attempts": 1}` |
| slow-but-alive job charged once | PASS | `{"before": 0, "after": 1}` |
| no duplicate delivery state after slow-but-alive job | PASS | `{"processing": [], "lease": null, "delayed": null, "dlq": null}` |
