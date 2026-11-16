# Daily API-key budget enforcement

The gateway reserves an estimate in the durable application database before
calling a provider. A conditional update on a per-key daily account serializes
competing reservations in SQLite and PostgreSQL; SQLite acquires its write lock
before reading the balance, while PostgreSQL serializes at the account row.
Redis counters are not used to approve spending. The estimate uses configured
input/output token prices, the sanitized prompt length, and the request's
maximum output tokens.

On successful synchronous requests, one transaction marks the reservation
settled, releases its reserved amount, updates the daily account, and inserts
the authoritative cost record. On provider failure or request cancellation,
the reservation is released. Async jobs perform the same reservation before
provider execution and settle it in the transaction that commits the job result
and cost row. Reservation identifiers are unique, key-scoped, and accepted once;
stale/expired identifiers cannot settle or release a different reservation.
Expired reservations are recovered during subsequent reservations.

Actual usage is attributed to the UTC day on which its reservation was created,
even if the provider response finishes after midnight. Actual usage is charged
after the provider response. If actual provider cost
exceeds its estimate, settlement records the actual amount and the account can
exceed its configured limit by that difference. This implementation therefore
guarantees that concurrent reservations do not commit more than the remaining
budget based on the estimate; it cannot guarantee the external provider's final
charge will stay below the configured limit. Requests with missing usage use
the existing character-based token estimate and remain approximate. A process
crash leaves a temporary reservation until its lease expires and a later
reservation performs recovery.

If a provider omits token usage, the gateway records a cost estimate derived
from prompt and completion text lengths. That estimate is approximate and does
not make final upstream spend strictly bounded.

This implementation enforces daily budgets per API-key identifier (and the
existing shared guest identifier where the chat API assigns one). It does not
implement monthly or global budgets. Database availability is required for
strict admission; a database error is not replaced by an eventually consistent
Redis approval.
