# Degraded-mode local fallbacks

Redis-backed rate limits and response caching have bounded process-local
fallbacks for Redis outages. The local rate limiter holds at most 10,000
least-recently-used buckets and expires an idle bucket after one hour. The
response cache holds at most 1,000 least-recently-used entries and 16 MiB of
serialized entries; it removes an expired entry when it is read and skips local
caching for a single response larger than that byte limit. These bounds apply
per process.

The local rate limiter is a best-effort availability fallback. Its decisions
are not shared across workers or service instances, so it does not provide a
distributed quota guarantee. The local response cache is also process-local;
cache misses can differ between workers. Production deployments that require
consistent rate limits or shared cache behavior must keep Redis available.

When Redis becomes reachable again, new operations use Redis immediately; the
process-local fallback state is not copied into Redis. A rate-limit bucket may
therefore start with Redis's current or initial token state, and local cache
entries remain local until they expire or are evicted.

Daily budget admission is separate: the application database is authoritative
and Redis is not used to approve reservations. A database failure rejects the
budgeted request path rather than switching to local or Redis-based admission.
