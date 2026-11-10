# CI and local verification

CI runs on Python 3.10 with PostgreSQL 16 and Redis 7.4 service containers. Each GitHub Actions run receives fresh service containers; PostgreSQL and Redis behavior is verified in their dedicated integration stages using disposable databases. The full regression suite uses local fallbacks and deterministic mocks. No provider API keys are required, and that suite does not call live LLM providers.

Provider startup smoke tests make live inference requests and are disabled by default. To run them explicitly, set `GATEWAY_STARTUP_PROVIDER_SMOKE_TEST=true` and configure the provider API keys in the environment. A passing application test or CI integration stage verifies gateway behavior, not external provider availability; live smoke results are the only checks that verify real provider connectivity and inference.

## Install and static checks

From the repository root, install the pinned direct dependencies and development tools:

```powershell
python -m pip install -r requirements-dev.txt
python -m pip check
python -m compileall -q app tests
```

The gateway import and dashboard JavaScript syntax checks are also run by Stage A in `.github/workflows/ci.yml`.

## SQLite-focused tests

Set a test-only admin token before importing the application. An empty `DATABASE_URL` selects SQLite:

```powershell
$env:GATEWAY_ADMIN_TOKEN = 'local-test-only'
$env:DATABASE_URL = ''
$env:GATEWAY_TEST_DATABASE_URL = ''
pytest tests/test_budget_reservations.py tests/test_tracing.py tests/test_local_fallback_bounds.py tests/test_cost_cache.py -q
```

## PostgreSQL integration tests

Start PostgreSQL 16 and create a disposable database. Set both URLs to that same test database; never use a production URL. Initialize the gateway key schema, then enable the required-integration guard so missing PostgreSQL configuration fails instead of skipping:

```powershell
$env:GATEWAY_ADMIN_TOKEN = 'local-test-only'
$env:DATABASE_URL = 'postgresql://gateway:gateway@127.0.0.1:5432/gateway_test'
$env:GATEWAY_TEST_DATABASE_URL = $env:DATABASE_URL
$env:GATEWAY_REQUIRE_POSTGRES_TESTS = '1'
python -c "from app.security import init_security_db; init_security_db()"
pytest tests/test_budget_reservations.py tests/test_jobs_crash_safety.py -q
```

The budget test checks that the configured database URLs match. PostgreSQL concurrency coverage is skipped only when the required-integration flag is unset and the dedicated URL is absent.

## Redis integration tests

Start Redis 7.4 locally. The circuit-breaker integration test uses database 14; job notification integration derives database 14 from `REDIS_URL`. `GATEWAY_REQUIRE_REDIS_TESTS=1` makes an absent or unavailable Redis service fail the integration tests:

```powershell
$env:GATEWAY_ADMIN_TOKEN = 'local-test-only'
$env:DATABASE_URL = ''
$env:REDIS_URL = 'redis://127.0.0.1:6379/15'
$env:CIRCUIT_TEST_REDIS_URL = 'redis://127.0.0.1:6379/14'
$env:CIRCUIT_BREAKER_REDIS_URL = $env:CIRCUIT_TEST_REDIS_URL
$env:GATEWAY_REQUIRE_REDIS_TESTS = '1'
pytest tests/test_circuit_breakers.py tests/test_jobs_crash_safety.py -q
```

## Full regression suite

With the test-only admin token set, run the full suite against SQLite with Redis-backed application clients disabled. Real Redis behavior is covered by the separate Redis integration command above:

```powershell
$env:GATEWAY_ADMIN_TOKEN = 'local-test-only'
$env:DATABASE_URL = ''
$env:REDIS_URL = ''
$env:CIRCUIT_BREAKER_REDIS_URL = ''
$env:GATEWAY_TEST_DISABLE_REDIS = '1'
$env:GATEWAY_STARTUP_PROVIDER_SMOKE_TEST = 'false'
pytest -q
```

Common setup failures are a missing `GATEWAY_ADMIN_TOKEN`, unavailable PostgreSQL/Redis services, setting only one of the PostgreSQL URLs, or accidentally pointing test variables at a non-disposable database. The PostgreSQL and Redis CI stages set required-integration flags, so unavailable services fail those stages rather than silently reducing coverage.
