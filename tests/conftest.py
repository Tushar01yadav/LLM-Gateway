import os

# The application refuses to import without an admin token. Tests use a local value.
os.environ.setdefault("GATEWAY_ADMIN_TOKEN", "test-admin-token")
os.environ.setdefault("APP_ENV", "development")

# Keep test jobs/cache/limiter keys out of the dev Redis database (db 0) so a
# locally running server cannot consume work enqueued by the test suite.
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/15")

# ASGITransport does not run the FastAPI lifespan, which is where the server
# creates its SQLite schema. On a fresh clone there is no gateway.db yet, so
# create the same schema here once for the whole test session.
from app.main import init_db  # noqa: E402

init_db()

