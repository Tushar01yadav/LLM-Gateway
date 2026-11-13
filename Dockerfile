FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    APP_ENV=production

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY models.json .
COPY scripts/migrate_sqlite_to_supabase.py ./scripts/migrate_sqlite_to_supabase.py

# gateway.db is created at /app/gateway.db on first start. Mount a volume specifically at /app/gateway.db (never mount over /app directly, as that shadows application code).
RUN useradd --create-home --uid 10001 gateway \
    && chown -R gateway:gateway /app
USER gateway

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=3s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=2)"

# GATEWAY_ADMIN_TOKEN is required at import time; pass it with -e or an env file.
# --no-proxy-headers keeps request.client authoritative for get_client_ip;
# see README "Deploying" for TRUSTED_PROXY_COUNT.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-proxy-headers"]
