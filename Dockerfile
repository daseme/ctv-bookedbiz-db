FROM python:3.13-slim

RUN apt-get update && apt-get install -y --no-install-recommends wget curl sqlite3 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev

COPY . .
COPY litestream.yml /etc/litestream.yml

ENV FLASK_ENV=production \
    PROJECT_ROOT=/app \
    DATABASE_PATH=/srv/spotops/db/production.db \
    DATA_PATH=/srv/spotops/processed \
    PYTHONPATH=/app \
    PYTHONUNBUFFERED=1

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -fsS http://localhost:8000/health/ || exit 1

EXPOSE 8000

CMD ["uv", "run", "uvicorn", "src.web.asgi:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
