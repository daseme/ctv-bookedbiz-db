FROM python:3.13-slim

ARG LITESTREAM_VERSION=0.5.10
ARG TARGETARCH

RUN apt-get update && apt-get install -y --no-install-recommends wget curl sqlite3 ca-certificates && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN if [ "${TARGETARCH}" = "amd64" ] || [ "${TARGETARCH}" = "x86_64" ]; then \
      LITESTREAM_ARCH="x86_64"; \
    elif [ "${TARGETARCH}" = "arm64" ] || [ "${TARGETARCH}" = "aarch64" ]; then \
      LITESTREAM_ARCH="arm64"; \
    elif [ "${TARGETARCH}" = "arm" ] || [ "${TARGETARCH}" = "armv7" ]; then \
      LITESTREAM_ARCH="armv7"; \
    elif [ "${TARGETARCH}" = "armv6" ]; then \
      LITESTREAM_ARCH="armv6"; \
    elif [ -z "${TARGETARCH}" ]; then \
      UARCH="$(uname -m)"; \
      if [ "${UARCH}" = "x86_64" ]; then \
        LITESTREAM_ARCH="x86_64"; \
      elif [ "${UARCH}" = "aarch64" ]; then \
        LITESTREAM_ARCH="arm64"; \
      elif [ "${UARCH}" = "armv7l" ]; then \
        LITESTREAM_ARCH="armv7"; \
      elif [ "${UARCH}" = "armv6l" ]; then \
        LITESTREAM_ARCH="armv6"; \
      else \
        echo "Unsupported runtime architecture: ${UARCH}"; exit 1; \
      fi; \
    else \
      echo "Unsupported TARGETARCH: ${TARGETARCH}"; exit 1; \
    fi && \
    wget -qO /tmp/litestream.tar.gz "https://github.com/benbjohnson/litestream/releases/download/v${LITESTREAM_VERSION}/litestream-${LITESTREAM_VERSION}-linux-${LITESTREAM_ARCH}.tar.gz" && \
    tar -xzf /tmp/litestream.tar.gz -C /tmp && \
    install -m 0755 /tmp/litestream /usr/local/bin/litestream && \
    rm -f /tmp/litestream.tar.gz /tmp/litestream

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY . .
COPY litestream.yml /etc/litestream.yml
COPY backblaze_startup.sh /app/backblaze_startup.sh
RUN chmod +x /app/backblaze_startup.sh

ENV FLASK_ENV=production \
    PROJECT_ROOT=/app \
    DB_PATH=/srv/spotops/db/production.db \
    DATABASE_PATH=/srv/spotops/db/production.db \
    DATA_PATH=/srv/spotops/processed \
    APP_MODE=replica_readonly \
    RESTORE_ON_START=true \
    LITESTREAM_CONFIG=/etc/litestream.yml \
    PYTHONPATH=/app \
    PYTHONUNBUFFERED=1

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -fsS http://localhost:8000/health/ || exit 1

EXPOSE 8000

ENTRYPOINT ["/app/backblaze_startup.sh"]
CMD ["/app/.venv/bin/uvicorn", "src.web.asgi:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
