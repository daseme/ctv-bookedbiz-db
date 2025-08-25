# Multi-stage build for CTV BookedBiz Flask App
# Based on your clean architecture principles

# Build stage - install dependencies
FROM python:3.11-slim as builder

# Set working directory
WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies (including uvicorn for ASGI server)
RUN pip install --no-cache-dir --user -r requirements.txt uvicorn[standard]

# Production stage - minimal runtime
FROM python:3.11-slim

# Create non-root user for security
RUN groupadd -g 1001 ctvapp && \
    useradd -r -u 1001 -g ctvapp ctvapp

# Set working directory
WORKDIR /app

# Install runtime dependencies only
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

# Copy Python dependencies from builder stage
COPY --from=builder /root/.local /home/ctvapp/.local

# Copy application code
COPY --chown=ctvapp:ctvapp . .

# Create necessary directories with proper permissions
RUN mkdir -p data/database logs && \
    chown -R ctvapp:ctvapp data/ logs/ && \
    chmod 755 data/ logs/

# Set PATH to include user-installed packages
ENV PATH="/home/ctvapp/.local/bin:$PATH"

# Create necessary directories with proper permissions
RUN mkdir -p data/database data/processed logs && \
    chown -R ctvapp:ctvapp data/ logs/ && \
    chmod 755 data/ logs/

# Switch to non-root user
USER ctvapp

# Health check endpoint
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8000/api/system-stats || exit 1

# Expose port
EXPOSE 8000

# Use Uvicorn ASGI server (matches pi-ctv and pi2 production setup)
CMD ["uvicorn", "src.web.asgi:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]