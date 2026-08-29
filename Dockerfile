# QobuzProxy Docker Image
# Headless Qobuz music player service with DLNA support

FROM python:3.11-slim

# Labels
LABEL org.opencontainers.image.title="qobuz-proxy"
LABEL org.opencontainers.image.description="Headless Qobuz music player with DLNA support"

# Install system dependencies
# - curl: for health check
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Create non-root user
RUN useradd --create-home --shell /bin/bash qobuzproxy

# Set working directory
WORKDIR /app

# Copy package files
COPY pyproject.toml README.md ./
COPY qobuz_proxy/ ./qobuz_proxy/
COPY protos/ ./protos/

# Install package, including the `hires` extra (numpy/soundfile/soxr) needed
# for on-the-fly Hi-Res downsampling on the DLNA transcoding path.
RUN pip install --no-cache-dir ".[hires]"

# Create data directory and set ownership
RUN mkdir -p /data && chown qobuzproxy:qobuzproxy /data

# Switch to non-root user
USER qobuzproxy

# Credential cache and config live under /data
ENV QOBUZPROXY_DATA_DIR=/data

# Optional: short git commit hash baked in at build time so the running app
# can show which build is deployed. Pass with `--build-arg GIT_COMMIT=$(git rev-parse --short HEAD)`.
ARG GIT_COMMIT=""
ENV QOBUZPROXY_COMMIT=$GIT_COMMIT

# Expose ports (documentation only - host networking bypasses this)
# 8689: HTTP server for mDNS discovery endpoints
# 7120: Audio proxy server for DLNA streaming
EXPOSE 8689 7120

# Health check - verify web UI server is responding
# Note: With host networking, this checks localhost
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -sf http://localhost:${QOBUZPROXY_HTTP_PORT:-8689}/api/status || exit 1

# Default command
CMD ["qobuz-proxy"]
