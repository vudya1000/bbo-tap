# bbo-tap ingestor. The config is mounted, never baked in:
#   docker run -v ./config.yaml:/etc/bbo-tap/config.yaml:ro -e BBO_TAP_BROKERS=host:9092 bbo-tap

# Build: resolve the locked dependencies into a venv.
FROM python:3.14-slim-bookworm AS build
COPY --from=ghcr.io/astral-sh/uv:0.11.32 /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=0
WORKDIR /app
# Dependencies first, so editing src/ doesn't reinstall them.
COPY pyproject.toml uv.lock README.md LICENSE ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src ./src
RUN uv sync --frozen --no-dev --no-editable

# Runtime: same Python, just the venv. confluent-kafka wheels bundle librdkafka.
FROM python:3.14-slim-bookworm
RUN useradd --system --uid 10001 --no-create-home app
COPY --from=build /app/.venv /app/.venv
ENV PATH=/app/.venv/bin:$PATH PYTHONUNBUFFERED=1
USER app

# Healthy while records flow: the heartbeat file is touched every 10 s only then.
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=2 \
    CMD ["python", "-c", "import os, sys, time; sys.exit(time.time() - os.path.getmtime('/tmp/bbo-tap.alive') > 60)"]

# Prometheus metrics at :9464/metrics.
EXPOSE 9464

# Exec form: the app is PID 1 and gets SIGTERM directly (it has no child processes).
ENTRYPOINT ["bbo-tap"]
CMD ["--config", "/etc/bbo-tap/config.yaml", "--heartbeat", "/tmp/bbo-tap.alive", "--metrics-port", "9464"]
