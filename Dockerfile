# Deployable unit: one image that runs the whole pipeline and exits.
# Run it on a schedule (Azure Container Apps Job, AWS ECS scheduled task,
# Kubernetes CronJob, or plain cron). Exit code: 0 ok, 1 DQ failure, 2 crash.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    MSP_DATA_DIR=/data \
    MSP_CONFIG_DIR=/app/config \
    MSP_SQL_DIR=/app/sql

WORKDIR /app

# Dependencies first so code changes don't invalidate this layer
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install .

COPY config ./config
COPY sql ./sql

# Non-root user; /data is the mounted volume (landing files, warehouse, reports)
RUN useradd --create-home --uid 10001 msp && mkdir -p /data && chown msp /data
USER msp
VOLUME ["/data"]

ENTRYPOINT ["msp"]
CMD ["run"]
