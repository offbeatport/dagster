
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/dagster/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -U pip \
    && pip install --no-cache-dir -r requirements.txt

COPY burningdemand/ ./burningdemand/
COPY workspace.yaml dagster.yaml ./

ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p "$DAGSTER_HOME" \
    && cp /opt/dagster/app/dagster.yaml "$DAGSTER_HOME/dagster.yaml"

EXPOSE 8091

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -fsS http://localhost:8091/ || exit 1

CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "8091", "-w", "workspace.yaml"]