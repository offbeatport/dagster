FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/dagster/app

# Copy project files
COPY pyproject.toml ./
COPY src/ ./src/
COPY dagster.yaml ./

# Install build dependencies and the project
RUN pip install --no-cache-dir -U pip \
    && pip install --no-cache-dir build \
    && pip install --no-cache-dir -e .

ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p "$DAGSTER_HOME" \
    && cp /opt/dagster/app/dagster.yaml "$DAGSTER_HOME/dagster.yaml"

EXPOSE 8091

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -fsS http://localhost:8091/ || exit 1

# Default command (can be overridden in docker-compose)
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "8091", "-m", "burningdemand.definitions"]