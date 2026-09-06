# No `# syntax=docker/dockerfile:1.4` line: resolving that frontend needs Docker
# Hub at build time and timed out on 2026-09-06; the built-in BuildKit frontend
# already understands the named build context used below.
FROM python:3.11-slim-bookworm

WORKDIR /app
RUN pip install --no-cache-dir --upgrade pip

# Sibling package provided via --build-context core=../bifrost-trade-core
COPY --from=core pyproject.toml README.md /src/bifrost-trade-core/
COPY --from=core src /src/bifrost-trade-core/src
RUN pip install --no-cache-dir /src/bifrost-trade-core

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir .

COPY config/flex-query.yaml.example /config/flex-query.yaml
COPY config/schedule.yaml /config/schedule.yaml
COPY scripts/ ./scripts/

ENV PYTHONUNBUFFERED=1
ENV FLEX_QUERY_CONFIG=/config/flex-query.yaml
ENV SCHEDULE_CONFIG=/config/schedule.yaml

# Default: worker. API Deployment overrides with scripts/run_api.py (port 8791)
CMD ["python", "scripts/run_worker.py"]
