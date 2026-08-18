# Combined-context image build for Tekton / Kaniko.
# Expected context layout:
#   bifrost-trade-core/
#   plugin/                  (this repo)
FROM python:3.11-slim-bookworm

WORKDIR /app
RUN pip install --no-cache-dir --upgrade pip

COPY bifrost-trade-core /src/bifrost-trade-core
COPY plugin /src/plugin

RUN pip install --no-cache-dir /src/bifrost-trade-core /src/plugin

COPY plugin/config/flex-query.yaml.example /config/flex-query.yaml
COPY plugin/config/schedule.yaml /config/schedule.yaml
COPY plugin/scripts/ /app/scripts/

ENV PYTHONUNBUFFERED=1
ENV FLEX_QUERY_CONFIG=/config/flex-query.yaml
ENV SCHEDULE_CONFIG=/config/schedule.yaml

CMD ["python", "scripts/run_worker.py"]
