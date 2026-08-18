.PHONY: install-dev test lint db-init run-api run-worker kustomize-check docker-build

install-dev:
	pip install -e "../bifrost-trade-core"
	pip install -e ".[dev]"

test:
	PYTHONPATH=src pytest -q

lint:
	ruff check src tests scripts

db-init:
	python scripts/init_schema.py

run-api:
	python scripts/run_api.py

run-worker:
	python scripts/run_worker.py

kustomize-check:
	kubectl kustomize k8s/base >/dev/null

docker-build:
	docker build -t bifrost-flex-query:0.2.0 \
	  --build-context core=../bifrost-trade-core \
	  -f Dockerfile .
