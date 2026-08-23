.PHONY: install-dev test lint db-init run-api run-worker kustomize-check docker-build sync-dev-env sync-k8s-secrets

install-dev:
	pip install -e "../bifrost-trade-core"
	pip install -e ".[dev]"

sync-dev-env:
	bash scripts/sync_dev_env.sh

sync-k8s-secrets:
	bash scripts/sync_k8s_secrets.sh

test:
	PYTHONPATH=src pytest -q

lint:
	ruff check src tests scripts

db-init:
	bash -c 'set -a; [ -f .env ] && source .env; set +a; .venv/bin/python scripts/init_schema.py'

run-api:
	bash scripts/run_local_api.sh

run-worker:
	python scripts/run_worker.py

kustomize-check:
	kubectl kustomize k8s/base >/dev/null

docker-build:
	docker build -t bifrost-flex-query:0.2.0 \
	  --build-context core=../bifrost-trade-core \
	  -f Dockerfile .
