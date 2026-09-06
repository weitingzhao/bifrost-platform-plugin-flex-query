.PHONY: install-dev test lint db-init run-api run-worker kustomize-check docker-build sync-dev-env sync-k8s-secrets sync-flex-tokens

install-dev:
	pip install -e "../bifrost-trade-core"
	pip install -e ".[dev]"

sync-dev-env:
	bash scripts/sync_dev_env.sh

sync-k8s-secrets:
	bash scripts/sync_k8s_secrets.sh

sync-flex-tokens:
	bash scripts/sync_flex_tokens.sh

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
	docker build --platform linux/amd64 -t bifrost-flex-query:0.6.1 \
	  --build-context core=../bifrost-trade-core \
	  -f Dockerfile .
	docker tag bifrost-flex-query:0.6.1 192.168.10.73:30500/bifrost-flex-query:0.6.1
	docker tag bifrost-flex-query:0.6.1 192.168.10.73:30500/bifrost-flex-query:latest
	docker push 192.168.10.73:30500/bifrost-flex-query:0.6.1
	docker push 192.168.10.73:30500/bifrost-flex-query:latest
