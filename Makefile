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

# The image installs core from this sibling checkout as it is on disk, uncommitted and
# untracked files included, so the build refuses a dirty one (TD-37). Point CORE_DIR at
# a clean checkout of the intended commit; the commit is recorded as an image label.
CORE_DIR ?= ../bifrost-trade-core
VERSION := $(shell sed -n 's/^version *= *"\([^"]*\)".*/\1/p' pyproject.toml | head -n 1)

docker-build:
	@test -z "$$(git -C $(CORE_DIR) status --porcelain)" || { \
	  echo "$(CORE_DIR) has uncommitted or untracked files; the image would ship them." >&2; \
	  echo "Build from a clean core checkout on the intended commit (make docker-build CORE_DIR=...)." >&2; \
	  exit 1; }
	@echo "flex-query $(VERSION) with bifrost-core $$(git -C $(CORE_DIR) rev-parse HEAD)"
	docker build --platform linux/amd64 -t bifrost-flex-query:$(VERSION) \
	  --build-context core=$(CORE_DIR) \
	  --label io.bifrost.core.sha=$$(git -C $(CORE_DIR) rev-parse HEAD) \
	  -f Dockerfile .
	docker tag bifrost-flex-query:$(VERSION) 192.168.10.73:30500/bifrost-flex-query:$(VERSION)
	docker tag bifrost-flex-query:$(VERSION) 192.168.10.73:30500/bifrost-flex-query:latest
	docker push 192.168.10.73:30500/bifrost-flex-query:$(VERSION)
	docker push 192.168.10.73:30500/bifrost-flex-query:latest
