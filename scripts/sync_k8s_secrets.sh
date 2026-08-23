#!/usr/bin/env bash
# Patch plugin-flex-query/flex-query-secrets from trade-infra PGPASSWORD.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
INFRA_ENV="${BIFROST_TRADE_INFRA_ENV:-$HOME/Desktop/stocks/bifrost-trade-infra/.env}"
KUBECONFIG="${KUBECONFIG:-$HOME/.kube/bifrost-k3s.yaml}"
export KUBECONFIG

if [[ ! -f "$INFRA_ENV" ]]; then
  echo "Missing $INFRA_ENV" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "$INFRA_ENV"
set +a

if [[ -z "${PGPASSWORD:-}" ]]; then
  echo "PGPASSWORD empty in $INFRA_ENV" >&2
  exit 1
fi

kubectl create secret generic flex-query-secrets \
  --namespace plugin-flex-query \
  --from-literal=postgres-host=bifrost-postgres-rw.data.svc.cluster.local \
  --from-literal=postgres-password="$PGPASSWORD" \
  --from-literal=trade-pg-host=bifrost-postgres-rw.data.svc.cluster.local \
  --from-literal=trade-pg-password="$PGPASSWORD" \
  --from-literal=trade-pg-db=bifrost_dev \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl rollout restart deployment/flex-query-api deployment/flex-query-worker -n plugin-flex-query
echo "flex-query-secrets patched; api + worker rollout restarted."
