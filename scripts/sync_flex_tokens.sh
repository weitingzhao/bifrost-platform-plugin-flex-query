#!/usr/bin/env bash
# Create/update plugin-flex-query/bifrost-flex-tokens from IB_FLEX_*_TOKEN env.
# Wave 4: Flex tokens prefer K8s Secret over Trade DB plaintext columns.
set -euo pipefail
INFRA_ENV="${BIFROST_TRADE_INFRA_ENV:-$HOME/Desktop/stocks/bifrost-trade-infra/.env}"
# Prefer plugin-local .env, then trade-infra .env
PLUGIN_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
if [[ -f "$PLUGIN_ROOT/.env" ]]; then
  ENV_FILE="$PLUGIN_ROOT/.env"
elif [[ -f "$INFRA_ENV" ]]; then
  ENV_FILE="$INFRA_ENV"
else
  echo "Missing .env (tried $PLUGIN_ROOT/.env and $INFRA_ENV)" >&2
  exit 1
fi

KUBECONFIG="${KUBECONFIG:-$HOME/.kube/bifrost-k3s.yaml}"
export KUBECONFIG

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

HOST_TOKEN="${IB_FLEX_HOST_TOKEN:-${FLEX_HOST_TOKEN:-}}"
SEC_TOKEN="${IB_FLEX_SECONDARY_TOKEN:-${FLEX_SECONDARY_TOKEN:-}}"

if [[ -z "$HOST_TOKEN" && -z "$SEC_TOKEN" ]]; then
  echo "IB_FLEX_HOST_TOKEN / IB_FLEX_SECONDARY_TOKEN empty in $ENV_FILE" >&2
  echo "Secret will still be applied with empty values (optional envFrom)." >&2
fi

kubectl create secret generic bifrost-flex-tokens \
  --namespace plugin-flex-query \
  --from-literal=FLEX_HOST_TOKEN="$HOST_TOKEN" \
  --from-literal=FLEX_SECONDARY_TOKEN="$SEC_TOKEN" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl rollout restart deployment/flex-query-api deployment/flex-query-worker -n plugin-flex-query || true
echo "bifrost-flex-tokens applied in plugin-flex-query; api + worker rollout restarted (best-effort)."
