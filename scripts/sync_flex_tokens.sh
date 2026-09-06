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

# IB expires Flex tokens (error 1012) and nothing else records when they were
# issued: keep the date in the Secret so /metrics can warn before that day.
# Set IB_FLEX_TOKENS_ISSUED_AT=YYYY-MM-DD in .env when re-syncing unchanged tokens.
ISSUED_AT="${IB_FLEX_TOKENS_ISSUED_AT:-$(date -u +%F)}"

kubectl create secret generic bifrost-flex-tokens \
  --namespace plugin-flex-query \
  --from-literal=FLEX_HOST_TOKEN="$HOST_TOKEN" \
  --from-literal=FLEX_SECONDARY_TOKEN="$SEC_TOKEN" \
  --from-literal=FLEX_TOKENS_ISSUED_AT="$ISSUED_AT" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl rollout restart deployment/flex-query-api deployment/flex-query-worker -n plugin-flex-query || true
echo "bifrost-flex-tokens applied in plugin-flex-query (issued_at=$ISSUED_AT); api + worker rollout restarted (best-effort)."
