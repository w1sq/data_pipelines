#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

NAMESPACE="${NAMESPACE:-argo}"
SOURCE_URL="${SOURCE_URL:-http://data-demo-server.${NAMESPACE}.svc.cluster.local:8000/sales-broken.csv}"

ensure_argo

argo submit -n "${NAMESPACE}" --watch "${HW11_ROOT}/workflows/data-pipeline-workflow.yaml" \
  -p source-url="${SOURCE_URL}" \
  -p dataset-filename=sales-broken.csv \
  -p pipeline-name=SalesAnalyticsBroken \
  -p required-columns="region,product,amount" \
  -p min-rows=5 \
  -p group-by=region \
  -p value-column=amount \
  -p agg=sum

echo
echo "Latest workflow details:"
argo get -n "${NAMESPACE}" @latest
echo
echo "Latest workflow logs:"
argo logs -n "${NAMESPACE}" @latest
