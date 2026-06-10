#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

CLUSTER_NAME="${CLUSTER_NAME:-hw11-argo}"

ensure_kind

if kind get clusters | grep -qx "${CLUSTER_NAME}"; then
  kind delete cluster --name "${CLUSTER_NAME}"
else
  echo "Cluster ${CLUSTER_NAME} does not exist."
fi
