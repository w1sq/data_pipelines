#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

CLUSTER_NAME="${CLUSTER_NAME:-hw11-argo}"
NAMESPACE="${NAMESPACE:-argo}"
ARGO_WAIT_TIMEOUT="${ARGO_WAIT_TIMEOUT:-600s}"
DEMO_WAIT_TIMEOUT="${DEMO_WAIT_TIMEOUT:-300s}"

need_cmd docker
need_cmd kubectl
need_cmd curl
ensure_kind
ensure_argo

print_debug_info() {
  echo
  echo "Deployment rollout timed out. Current diagnostics for namespace ${NAMESPACE}:"
  echo
  kubectl get pods -n "${NAMESPACE}" -o wide || true
  echo
  kubectl get deployments -n "${NAMESPACE}" || true
  echo
  kubectl get events -n "${NAMESPACE}" --sort-by=.lastTimestamp | tail -n 40 || true
}

wait_for_deployment() {
  local deployment_name="$1"

  if ! kubectl rollout status "deployment/${deployment_name}" -n "${NAMESPACE}" --timeout="${ARGO_WAIT_TIMEOUT}"; then
    print_debug_info
    kubectl describe deployment "${deployment_name}" -n "${NAMESPACE}" || true
    echo
    kubectl describe pods -n "${NAMESPACE}" -l "app=${deployment_name}" || true
    exit 1
  fi
}

if ! kind get clusters | grep -qx "${CLUSTER_NAME}"; then
  kind create cluster --name "${CLUSTER_NAME}"
else
  kubectl config use-context "kind-${CLUSTER_NAME}" >/dev/null
fi

kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -n "${NAMESPACE}" -f "https://github.com/argoproj/argo-workflows/releases/download/${ARGO_VERSION}/quick-start-minimal.yaml"

wait_for_deployment workflow-controller
wait_for_deployment argo-server
wait_for_deployment minio

kubectl apply -n "${NAMESPACE}" -f "${HW11_ROOT}/demo/data-demo-server.yaml"
if ! kubectl rollout status deployment/data-demo-server -n "${NAMESPACE}" --timeout="${DEMO_WAIT_TIMEOUT}"; then
  print_debug_info
  kubectl describe deployment data-demo-server -n "${NAMESPACE}" || true
  echo
  kubectl describe pods -n "${NAMESPACE}" -l app=data-demo-server || true
  exit 1
fi

kubectl apply -n "${NAMESPACE}" -f "${HW11_ROOT}/templates/"

echo
echo "Local demo environment is ready."
echo "Cluster: ${CLUSTER_NAME}"
echo "Namespace: ${NAMESPACE}"
echo
kubectl get pods -n "${NAMESPACE}"
echo
kubectl get svc -n "${NAMESPACE}" data-demo-server argo-server minio
