#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
HW11_ROOT="${PROJECT_ROOT}/hw11"
BIN_DIR="${HW11_ROOT}/bin"

ARGO_VERSION="${ARGO_VERSION:-v3.7.4}"
KIND_VERSION="${KIND_VERSION:-v0.31.0}"

mkdir -p "${BIN_DIR}"
export PATH="${BIN_DIR}:${PATH}"

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Required command is missing: $1" >&2
    exit 1
  fi
}

detect_os() {
  case "$(uname -s)" in
    Darwin) echo "darwin" ;;
    Linux) echo "linux" ;;
    *)
      echo "Unsupported OS: $(uname -s)" >&2
      exit 1
      ;;
  esac
}

detect_arch() {
  case "$(uname -m)" in
    x86_64|amd64) echo "amd64" ;;
    arm64|aarch64) echo "arm64" ;;
    *)
      echo "Unsupported architecture: $(uname -m)" >&2
      exit 1
      ;;
  esac
}

ensure_kind() {
  local os arch target tmpfile
  target="${BIN_DIR}/kind"
  if [ -x "${target}" ]; then
    return
  fi

  os="$(detect_os)"
  arch="$(detect_arch)"
  tmpfile="$(mktemp)"

  curl -fsSL "https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-${os}-${arch}" -o "${tmpfile}"
  install -m 0755 "${tmpfile}" "${target}"
  rm -f "${tmpfile}"
}

ensure_argo() {
  local os arch target tmpfile gzfile
  target="${BIN_DIR}/argo"
  if [ -x "${target}" ]; then
    return
  fi

  os="$(detect_os)"
  arch="$(detect_arch)"
  tmpfile="$(mktemp)"
  gzfile="${tmpfile}.gz"

  curl -fsSL "https://github.com/argoproj/argo-workflows/releases/download/${ARGO_VERSION}/argo-${os}-${arch}.gz" -o "${gzfile}"
  gunzip -c "${gzfile}" > "${tmpfile}"
  install -m 0755 "${tmpfile}" "${target}"
  rm -f "${tmpfile}" "${gzfile}"
}
