#!/usr/bin/env bash
set -euo pipefail

if [ -f /etc/os-release ]; then
  # shellcheck disable=SC1091
  . /etc/os-release
fi

OS_ID="${ID:-unknown}"
OS_ID_LIKE="${ID_LIKE:-}"
OS_FAMILY="$OS_ID $OS_ID_LIKE"

if echo "$OS_FAMILY" | grep -qiE 'debian|ubuntu'; then
  sudo apt-get update
  sudo apt-get install -y --no-install-recommends sqlite3 zstd jq curl unzip
  if ! command -v gh >/dev/null 2>&1 && apt-cache show gh >/dev/null 2>&1; then
    sudo apt-get install -y --no-install-recommends gh || true
  fi
elif echo "$OS_FAMILY" | grep -qiE 'rhel|centos|fedora|oracle|almalinux|rocky'; then
  sudo dnf install -y sqlite zstd jq curl unzip
  if ! command -v gh >/dev/null 2>&1 && dnf info gh >/dev/null 2>&1; then
    sudo dnf install -y gh || true
  fi
else
  echo "Unsupported OS for DB workflow dependencies: $OS_ID ($OS_ID_LIKE)" >&2
  exit 1
fi

if ! command -v gh >/dev/null 2>&1; then
  echo "gh not found from package manager; installing GitHub CLI release binary"
  GH_VERSION=$(curl -fsSL https://api.github.com/repos/cli/cli/releases/latest | jq -r '.tag_name | sub("^v"; "")')
  case "$(uname -m)" in
    x86_64) GH_ARCH=amd64 ;;
    aarch64|arm64) GH_ARCH=arm64 ;;
    *)
      echo "Unsupported architecture for GitHub CLI: $(uname -m)" >&2
      exit 1
      ;;
  esac

  curl -fL "https://github.com/cli/cli/releases/download/v${GH_VERSION}/gh_${GH_VERSION}_linux_${GH_ARCH}.tar.gz" -o /tmp/gh.tar.gz
  rm -rf /tmp/gh
  mkdir -p /tmp/gh
  tar -xzf /tmp/gh.tar.gz -C /tmp/gh
  GH_BIN=$(find /tmp/gh -type f -path '*/bin/gh' | head -n 1 || true)
  if [ -z "$GH_BIN" ]; then
    echo "Failed to locate gh binary after download" >&2
    exit 1
  fi
  sudo install -m 0755 "$GH_BIN" /usr/local/bin/gh
fi

command -v gh
command -v sqlite3
command -v zstd
command -v unzstd
command -v jq
command -v unzip
