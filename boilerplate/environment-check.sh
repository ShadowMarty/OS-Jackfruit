#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

BUILD_ONLY=0
if [[ "${1:-}" == "--build-only" ]]; then
    BUILD_ONLY=1
fi

ok() {
    echo "[OK] $1"
}

warn() {
    echo "[WARN] $1"
}

fail() {
    echo "[FAIL] $1" >&2
    exit 1
}

echo "== Supervised Runtime Project Preflight =="

if [[ -r /etc/os-release ]]; then
    # shellcheck disable=SC1091
    source /etc/os-release
    [[ "${ID:-}" == "ubuntu" ]] || fail "Unsupported distro: ${ID:-unknown}. Use Ubuntu 22.04/24.04 VM."
    case "${VERSION_ID:-}" in
        22.04|24.04) ok "Ubuntu version ${VERSION_ID} detected." ;;
        *) fail "Unsupported Ubuntu version: ${VERSION_ID:-unknown}. Use 22.04 or 24.04." ;;
    esac
else
    fail "Cannot read /etc/os-release to verify environment."
fi

if grep -qi microsoft /proc/sys/kernel/osrelease 2>/dev/null || grep -qi microsoft /proc/version 2>/dev/null; then
    fail "WSL detected. This project does not support WSL."
fi
ok "No WSL signature detected."

if command -v systemd-detect-virt >/dev/null 2>&1; then
    VIRT="$(systemd-detect-virt || true)"
    [[ "$VIRT" != "none" ]] || fail "No VM detected. Use an Ubuntu VM for this project."
    ok "Virtualized environment detected: $VIRT."
else
    warn "systemd-detect-virt not found; cannot strictly verify VM."
fi

if command -v mokutil >/dev/null 2>&1; then
    SB_STATE="$(mokutil --sb-state 2>/dev/null || true)"
    if echo "$SB_STATE" | grep -qi "SecureBoot enabled"; then
        fail "Secure Boot is enabled. Disable it before module testing."
    fi
    if echo "$SB_STATE" | grep -qi "SecureBoot disabled"; then
        ok "Secure Boot is disabled."
    else
        warn "Could not determine Secure Boot state via mokutil output."
    fi
else
    warn "mokutil not installed; cannot auto-check Secure Boot state."
fi

KBUILD_DIR="/lib/modules/$(uname -r)/build"
[[ -d "$KBUILD_DIR" ]] || fail "Kernel headers missing for $(uname -r). Install linux-headers-$(uname -r)."
ok "Kernel headers found at $KBUILD_DIR."

echo "Building user targets and module..."
LOG_FILE=$(mktemp /tmp/runtime_preflight_make.XXXXXX.log)
if make all >"$LOG_FILE" 2>&1; then
    ok "Boilerplate build succeeded."
else
    tail -n 20 "$LOG_FILE" || true
    fail "Build failed. See $LOG_FILE for details."
fi
rm -f "$LOG_FILE"

if [[ "$BUILD_ONLY" -eq 1 ]]; then
    ok "Build-only mode complete. Skipped insmod/rmmod and /dev checks."
    exit 0
fi

[[ "$(id -u)" -eq 0 ]] || fail "Run as root (sudo ./environment-check.sh) to verify insmod/rmmod and /dev/container_monitor."

INSERTED_BY_SCRIPT=0
cleanup() {
    if [[ "$INSERTED_BY_SCRIPT" -eq 1 ]]; then
        rmmod monitor >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

if lsmod | awk '{print $1}' | grep -qx monitor; then
    ok "monitor module is already loaded."
else
    insmod ./monitor.ko
    INSERTED_BY_SCRIPT=1
    ok "insmod monitor.ko succeeded."
fi

for _ in $(seq 1 10); do
    [[ -e /dev/container_monitor ]] && break
    sleep 0.2
done
[[ -e /dev/container_monitor ]] || fail "/dev/container_monitor not found after module load."
ok "/dev/container_monitor exists."

if [[ "$INSERTED_BY_SCRIPT" -eq 1 ]]; then
    rmmod monitor
    INSERTED_BY_SCRIPT=0
    ok "rmmod monitor succeeded."
fi

echo "Preflight passed."
