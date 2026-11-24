#!/usr/bin/env bash
# Smoke-tests a bunpo-server binary: starts it on loopback with test tokens,
# verifies /healthz and the embedded dashboard with CSP, and prints the server
# log if any check fails.
#
#   scripts/smoke-server.sh path/to/bunpo-server
set -euo pipefail

bin=${1:?usage: smoke-server.sh BINARY}
port=${BUNPO_SMOKE_PORT:-18080}
url="http://127.0.0.1:$port"

log=$(mktemp)
pid=""
cleanup() {
	local rc=$?
	if [ -n "$pid" ]; then
		kill "$pid" 2>/dev/null || true
		wait "$pid" 2>/dev/null || true
	fi
	if [ "$rc" -ne 0 ] && [ -s "$log" ]; then
		echo "bunpo-server log:" >&2
		cat "$log" >&2
	fi
	rm -f "$log"
}
trap cleanup EXIT

chmod +x "$bin"
BUNPO_QUEUE_TOKEN="smoke-check-write-token-01234567" \
BUNPO_QUEUE_READ_TOKEN="smoke-check-read-token-fedcba987" \
	"$bin" --addr "127.0.0.1:$port" >"$log" 2>&1 &
pid=$!

ok=0
for _ in $(seq 1 30); do
	if curl -fsS "$url/healthz" 2>/dev/null | grep -q '{"ok":true}'; then
		ok=1
		break
	fi
	if ! kill -0 "$pid" 2>/dev/null; then
		echo "bunpo-server exited early" >&2
		exit 1
	fi
	sleep 0.2
done
if [ "$ok" -ne 1 ]; then
	echo "bunpo-server did not answer /healthz in time" >&2
	exit 1
fi

curl -fsSI "$url/" | grep -qi "^content-security-policy:"
curl -fsS "$url/" | grep -q "Bunpō Queue"
