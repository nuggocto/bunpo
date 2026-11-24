#!/usr/bin/env bash
set -euo pipefail

# Runs the Go black-box protocol suite (BUNPO_TEST_SERVER_URL) and bunpo --queue
# end-to-end tests against a local workerd instance started by wrangler dev --local.
# Zero Cloudflare API calls, zero telemetry, stops all child processes on exit or Ctrl-C.

ROOT_DIR=$(cd "$(dirname "$0")/../.." && pwd)
WORKER_DIR="$ROOT_DIR/worker"
TMP_DIR=$(mktemp -d)
SERVER_PID=""
VICTIM_PID=""

stop_tree() {
  local pid="$1"
  [ -z "$pid" ] && return
  local children
  children=$(pgrep -P "$pid" 2>/dev/null || true)
  for c in $children; do
    stop_tree "$c"
  done
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    for _ in $(seq 1 20); do
      kill -0 "$pid" 2>/dev/null || break
      sleep 0.1
    done
    kill -9 "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
}

cleanup() {
  stop_tree "$VICTIM_PID"
  stop_tree "$SERVER_PID"
  rm -rf "$TMP_DIR"
}
trap 'exit 130' INT
trap 'exit 143' TERM
trap cleanup EXIT

WRITE_TOKEN="blackbox-write-token-0123456789abcdef"
READ_TOKEN="blackbox-read-token-fedcba9876543210"
PORT="${BUNPO_WORKER_TEST_PORT:-18787}"
BASE_URL="http://127.0.0.1:${PORT}"

export WRANGLER_SEND_METRICS=false
export WRANGLER_LOG_PATH="${WRANGLER_LOG_PATH:-$WORKER_DIR/.wrangler/logs}"
mkdir -p "$WRANGLER_LOG_PATH"

(
  cd "$WORKER_DIR"
  exec ./node_modules/.bin/wrangler dev --local \
    --ip 127.0.0.1 \
    --port "$PORT" \
    --inspector-port 0 \
    --persist-to "$TMP_DIR/do-state" \
    --show-interactive-dev-session=false \
    --var "BUNPO_QUEUE_TOKEN:${WRITE_TOKEN}" \
    --var "BUNPO_QUEUE_READ_TOKEN:${READ_TOKEN}"
) > "$TMP_DIR/wrangler.log" 2>&1 &
SERVER_PID=$!

ready=0
for _ in $(seq 1 60); do
  if curl -fsS "${BASE_URL}/healthz" 2>/dev/null | grep -q '{"ok":true}'; then
    ready=1
    break
  fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "wrangler dev exited early:" >&2
    cat "$TMP_DIR/wrangler.log" >&2
    exit 1
  fi
  sleep 0.5
done
if [ "$ready" -ne 1 ]; then
  echo "wrangler dev did not become ready in time:" >&2
  cat "$TMP_DIR/wrangler.log" >&2
  exit 1
fi

echo "==> Checking dashboard Static Assets and headers on ${BASE_URL}"
for p in "/" "/builds/release-1.2.3" "/builds/app.js" "/dashboard/app.js" "/dashboard/core.js" "/dashboard/style.css"; do
  HDR=$(curl -fsSI "${BASE_URL}${p}" | tr -d '\r')
  CSP_LINES=$(echo "$HDR" | grep -ic "^content-security-policy:" || true)
  test "$CSP_LINES" -eq 1
  echo "$HDR" | grep -qi "^content-security-policy: default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'$"
  echo "$HDR" | grep -qi "^x-content-type-options: nosniff$"
  echo "$HDR" | grep -qi "^referrer-policy: no-referrer$"
  if [[ "$p" == /dashboard/* ]]; then
    echo "$HDR" | grep -qi "^cache-control: public, max-age=300$"
  else
    echo "$HDR" | grep -qi "^cache-control: no-cache$"
  fi
done
for bad in "/dashboard/" "/dashboard/%2e%2e/%2e%2e/dashboard.go" "/%2e%2e/dashboard.go" "/_headers" "/builds/a..b" "/unknown-route"; do
  CODE=$(curl --path-as-is -sS -o "$TMP_DIR/bad.json" -w "%{http_code}" "${BASE_URL}${bad}")
  test "$CODE" -eq 404
  grep -q '"code":"not_found"' "$TMP_DIR/bad.json"
done

echo "==> Running Go black-box protocol suite against local workerd (${BASE_URL})"
(
  cd "$ROOT_DIR"
  BUNPO_TEST_SERVER_URL="$BASE_URL" \
  BUNPO_TEST_WRITE_TOKEN="$WRITE_TOKEN" \
  BUNPO_TEST_READ_TOKEN="$READ_TOKEN" \
  BUNPO_TEST_TTL_MS=10000 \
    go test -v -count=1 -run '^TestProtocolBlackBox$' ./internal/queueapi
)

echo "==> Building bunpo binary for end-to-end tests"
BUNPO_BIN="$TMP_DIR/bunpo"
(cd "$ROOT_DIR" && go build -o "$BUNPO_BIN" ./cmd/bunpo)

echo "==> E2E 1: 2 runners, one killed mid-run -> build passes and --status writes report-0.json"
E2E1_DIR="$TMP_DIR/e2e1"
mkdir -p "$E2E1_DIR/work" "$E2E1_DIR/out"
cat > "$E2E1_DIR/tests.txt" <<'EOF'
tests/alpha.py
tests/beta.py
tests/gamma.py
tests/delta.py
EOF

# Mock test runner script: runner 0 hangs on its first batch (gets SIGKILLed); runner 1 passes everything.
cat > "$E2E1_DIR/run-test.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
REPORT="$1"
ROLE="$2"
if [ "$ROLE" = "victim" ]; then
  touch "$MARKER_FILE"
  sleep 15
  exit 0
fi
{
  echo '<?xml version="1.0" encoding="UTF-8"?>'
  echo '<testsuites><testsuite name="s">'
  while IFS= read -r f; do
    [ -z "$f" ] && continue
    echo "  <testcase classname=\"c\" name=\"ok\" file=\"${f}\" time=\"0.050\"/>"
  done < "$BUNPO_TESTS_FILE"
  echo '</testsuite></testsuites>'
} > "$REPORT"
EOF
chmod +x "$E2E1_DIR/run-test.sh"

BUILD1="e2e-kill-$(date +%s)-$$"
MARKER="$E2E1_DIR/victim-started"

(
  cd "$E2E1_DIR/work"
  BUNPO_QUEUE_TOKEN="$WRITE_TOKEN" MARKER_FILE="$MARKER" \
    exec "$BUNPO_BIN" --queue "$BASE_URL" --build "$BUILD1" --total 2 --index 0 \
      --lease-ttl 10s --max-batch 1 --format junit \
      --report "$E2E1_DIR/r0.xml" -- \
      "$E2E1_DIR/run-test.sh" "$E2E1_DIR/r0.xml" victim < "$E2E1_DIR/tests.txt"
) > "$E2E1_DIR/r0.log" 2>&1 &
VICTIM_PID=$!

# Wait for victim to acquire a lease and start its test command, then SIGKILL it.
for _ in $(seq 1 40); do
  if [ -f "$MARKER" ]; then
    break
  fi
  sleep 0.25
done
if [ ! -f "$MARKER" ]; then
  echo "victim runner never started command:" >&2
  cat "$E2E1_DIR/r0.log" >&2
  kill -9 "$VICTIM_PID" 2>/dev/null || true
  exit 1
fi
kill -9 "$VICTIM_PID" 2>/dev/null || true
wait "$VICTIM_PID" 2>/dev/null || true

# Runner 1 completes the remaining tests AND picks up victim's expired lease after 10s TTL.
(
  cd "$E2E1_DIR/work"
  BUNPO_QUEUE_TOKEN="$WRITE_TOKEN" \
    "$BUNPO_BIN" --queue "$BASE_URL" --build "$BUILD1" --total 2 --index 1 \
      --lease-ttl 10s --max-batch 1 --format junit \
      --report "$E2E1_DIR/r1.xml" -- \
      "$E2E1_DIR/run-test.sh" "$E2E1_DIR/r1.xml" survivor < "$E2E1_DIR/tests.txt"
)

BUNPO_QUEUE_READ_TOKEN="$READ_TOKEN" \
  "$BUNPO_BIN" --queue "$BASE_URL" --build "$BUILD1" --status --dir "$E2E1_DIR/out"
test -f "$E2E1_DIR/out/report-0.json"

echo "==> E2E 2: failing test file -> exit 1"
E2E2_DIR="$TMP_DIR/e2e2"
mkdir -p "$E2E2_DIR/work" "$E2E2_DIR/out"
cat > "$E2E2_DIR/tests.txt" <<'EOF'
tests/pass.py
tests/fail.py
EOF

cat > "$E2E2_DIR/run-fail.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
REPORT="$1"
FAILED=0
{
  echo '<?xml version="1.0" encoding="UTF-8"?>'
  echo '<testsuites><testsuite name="s">'
  while IFS= read -r f; do
    [ -z "$f" ] && continue
    if [ "$f" = "tests/fail.py" ]; then
      FAILED=1
      echo "  <testcase classname=\"c\" name=\"bad\" file=\"${f}\" time=\"0.010\"><failure message=\"boom\"/></testcase>"
    else
      echo "  <testcase classname=\"c\" name=\"ok\" file=\"${f}\" time=\"0.020\"/>"
    fi
  done < "$BUNPO_TESTS_FILE"
  echo '</testsuite></testsuites>'
} > "$REPORT"
exit "$FAILED"
EOF
chmod +x "$E2E2_DIR/run-fail.sh"

BUILD2="e2e-fail-$(date +%s)-$$"
set +e
(
  cd "$E2E2_DIR/work"
  BUNPO_QUEUE_TOKEN="$WRITE_TOKEN" \
    "$BUNPO_BIN" --queue "$BASE_URL" --build "$BUILD2" --total 1 --index 0 \
      --lease-ttl 10s --max-batch 2 --format junit \
      --report "$E2E2_DIR/r.xml" -- \
      "$E2E2_DIR/run-fail.sh" "$E2E2_DIR/r.xml" < "$E2E2_DIR/tests.txt"
)
RC=$?
set -e
if [ "$RC" -ne 1 ]; then
  echo "expected exit 1 on failing test build, got $RC" >&2
  exit 1
fi

set +e
BUNPO_QUEUE_READ_TOKEN="$READ_TOKEN" \
  "$BUNPO_BIN" --queue "$BASE_URL" --build "$BUILD2" --status --dir "$E2E2_DIR/out"
STATUS_RC=$?
set -e
if [ "$STATUS_RC" -ne 1 ]; then
  echo "expected --status exit 1 on failed build, got $STATUS_RC" >&2
  exit 1
fi

echo "==> All Worker black-box and bunpo --queue E2E checks passed!"
