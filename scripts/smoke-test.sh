#!/usr/bin/env bash
# Checks the deployed version and a SQL round trip through the ingress.
# Usage: scripts/smoke-test.sh <expected-version>
set -euo pipefail
EXPECTED=${1:?expected version required}
BASE=${BASE_URL:-https://arbordb.localhost}

# the single API pod takes a few seconds to be routable after a restart
for i in $(seq 1 30); do
  version=$(curl -sk "$BASE/api/health" | sed -n 's/.*"version":"\([^"]*\)".*/\1/p' || true)
  [ "$version" = "$EXPECTED" ] && break
  sleep 2
done
echo "running version: ${version:-none}"
[ "$version" = "$EXPECTED" ] || { echo "FAIL: expected $EXPECTED"; exit 1; }

query() {
  curl -sk -X POST "$BASE/api/query" -H 'Content-Type: application/json' -d "{\"sql\":\"$1\"}"
}
query "CREATE TABLE smoke (id INT PRIMARY KEY, note TEXT)" >/dev/null || true
query "DELETE FROM smoke WHERE id = 1" >/dev/null || true
query "INSERT INTO smoke VALUES (1, '$EXPECTED')" >/dev/null
result=$(query "SELECT * FROM smoke WHERE id = 1")
echo "$result" | grep -q "\"note\":\"$EXPECTED\"" || { echo "FAIL: query round trip: $result"; exit 1; }
echo "query round trip OK"
