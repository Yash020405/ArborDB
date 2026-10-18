#!/usr/bin/env bash
# Deploys a release with a missing image to show the rollback and recovery.
set -uo pipefail
cd "$(dirname "$0")/.."

GOOD=$(helm get values arbordb -n arbordb -o json | sed -n 's/.*"tag":"\([^"]*\)".*/\1/p')
BAD=does-not-exist
echo "current good version: $GOOD"

echo "== watching availability (frontend/api status codes) during the bad release"
( for i in $(seq 1 40); do
    printf "%s/%s " "$(curl -sk -o /dev/null -w '%{http_code}' https://arbordb.localhost/)" \
                    "$(curl -sk -o /dev/null -w '%{http_code}' https://arbordb.localhost/api/health)"
    sleep 2
  done; echo ) &
WATCH=$!

echo "== deploying bad release $BAD"
helm upgrade arbordb deploy/helm/arbordb -n arbordb -f deploy/helm/arbordb/values-kind.yaml \
  --set image.tag=$BAD --atomic --wait --timeout 45s
echo "helm exit code: $? (non-zero = release failed and was rolled back)"

echo "== recovering API pod"
kubectl -n arbordb rollout status sts/arbordb-api --timeout=10s \
  || kubectl -n arbordb delete pod arbordb-api-0
kubectl -n arbordb rollout status sts/arbordb-api --timeout=120s

wait $WATCH
echo "== release history"
helm history arbordb -n arbordb --max 4
./scripts/smoke-test.sh "$GOOD"
