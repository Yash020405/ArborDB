#!/usr/bin/env bash
# End-to-end checks against the running cluster.
# Usage: scripts/verify.sh
set -uo pipefail
cd "$(dirname "$0")/.."
BASE=https://arbordb.localhost
PASS=0; FAIL=0
ok()   { echo "  PASS  $*"; PASS=$((PASS+1)); }
bad()  { echo "  FAIL  $*"; FAIL=$((FAIL+1)); }
q()    { curl -sk -X POST "$BASE/api/query" -H 'Content-Type: application/json' -d "{\"sql\":\"$1\"}"; }

echo "== 1. Which version is running"
TAG=$(helm get values arbordb -n arbordb -o json | sed -n 's/.*"tag":"\([^"]*\)".*/\1/p')
echo "  helm release image tag : $TAG"
echo "  git commit             : $(git log -1 --format='%h %s' "$TAG" 2>/dev/null || echo 'not in local history')"
kubectl -n arbordb get pods -o custom-columns='POD:.metadata.name,IMAGE:.spec.containers[0].image,READY:.status.containerStatuses[0].ready,NODE:.spec.nodeName' | sed 's/^/  /'
echo "  /api/health            : $(curl -sk "$BASE/api/health")"
echo "== 2. User journey through the HTTPS ingress"
[ "$(curl -s -o /dev/null -w '%{http_code}' http://arbordb.localhost/)" = "308" ] && ok "HTTP redirects to HTTPS" || bad "no HTTPS redirect"
curl -sk "$BASE/" | grep -q '<div id="root">' && ok "frontend served" || bad "frontend not served"
./scripts/smoke-test.sh "$TAG" | sed 's/^/  /' && ok "API reports $TAG and a SQL insert + select works" || bad "smoke test failed"
NODES=$(kubectl -n arbordb get pods -l app.kubernetes.io/name=arbordb-frontend --field-selector=status.phase=Running -o jsonpath='{.items[*].spec.nodeName}' | tr ' ' '\n' | sort -u | wc -l)
[ "$NODES" -ge 2 ] && ok "frontend replicas spread over $NODES nodes" || bad "frontend replicas on one node"

echo "== 3. Data survives losing the API pod (StatefulSet + PVC)"
kubectl -n arbordb delete pod arbordb-api-0 --wait=true >/dev/null
kubectl -n arbordb wait pod/arbordb-api-0 --for=condition=Ready --timeout=120s >/dev/null
# the smoke test above wrote a row to the "smoke" table; it must survive the restart
for i in $(seq 1 20); do R=$(q "SELECT * FROM smoke WHERE id = 1"); echo "$R" | grep -q '"status":"ok"' && break; sleep 2; done
echo "$R" | grep -q "\"note\":\"$TAG\"" && ok "row written before the restart is still there" || bad "data lost after restart: $R"

echo "== 4. Security controls"
kubectl get ns arbordb -o jsonpath='{.metadata.labels.pod-security\.kubernetes\.io/enforce}' | grep -qx restricted \
  && ok "namespace enforces Pod Security 'restricted'" || bad "PSS not enforced"
kubectl -n arbordb get pod arbordb-api-0 -o jsonpath='{.spec.securityContext.runAsNonRoot} {.spec.containers[0].securityContext.readOnlyRootFilesystem}' | grep -qx "true true" \
  && ok "API runs as non-root with read-only root filesystem" || bad "API security context"
CODE=$(kubectl run verify-np --rm -i --restart=Never --image=curlimages/curl:8.11.1 -q -- \
  curl -s -m 5 -o /dev/null -w '%{http_code}' http://arbordb-api.arbordb.svc/health 2>/dev/null | tail -c 3)
[ "$CODE" != "200" ] && ok "NetworkPolicy blocks pods outside ingress/monitoring namespaces" || bad "API reachable from default namespace"
D=~/.kube/arbordb-deployer
if [ -f "$D" ]; then
  [ "$(KUBECONFIG=$D kubectl auth can-i create namespaces 2>/dev/null)" = "no" ] && \
  [ "$(KUBECONFIG=$D kubectl auth can-i get secrets -n kube-system 2>/dev/null)" = "no" ] && \
  ok "CI deployer is limited to the arbordb namespace" || bad "CI deployer has too much access"
fi

echo "== 5. Observability"
kubectl -n monitoring port-forward svc/monitoring-kube-prometheus-prometheus 9090:9090 >/dev/null 2>&1 & PF=$!
sleep 3
for i in $(seq 1 10); do curl -sk "$BASE/api/tables" >/dev/null; done
pq() { curl -s localhost:9090/api/v1/query --data-urlencode "query=$1"; }
pq "up{job=\"arbordb-api\"}" | grep -q '"value":\[[0-9.]*,"1"\]' && ok "Prometheus is scraping the API" || bad "API not scraped"
pq "arbordb_build_info" | grep -q "\"version\":\"$TAG\"" && ok "Prometheus sees version $TAG" || bad "build info missing"
RULES=$(curl -s localhost:9090/api/v1/rules | grep -o '"name":"ArborDB[A-Za-z]*"' | sort -u | tr '\n' ' ')
[ -n "$RULES" ] && ok "alert rules loaded: $RULES" || bad "alert rules missing"
echo "  current alerts: $(curl -s localhost:9090/api/v1/alerts | grep -o '"alertname":"ArborDB[A-Za-z]*","[^}]*"state":"[a-z]*"' | tr '\n' ' ' || true)"
kill $PF 2>/dev/null
echo "  sample API log line:"
kubectl -n arbordb logs arbordb-api-0 --tail=50 | grep '"path":"/tables' | tail -1 | sed 's/^/    /'

echo
echo "RESULT: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
