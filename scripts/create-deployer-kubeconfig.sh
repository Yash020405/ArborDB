#!/usr/bin/env bash
# Writes a kubeconfig for the ci-deployer service account (namespace-scoped).
# Usage: scripts/create-deployer-kubeconfig.sh <output-file>
set -euo pipefail
cd "$(dirname "$0")/.."
OUT=${1:?output file required}

kubectl apply -f deploy/rbac/deployer.yaml >/dev/null
until kubectl -n arbordb get secret ci-deployer-token -o jsonpath='{.data.token}' | grep -q .; do sleep 1; done

SERVER=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')
CA=$(kubectl config view --raw --minify -o jsonpath='{.clusters[0].cluster.certificate-authority-data}')
TOKEN=$(kubectl -n arbordb get secret ci-deployer-token -o jsonpath='{.data.token}' | base64 -d)

umask 077
cat > "$OUT" <<KCFG
apiVersion: v1
kind: Config
clusters:
  - name: arbordb
    cluster:
      server: $SERVER
      certificate-authority-data: $CA
users:
  - name: ci-deployer
    user:
      token: $TOKEN
contexts:
  - name: ci-deployer
    context: { cluster: arbordb, user: ci-deployer, namespace: arbordb }
current-context: ci-deployer
KCFG
echo "wrote $OUT"
