#!/usr/bin/env bash
# One-time setup of the local kind cluster and shared add-ons.
set -euo pipefail
cd "$(dirname "$0")/.."

CLUSTER=arbordb
HOST=arbordb.localhost

if ! kind get clusters | grep -qx "$CLUSTER"; then
  kind create cluster --config deploy/kind-config.yaml --wait 120s
fi

echo "== ingress-nginx"
kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v1.13.0/deploy/static/provider/kind/deploy.yaml
# ports 80/443 are only mapped on the control-plane node, so run the controller there
kubectl -n ingress-nginx patch deploy ingress-nginx-controller --type merge \
  -p '{"spec":{"template":{"spec":{"nodeSelector":{"ingress-ready":"true"}}}}}'
kubectl -n ingress-nginx rollout status deploy/ingress-nginx-controller --timeout=180s

echo "== helm repos"
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts >/dev/null
helm repo update >/dev/null

echo "== monitoring (Prometheus, Grafana, Alertmanager)"
kubectl create namespace monitoring --dry-run=client -o yaml | kubectl apply -f -
if ! kubectl -n monitoring get secret grafana-admin >/dev/null 2>&1; then
  kubectl -n monitoring create secret generic grafana-admin \
    --from-literal=admin-user=admin \
    --from-literal=admin-password="$(openssl rand -base64 18)"
fi
helm upgrade --install monitoring prometheus-community/kube-prometheus-stack -n monitoring \
  --version 75.15.1 -f deploy/monitoring/values.yaml --wait --timeout 10m

echo "== app namespace + TLS certificate"
kubectl apply -f deploy/namespace.yaml
if ! kubectl -n arbordb get secret arbordb-tls >/dev/null 2>&1; then
  tmp=$(mktemp -d)
  openssl req -x509 -nodes -newkey rsa:2048 -days 365 \
    -keyout "$tmp/tls.key" -out "$tmp/tls.crt" \
    -subj "/CN=$HOST" -addext "subjectAltName=DNS:$HOST"
  kubectl -n arbordb create secret tls arbordb-tls --cert="$tmp/tls.crt" --key="$tmp/tls.key"
  rm -rf "$tmp"
fi

echo "Cluster ready. Grafana password:"
echo "  kubectl -n monitoring get secret grafana-admin -o jsonpath='{.data.admin-password}' | base64 -d"
