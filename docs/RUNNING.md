# Running ArborDB on Kubernetes

Everything runs locally on a kind cluster. No cloud account is needed.

## Prerequisites

| Tool | Version used |
|---|---|
| Docker | 29.x |
| kind | 0.29.0 |
| kubectl | 1.33+ |
| Helm | 3.18 |
| openssl, curl | any recent |

Free ports 80 and 443 on the host (the ingress listens there).

## 1. Create the cluster and add-ons

```bash
./scripts/setup-cluster.sh
```

This creates a 3-node kind cluster (1 control plane, 2 workers) and installs:

- ingress-nginx
- kube-prometheus-stack (Prometheus, Grafana, Alertmanager) in the `monitoring` namespace
- the `arbordb` namespace with Pod Security `restricted` enforced
- a self-signed TLS certificate as the `arbordb-tls` secret

The Grafana admin password is generated randomly and stored only in the cluster.

## 2. Deploy

### Through the pipeline

Every push to `main` or a `feature/**` branch runs `.github/workflows/ci-cd.yml`:

1. API lint and tests, engine tests, frontend lint and build, and a Trivy scan for vulnerable dependencies and committed secrets (all in parallel)
2. Build both images, tag them with the short commit SHA and scan them. Images are pushed to ghcr.io only if the scan passes
3. The `deploy` job waits for approval on the `production` environment, then runs on the self-hosted runner: it pulls the images, loads them into kind and runs `helm upgrade --atomic`, followed by a smoke test

One-time setup for the deploy job:

```bash
# kubeconfig for the namespace-scoped ci-deployer service account
./scripts/create-deployer-kubeconfig.sh ~/.kube/arbordb-deployer
```

- In GitHub, under Settings > Environments, create `production` and add yourself as a required reviewer
- In GitHub, under Settings > Actions > Runners, add a self-hosted Linux runner with the label `arbordb-kind`, on the machine that runs the cluster
- In the runner directory, add two lines to its `.env` file: `KUBECONFIG=/home/<you>/.kube/arbordb-deployer` so deploys use the restricted account, and a `PATH` that includes where `kind`, `helm` and `kubectl` are installed

### By hand

```bash
TAG=$(git rev-parse --short HEAD)
docker build -f api/Dockerfile --build-arg APP_VERSION=$TAG -t ghcr.io/yash020405/arbordb-api:$TAG .
docker build -t ghcr.io/yash020405/arbordb-frontend:$TAG frontend
kind load docker-image --name arbordb ghcr.io/yash020405/arbordb-api:$TAG ghcr.io/yash020405/arbordb-frontend:$TAG
helm upgrade --install arbordb deploy/helm/arbordb -n arbordb \
  -f deploy/helm/arbordb/values-kind.yaml --set image.tag=$TAG --atomic --wait
```

## 3. Use it

| What | Where |
|---|---|
| App | https://arbordb.localhost (self-signed cert, so accept the browser warning) |
| API | https://arbordb.localhost/api/health |
| Grafana | `kubectl -n monitoring port-forward svc/monitoring-grafana 3001:80`, then http://localhost:3001, user `admin` |
| Grafana password | `kubectl -n monitoring get secret grafana-admin -o jsonpath='{.data.admin-password}' \| base64 -d` |
| Prometheus | `kubectl -n monitoring port-forward svc/monitoring-kube-prometheus-prometheus 9090:9090` |
| Logs | `kubectl -n arbordb logs arbordb-api-0` (one JSON line per request, with `requestId` and `version`) |

`*.localhost` resolves to the local machine, so no `/etc/hosts` change is needed. If your system resolves it to `::1` only, use `curl --resolve arbordb.localhost:443:127.0.0.1 ...`.

## 4. Check it

```bash
./scripts/verify.sh               # end to end checks, prints PASS/FAIL per claim
./scripts/demo-bad-release.sh     # deploys a broken release and shows the rollback
helm history arbordb -n arbordb   # release history; roll back with: helm rollback arbordb <rev> -n arbordb
```

## 5. Tear down

```bash
kind delete cluster --name arbordb
```

## Known limitations

- The API runs as a single replica. The engine stores tables and the WAL on local disk, so only one pod can own the volume. Restarting or upgrading the API causes a short outage, a few seconds normally and longer for a release that fails. The frontend has 2 replicas and stays up.
- A failed API release needs its pod deleted, because Kubernetes doesn't replace a StatefulSet pod that never became ready, even after the template is reverted. The pipeline does this in its failure step, and `scripts/demo-bad-release.sh` shows it.
- Drift isn't detected. The cluster is only changed by the pipeline, which deploys from git with Helm, and the CI account can't touch anything outside the `arbordb` namespace, but a manual `kubectl edit` by an admin would go unnoticed. `helm get manifest` can be compared by hand, and Argo CD would detect and revert it.
- There is no distributed tracing. With one backend service, request IDs in the logs are enough to follow a request. OpenTelemetry would be the next step if the API were split into services.
- Logs aren't centralised and are read with `kubectl logs`. Loki would be the next step.
- TLS uses a self-signed certificate. On a real cluster, cert-manager with Let's Encrypt would issue it.
- The data volume isn't backed up. A CronJob that snapshots the PVC would be needed before real use.
- Only the `kind` environment is set up, though the chart can take more values files such as `values-staging.yaml`.
