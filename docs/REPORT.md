# ArborDB on Kubernetes: project report

This report covers the work done after the baseline commit `83cb207`. ArborDB itself (the C++ engine, the Node.js API and the React frontend) existed before and is only described where it affects the deployment.

At the baseline the app ran on a laptop through a bootstrap script. There were no containers, no pipeline and no deployment setup.

## Architecture and request path

A user opens `https://arbordb.localhost`. ingress-nginx on the kind control-plane node terminates TLS and routes `/api/*` to the API service with the prefix removed. Everything else goes to the frontend service. Since the UI and the API are on one host, the browser makes no cross-origin calls.

The API pod starts the engine as a child process, and the engine keeps its tables and write-ahead log under `/data`, which is a persistent volume. Only one engine process can safely use those files, and most of the Kubernetes choices below come from that.

Prometheus scrapes the API every 15 seconds, Grafana shows the dashboard, and Alertmanager receives the alerts.

## Containers

Both images use multi-stage builds:
- The API image compiles the engine in a build stage, installs production dependencies in another, and copies only the results into the final image.
- The frontend image builds the React app with Node and serves it from `nginx-unprivileged`.

Both run as non-root users and work with a read-only root filesystem. I also added `engine/Makefile` so the engine and its tests build the same way in Docker, CI and locally.

## Kubernetes setup

### API

The API runs as a StatefulSet with one replica. Two engine processes writing the same files would corrupt the data, and two pods with their own volumes would hold different data.

I used a StatefulSet rather than a Deployment because of how pods get replaced:
- A Deployment's default rolling update starts the new pod before stopping the old one. A ReadWriteOnce volume can be mounted by two pods on the same node, so two engines could briefly write the same files.
- `strategy: Recreate` avoids that during a rollout. If a node becomes unreachable, though, a Deployment starts a replacement straight away, while a StatefulSet waits until the old `arbordb-api-0` is confirmed gone.

The StatefulSet also creates the volume through `volumeClaimTemplates` and keeps it across restarts. When I deleted the pod during testing, the data was still there after it came back.

### Frontend

The frontend is a Deployment with two nginx replicas. They are there for availability, not load:
- a topology spread rule keeps them on different nodes
- a PodDisruptionBudget stops a node drain from evicting both
- the rolling update uses `maxUnavailable: 0`

I didn't add an HPA. The API can't scale horizontally, and the frontend sits at about 2% CPU, so an autoscaler would never act. The Bitnami charts for PostgreSQL and MySQL don't autoscale the primary either, and Microsoft's AKS guidance says HPA isn't suitable for single-replica stateful services.

### Probes and resources

| Probe | Endpoint | Purpose |
|---|---|---|
| startup | `/health` | gives the engine up to 60 seconds to start |
| readiness | `/ready` | checks the data directory is writable before traffic is sent |
| liveness | `/health` | restarts the pod if the process stops answering |

`/ready` is new. While testing the probes I found that the app's rate limiter (100 requests per 15 minutes per IP) also covered `/health`. The probes alone send about 180 requests in that window, so Kubernetes would have started getting 429s and restarting a healthy pod. I moved the health and metrics routes in front of the limiter and set `trust proxy`, so the limit counts each client's real IP and not the ingress controller's.

Every container has CPU and memory requests and limits.

## Configuration

Everything is deployed with one Helm chart:
- `values.yaml` has the defaults.
- `values-kind.yaml` has the settings for the local cluster (host name and CORS origin).
- The image tag has no default, and the chart won't render without it, so each release maps to a commit.
- Non-secret settings go in a ConfigMap, and a checksum annotation restarts the API when it changes.

I picked Helm over Kustomize mostly for `helm history` and `helm rollback`, which the release process relies on. Another environment would be another values file.

## CI/CD

The GitHub Actions workflow runs on every push to `main` or a `feature/` branch.

| Stage | What it does |
|---|---|
| Checks, in parallel | API lint and tests, the engine tests, frontend lint and build, and a Trivy scan for vulnerable dependencies and committed secrets |
| Build | Builds each image, tags it with the short commit SHA, and scans it again. It is pushed to GHCR only if that scan passes |
| Deploy | Uses the `production` environment, which needs my approval in GitHub. It runs on a self-hosted runner on the machine with the kind cluster, since GitHub's hosted runners can't reach a cluster on my laptop. It pulls the images, loads them into kind, runs `helm upgrade --atomic`, then a smoke test that checks the version and runs a SQL insert and select |

The first dependency scan found 14 HIGH or CRITICAL issues in multer, joi, proxy-addr, xlsx, axios and form-data, and I upgraded all of them. In CI the image scan then flagged CRITICAL OpenSSL and perl issues in the base images, so I moved to newer Node and nginx images.

A self-hosted runner executes code from the repo on my machine. The workflow has no `pull_request` trigger, so a fork can't start a job there, and the runner deploys with a service account that can only manage the chart's resources in the `arbordb` namespace.

I also looked at Argo CD. For one local cluster, a push-based deploy with an approval step was simpler and covered what I needed. With more clusters I would switch to it, mainly for drift detection.

## Release control

Releases are rolling updates through `helm upgrade --atomic`, which rolls back to the previous revision if the new pods don't become ready in time. The commit SHA appears in the image tag, a pod label, `/api/health`, each log line and the `arbordb_build_info` metric, so the running version can be checked from any of them.

Blue-green and canary don't work for the API, because two versions can't share one data volume. They would work for the static frontend, but a rolling update is enough there.

To test recovery I deployed a release with an image tag that doesn't exist. Helm marked it failed and reverted the StatefulSet template, but the API pod stayed on the bad image, because Kubernetes doesn't replace a StatefulSet pod that never became ready. The frontend kept answering the whole time. I added a step to the pipeline that deletes the stuck pod when a deploy fails, and `scripts/demo-bad-release.sh` reproduces the test.

If a release rolls out but then fails the smoke test, I roll it back by hand with `helm rollback`. Doing that automatically would need the pipeline to know whether Helm had already rolled back.

The cluster is only changed through the pipeline, and the CI account can't touch anything outside its namespace. A manual `kubectl edit` by an admin wouldn't be noticed, though; Argo CD would catch that.

## Observability

I added Prometheus metrics to the API, mainly a request duration histogram labelled by route pattern rather than raw URL to keep the number of series small. The recording rules leave out probe and scrape requests so they don't hide what real users see.

The SLO is 99% of user requests succeeding, with p95 latency under 500 ms. The alerts are based on it:

| Alert | Fires when | Severity |
|---|---|---|
| `ArborDBApiDown` | the API can't be scraped for 2 minutes | critical |
| `ArborDBHighErrorRate` | more than 1% of requests return 5xx over 5 minutes | critical |
| `ArborDBHighLatency` | p95 is above 500 ms for 10 minutes | warning |

Each alert links to its section in `docs/RUNBOOK.md`. I tested `ArborDBApiDown` by scaling the API to zero: it went pending, fired after two minutes, reached Alertmanager, and cleared after I scaled it back. In the bad release test the API was down for about 90 seconds and the alert didn't fire, which is what the two-minute window is for.

Request logs are now one JSON line per request with a request ID and the running version. ingress-nginx generates the ID and passes it on, so the same request can be found in both the ingress and API logs. I didn't add distributed tracing, since there is only one backend service and the request ID already links the two logs.

## Security

| Area | What I did |
|---|---|
| Images | Multi-stage builds, no compiler in the runtime image, non-root users |
| Pods | `runAsNonRoot`, read-only root filesystem, all capabilities dropped, `RuntimeDefault` seccomp, and no service account token, since the app never calls the Kubernetes API |
| Namespace | Pod Security `restricted` is enforced, so a pod that breaks these rules is rejected |
| Network | Default deny for ingress and egress, with DNS allowed. Only the ingress controller can reach the pods, and only Prometheus can reach the API. ArborDB has no login, so anyone who could reach the API could drop a table |
| Secrets | The TLS key and the Grafana password are generated by the setup script and only exist in the cluster. Nothing secret is in git or in the images, and CI scans for it |
| CI | The workflow token is read-only by default, and only the build job can write packages |

## Verification

`scripts/verify.sh` checks the running system and prints a pass or fail for each claim:
- the version matches the commit
- the HTTPS user journey works
- data survives the pod being deleted
- the frontend runs on two nodes
- the security controls are active
- Prometheus has the right data

The output for the deployed version is in `docs/EVIDENCE.md`, along with the pipeline run, the bad release test and the alert test.

## Limitations and next steps

- One API replica means a short outage on every restart or upgrade. Removing that would need replication in the engine.
- TLS uses a self-signed certificate; on a real cluster I would use cert-manager.
- The data volume isn't backed up. A CronJob taking snapshots would come first before real use.
- Logs are read with `kubectl logs`. Loki would centralise them.
- Only the local kind environment is set up, though the chart can take more values files.
