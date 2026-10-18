# Runbook

Start with the ArborDB dashboard in Grafana. The commands below need cluster access.

## ArborDBApiDown

Prometheus hasn't been able to scrape the API for 2 minutes. The UI still loads, but every query fails.

```bash
kubectl -n arbordb get pods -l app.kubernetes.io/name=arbordb-api
kubectl -n arbordb describe pod arbordb-api-0 | tail -20     # events: probes, image pulls, scheduling
kubectl -n arbordb logs arbordb-api-0 --previous             # logs from the crashed container
helm history arbordb -n arbordb --max 3                      # was there a recent release?
```

| Pod state | Likely cause | Fix |
|---|---|---|
| `ErrImagePull` / `ImagePullBackOff` | bad image tag in the last release | `helm rollback arbordb <last good rev> -n arbordb`, then `kubectl -n arbordb delete pod arbordb-api-0` |
| `CrashLoopBackOff` | app fails on start | read the `--previous` logs; roll back if it came with a release |
| `Pending` | PVC not bound or no node has room | `kubectl -n arbordb get pvc`, `kubectl describe pod` |
| Running but not Ready | `/ready` failing, data dir not writable | `kubectl -n arbordb exec arbordb-api-0 -- ls -la /data/tables` |
| Pod fine, target down | NetworkPolicy or ServiceMonitor problem | `kubectl -n arbordb get networkpolicy`; Prometheus Targets page |

It's fixed when `./scripts/smoke-test.sh <version>` passes and the alert clears.

## ArborDBHighErrorRate

More than 1% of user requests returned 5xx over the last 5 minutes.

```bash
# failing requests
kubectl -n arbordb logs arbordb-api-0 --since=10m | grep '"level":"error"'
# the same request in the ingress logs
kubectl -n ingress-nginx logs deploy/ingress-nginx-controller --since=10m | grep <requestId>
```

If the errors started with a release (compare `helm history` with the `version` field in the logs), roll back. Engine errors such as the engine process dying or writes failing can come from a full disk, which `kubectl -n arbordb exec arbordb-api-0 -- df -h /data` shows.

It's fixed when the error panel on the dashboard is back at 0 and the alert clears after 5 minutes.

## ArborDBHighLatency

p95 latency has been above 500 ms for 10 minutes.

To see if the pod is hitting its 500m CPU limit, run this in Prometheus. A line flat at 0.5 means it is being throttled.

```
sum by (pod) (rate(container_cpu_usage_seconds_total{namespace="arbordb",container!=""}[5m]))
```

To find the slow route:

```
histogram_quantile(0.95, sum by (le, route) (rate(http_request_duration_seconds_bucket{job="arbordb-api"}[5m])))
```

Large uploads and queries without an index show up as high `durationMs` values in the logs.

Raise `api.resources.limits.cpu` in the values file and redeploy, or add an index for the slow query. It's fixed when the p95 line on the dashboard is back under 500 ms.
