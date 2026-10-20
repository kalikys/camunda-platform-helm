# Automatic Capacity Manager

The capacity manager is an opt-in chart 8.11 component that coordinates physical orchestration capacity
with Camunda's dynamic cluster topology.

It supports three policy modes:

- `recommend` observes and reports a desired broker count without applying it.
- `scheduled` applies an explicit target or an RFC3339 capacity window.
- `automatic` scales on named PromQL signals evaluated against a Prometheus server
  (`capacityManager.prometheusURL`) that scrapes every broker.

## Automatic Scaling Signals

Partitions are fixed, so another broker only helps when three things are true together: the cluster
is saturated, the brokers themselves are the bottleneck, and there is a partition replica for the new
broker to host. Scale-down needs the reverse: no saturation, and the current work fits on one broker
fewer. Throughput (`zeebe_log_appender_record_appended_total`) and `zeebe_flow_control_partition_load`
describe load, not saturation; a partition at its write limit needs more partitions, not brokers.

The policy therefore declares `scaleUpSignals` (all must hold), `scaleDownSignals` (all must hold)
and `vetoSignals` (any blocks every change). Missing data or a failed query holds and restarts
stabilization. The planner also caps scale-up at the topology's partition replica count and waits a
`cooldown` after each change. A recommended policy, supplied through
`capacityManager.extraConfiguration`:

```yaml
capacityManager:
  enabled: true
  replicaOwnership: capacityManager
  prometheusURL: http://prometheus.monitoring:9090
  extraConfiguration:
    - file: policy.json
      content: |
        {
          "mode": "automatic", "minBrokers": 1, "maxBrokers": 3,
          "scaleUpSamples": 3, "scaleDownSamples": 6,
          "scaleDownStabilization": "5m", "cooldown": "2m",
          "scaleUpSignals": [
            {"name": "backpressure", "operator": ">=", "threshold": 0.05,
             "query": "max(sum by (partition) (rate(zeebe_dropped_request_count_total{namespace=\"$NAMESPACE\",pod=~\"$STATEFULSET-[0-9]+\"}[2m])) / clamp_min(sum by (partition) (rate(zeebe_received_request_count_total{namespace=\"$NAMESPACE\",pod=~\"$STATEFULSET-[0-9]+\"}[2m])), 1))"},
            {"name": "broker-cpu", "operator": ">=", "threshold": 0.75,
             "query": "max(avg_over_time(process_cpu_usage{namespace=\"$NAMESPACE\",pod=~\"$STATEFULSET-[0-9]+\"}[2m]))"}
          ],
          "scaleDownSignals": [
            {"name": "no-backpressure", "operator": "<=", "threshold": 0.001,
             "query": "max(sum by (partition) (rate(zeebe_dropped_request_count_total{namespace=\"$NAMESPACE\",pod=~\"$STATEFULSET-[0-9]+\"}[2m])) / clamp_min(sum by (partition) (rate(zeebe_received_request_count_total{namespace=\"$NAMESPACE\",pod=~\"$STATEFULSET-[0-9]+\"}[2m])), 1))"},
            {"name": "fits-on-fewer-brokers", "operator": "<=", "threshold": 0.5,
             "query": "sum(avg_over_time(process_cpu_usage{namespace=\"$NAMESPACE\",pod=~\"$STATEFULSET-[0-9]+\"}[5m])) / ($BROKERS - 1)"}
          ],
          "vetoSignals": [
            {"name": "exporter-backlog", "operator": ">=", "threshold": 100000,
             "query": "max(max by (partition) (zeebe_log_appender_last_committed_position{namespace=\"$NAMESPACE\",pod=~\"$STATEFULSET-[0-9]+\"}) - min by (partition) (max by (partition, exporter) (zeebe_exporter_last_exported_position{namespace=\"$NAMESPACE\",pod=~\"$STATEFULSET-[0-9]+\"}))) or vector(0)"},
            {"name": "partition-unhealthy", "operator": "<=", "threshold": 0,
             "query": "min(zeebe_health{namespace=\"$NAMESPACE\",pod=~\"$STATEFULSET-[0-9]+\"})"}
          ]
        }
```

The queries assume Prometheus attaches `namespace` and `pod` labels, as ServiceMonitor and
Kubernetes pod discovery do. See `scripts/capacity-manager/README.md` for why each signal is used and
for optional partition-balance and disk vetoes.

## Safety Model

For scale-up, the manager starts the next StatefulSet pod before dry-running and applying a Camunda
cluster topology change. For scale-down, it evacuates the broker and waits for the topology change to
complete before reducing StatefulSet replicas.

The component does not change partition count. Size logical partitions for expected peak throughput;
the manager spreads those partitions across more brokers during peaks and consolidates them off peak.

Capacity management is disabled by default and currently supports fresh single-region deployments
with `orchestration.clusterSize: "1"`. Set `capacityManager.replicaOwnership: capacityManager` to
explicitly transfer StatefulSet replica ownership from Helm. Advisor-only mode leaves the value empty
and retains normal Helm ownership.

Returning ownership is an explicit handback using `capacityManager.replicaOwnership: helm`. It
requires live cluster access and succeeds only when the durable target, durable completion marker,
live replicas, and configured cluster size all match. Offline handback is rejected. Rolling back to a
chart version that predates this contract is unsupported until the cluster has been safely contracted.

## Operator Integration

The implementation under `scripts/capacity-manager` is an importable Go package. Kubernetes access,
Camunda topology access, policy loading, pressure measurement, and planning are interfaces. A future
operator can provide a CRD-backed policy source and status adapter while reusing the same planner and
reconciliation state machine.

## Validation

The `capacity-manager` GKE scenario deploys a namespace-scoped Prometheus that discovers the
orchestration pods, runs three partitions on one CPU-limited broker, and ramps a load generator until
the request limiter rejects commands. The verifier asserts that scale-up is driven by the
`backpressure` and `broker-cpu` signals, that the drop ratio falls after the new broker joins, and that
scale-down happens only after the load stops and the work fits on fewer brokers.

## Partition Advisor

The recommendation-only partition advisor can be enabled independently of broker autoscaling. It
uses the runtime topology's unique partition count and a sustained per-partition load reading to
recommend a small partition increase. Recommendations never call the partition-scaling API.

Partition load is the right input here, unlike for broker scaling: a partition that is at its write
throughput limit cannot be relieved by more brokers, only by more partitions. The advisor reads
`partitionAdvisor.loadQuery`, a PromQL query evaluated against `capacityManager.prometheusURL` with the
same `$NAMESPACE`/`$STATEFULSET`/`$BROKERS` placeholders, for example the busiest partition's appended
record rate:

```promql
max(sum by (partition) (rate(zeebe_log_appender_record_appended_total{namespace="$NAMESPACE",pod=~"$STATEFULSET-[0-9]+"}[1m])))
```

or `max(zeebe_flow_control_partition_load{...})` when write-rate flow control is enabled.

The GKE advisor scenario runs one broker and one partition under a 50 process-instances-per-second
noop workload, with the same in-namespace Prometheus as the capacity scenario. A per-partition
appended-record rate above 20 records per second for three consecutive samples produces an
incremental recommendation. The verifier asserts that StatefulSet replicas, active broker count,
runtime partition count, pending topology change, and last topology change ID remain unchanged.
Advisor-only RBAC cannot patch StatefulSets.
