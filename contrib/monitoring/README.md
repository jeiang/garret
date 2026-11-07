# Monitoring

- `garret.rules.yml`: recording and alert rules (Prometheus rule format).
- `garret-dashboard.json`: Grafana dashboard with a `Datasource` and an
  `Instance` variable.

Metric names and labels are the `garret_*` series each service exports at
`/metrics`; [docs/operations.md](../../docs/operations.md) covers the listeners
and the readiness probes. The rules
assume each service is scraped under a job whose name starts with `garret`
(for example `garret-pusher` on port 9091 and `garret-puller` on port 9092).
If your jobs use other names, edit the `GarretDown` selector.

## VictoriaMetrics

Pass the rules file to `vmalert` and point it at the datasource:

    vmalert -rule=garret.rules.yml \
      -datasource.url=http://victoriametrics:8428 \
      -notifier.url=http://alertmanager:9093 \
      -remoteWrite.url=http://victoriametrics:8428

`-remoteWrite.url` stores the recording rules. Plain Prometheus loads the same
file through `rule_files`.

## Grafana

Import `garret-dashboard.json` (Dashboards, New, Import) and choose the
Prometheus-compatible datasource that holds the Garret series.

## Deep readiness

Alerts on the S3 read path use `garret_s3_read_probes_total`, which only moves
when something requests `/ready/deep` on the Puller's public listener. Probe
that path, not `/ready`, every 30 s or more (a blackbox exporter or an uptime
check). Without a prober `GarretDeepReadinessFailing` never fires.

## Validation

`nix flake check` runs `promtool check rules` on the rules file and `jq empty`
on the dashboard.
