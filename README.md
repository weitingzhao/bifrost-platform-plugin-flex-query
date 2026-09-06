# bifrost-platform-plugin-flex-query

Bifrost **IB Flex Query** subcontractor — scheduled Flex Web Service ingest into
`bifrost_golden_source.raw_broker.*`.

## Slots

| Slot | Kind | Planned (America/New_York) | Attempts |
|------|------|----------------------------|----------|
| flex-trades | `flex-trades` | `30 6 * * 1-6` | 8 |
| flex-transactions | `flex-transactions` | `30 6 * * 1-6` | 8 |

The trigger is **Dagster** (`bifrost-research` `research_flex_morning_schedule`,
06:30 ET Mon–Sat) calling `POST /flex/ingest/enqueue`; this repo ships no
CronJob since 0.6.0. `config/schedule.yaml` mirrors that cron for the
dashboard's plan/adherence — change both together.

Why 06:30 ET: IB generates the previous day's Activity Flex statement
overnight. The old 22:30 ET slot met `[1003] Statement is not available` every
night, and the immediate fallback chain earned `[1018] Too many requests`.

## Retry semantics (0.6.0)

A failed attempt is classified from IB's error code (`worker/retry.py`):

| Category | Codes / markers | Plan |
|----------|-----------------|------|
| `not_ready` | 1003 1004 1005–1009 1019 1021 | `pending`, `not_before = now + 30 min` |
| `throttled` | 1018 | same, **and every pending job waits 30 min** |
| `transient` | network / Postgres | retry in 2 min |
| `config` | 1010–1017 1020, missing credentials | `failed` immediately |
| `unknown` | anything else | retry in 5 min |

Queued jobs run with `fallback: false` — a statement that is not ready is
waited for, not widened to "query default" / "last 365 days". A run where one
account failed is not `done` (the upsert makes the retry free). Stale
`running` jobs are requeued, not buried.

## Observability

- `GET /metrics` — Prometheus text: last success per kind, last job status /
  category, next retry, queue counts, planned fires, data age, token age.
  Scraped by the `bifrost-flex-query` ServiceMonitor; alerts live in
  `bifrost-trade-infra/k8s/monitoring/bifrost-alerting-rules.yaml`.
- `GET /flex/dashboard/freshness-kpis` — `last_run` is the newest **job**
  (status, error, category, `next_retry_at`); `dimensions[]` carries
  `last_ok` / `last_error` / `new_rows` per kind.
- Manual runs (`/flex/ingest/trigger`, `/flex/ingest/upload-xml`) leave a job
  row (`payload.manual = true`) and an outcome like any queued run.

## Quick start

```bash
make install-dev
make lint
make test
```

## Layout

```
src/bifrost_flex_query/
  client/      # IB Flex HTTPS + XML
  orchestration/  # trades/cash fetch, config R/W
  schema/      # ops_jobs flex ingest DDL (Golden Source)
  scheduler/   # CronJob enqueue
  worker/      # claim + dispatch
  api/         # FastAPI :8791
k8s/base/      # Namespace, Deployments, CronJobs
```
