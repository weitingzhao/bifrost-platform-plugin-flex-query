# bifrost-platform-plugin-flex-query

Bifrost **IB Flex Query** subcontractor — scheduled Flex Web Service ingest into
`bifrost_golden_source.brokerage.*`.

## Slots

| Slot | Kind | Cron (UTC, weekdays) |
|------|------|----------------------|
| flex-trades | `flex-trades` | `30 22 * * 1-5` |
| flex-transactions | `flex-transactions` | `0 23 * * 1-5` |

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
  schema/      # flex_ops DDL
  scheduler/   # CronJob enqueue
  worker/      # claim + dispatch
  api/         # FastAPI :8791
k8s/base/      # Namespace, Deployments, CronJobs
```
