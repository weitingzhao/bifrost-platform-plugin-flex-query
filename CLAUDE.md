# CLAUDE.md — bifrost-platform-plugin-flex-query

与本项目用户的所有对话一律使用中文回复；UI 字符串与代码标识符使用 English。

## 工作区定位（2026-09-06）

| 项 | 值 |
|---|---|
| 域 / 载荷 | Ops · Subcontractor（供数插件）· IB Flex Query → `raw_broker.*`（Golden Source） |
| 运行位置 | K3s `plugin-flex-query` NS，API `:8791`（Trade 经 `/api/plugin/flex-query/`）；`bifrost-build-flex-query` 流水线 |
| 秘密 | Flex token 在未跟踪的 `flex-tokens-secret.yaml` / `.env` 里，永不入库；账户号本身不是秘密 |
| 仓库可见性 | GitHub **PUBLIC**（12 个 repo 全部公开）—— `.env`、Secret YAML、dump、kubeconfig、账户内容永不入库 |
| 硬边界 | D10 交易执行冻结（BLOCKED）· D13 三域边界 · 平台/业务解耦（Flywheel A/B） |
| 事实基线 | `../AGENT_FACTS.md`（§8c 运行时与安全事实）· 规则 `../CLAUDE.md`（§8 Claude Code 运行配置） |

会话请在工作区根 `/stocks` 启动（加载治理层 hooks / auto mode / 共享记忆）；运行时与安全事实以 `../AGENT_FACTS.md` §8c 为准。

## 职责

**`bifrost-flex-query`** — Bifrost Ops Platform 的 **IB Flex Query Subcontractor**。
每天收盘后从 IB Flex Web Service 拉取 Trades / Cash Transactions，写入
每天收盘后从 IB Flex Web Service 拉取 Trades / Cash Transactions，写入
`bifrost_golden_source.raw_broker.*`。

| 组件 | 说明 |
|------|------|
| 触发 | **Dagster** `research_flex_morning_schedule`（bifrost-research，06:30 ET 周一–周六）→ `POST /flex/ingest/enqueue`；本 repo 自 0.6.0 起**无 CronJob**，`config/schedule.yaml` 仅镜像该 cron 供看板计算计划/偏差 |
| Worker | `SELECT FOR UPDATE SKIP LOCKED` 认领（`not_before` 到期才可认领）→ 调用本包 Flex orchestration；失败按 `worker/retry.py` 分类：未就绪/限流 → 延后 30 分钟重试，配置类 → 直接 failed |
| API | `:8791` — `/health`, `/metrics`, `/flex/ingest/*`, `/flex/config/*`, `/flex/coverage/*`, `/flex/dashboard/*` |

### 0.6.0 行为约定（改动前先读）

- 排队任务 `payload.fallback=false`：IB 报表未生成就等（1003/1004/1019 → `not_before`），不再回退到"查询默认周期 / 最近 365 天"；那条回退链只保留给手动运行。
- 1018 限流 → 本任务与**所有 pending** 任务一起延后（token 级冷却）。
- 单账户失败 = 任务未完成（upsert 幂等，重试免费）；`flex_ingest_freshness.latest_ts` 只在完整成功时前移，失败只写 `last_ok/last_error`。
- 手动触发（Trade UI 按钮 / XML 上传）同步执行，但也落一行 job（`payload.manual=true`）与 outcome。
- Worker 用 `clock_timestamp()`，空轮询后 `rollback()`；Postgres 断连自动重连。
- `/metrics` 由 `bifrost-trade-infra/k8s/monitoring` 的 ServiceMonitor 抓取，告警 `BifrostFlexIngest*`。
- `client/flex_client.py`：Trades 的 `dateTime` 保留时分秒；仅日期时取 `FLEX_LOCAL_TZ`（默认 America/New_York）当日零点。Cash transactions 的 `ts` 因是 UNIQUE 键的一部分**保持原样**。

## 架构边界

- **Platform core** (`bifrost-platform`): 通用环境治理 — Console proxy `/plugins/flex-query/*`
- **本 repo**: 独立进程、独立 K8s namespace `plugin-flex-query`；Flex HTTPS 客户端 + 编排引擎内化在 `bifrost_flex_query.client` / `orchestration`
- **Trade** (`bifrost-trade-*`): 手动 Flex 按钮走 Trade gateway `/api/plugin/flex-query` → 本 Plugin（配置写入 `POST /flex/config/write`）
- **数据**: 写 `raw_broker.executions_raw_flex` / `raw_broker.transactions`；队列在 Golden Source `ops_jobs.*`
- **Trade DB 仅配置**: `public.settings` Flex token（Wave 4: **deprecated fallback**）；`brokerage.settings_flex` FDW 读 query id — **不在 Trade DB 建 flex_ops**
- **flex_ops.***: **DEPRECATED** (Wave 6.3) compat views on Golden Source → use `ops_jobs.*` directly

## Token source (Wave 11 / 0.5.1)

Read priority for Flex tokens:

1. Env `FLEX_HOST_TOKEN` / `FLEX_SECONDARY_TOKEN` (K8s Secret `bifrost-flex-tokens`, `envFrom`)
2. Empty (`none`)

`GET /flex/config/summary` exposes `source` (`secret` | `none`). Token writes via `/flex/config/write` require Secret env; Trade DB token columns were dropped in core **0.18.0**.

## 依赖

```
bifrost-flex-query
  └── bifrost-trade-core   (write_account_executions_to_db / upsert_account_transactions / connection helpers)
```

Flex token / query_id 由本包 `orchestration.config_rw` 读写（env Secret + `brokerage.settings_flex` query rows）。

## 命令

```
make install-dev
make lint
make test
make db-init
# Legacy Trade DB cleanup (if flex_ops was ever created on bifrost_dev):
#   psql -U postgres -d bifrost_dev -f scripts/drop_trade_flex_ops_legacy.sql
# Golden Source flex_ops compat views (DEPRECATED Wave 6.3 — use ops_jobs.* directly)
#   psql -U postgres -d bifrost_golden_source -f scripts/golden_source_flex_ops_compat_views.sql
make run-api    # :8791
```

## 修改纪律

- 公开 Flex 队列 / coverage 契约变更需同步 Ops Console catalog
- D10 BLOCKED — 不涉及交易执行路径
