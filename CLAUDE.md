# CLAUDE.md — bifrost-platform-plugin-flex-query

与本项目用户的所有对话一律使用中文回复；UI 字符串与代码标识符使用 English。

## 职责

**`bifrost-flex-query`** — Bifrost Ops Platform 的 **IB Flex Query Subcontractor**。
每天收盘后从 IB Flex Web Service 拉取 Trades / Cash Transactions，写入
`bifrost_golden_source.brokerage.*`。

| 组件 | 说明 |
|------|------|
| CronJob | `flex-trades` / `flex-transactions` → enqueue `flex_ops.job_flex_ingest` |
| Worker | `SELECT FOR UPDATE SKIP LOCKED` 认领 → 调用本包 Flex orchestration |
| API | `:8791` — `/health`, `/flex/ingest/*`, `/flex/config/*`, `/flex/coverage/*` |

## 架构边界

- **Platform core** (`bifrost-platform`): 通用环境治理 — Console proxy `/plugins/flex-query/*`
- **本 repo**: 独立进程、独立 K8s namespace `plugin-flex-query`；Flex HTTPS 客户端 + 编排引擎内化在 `bifrost_flex_query.client` / `orchestration`
- **Trade** (`bifrost-trade-*`): 手动 Flex 按钮走 Trade gateway `/api/plugin/flex-query` → 本 Plugin（配置写入 `POST /flex/config/write`）
- **数据**: 写 `brokerage.executions_raw_flex` / `brokerage.transactions`；队列在 `flex_ops.*`

## 依赖

```
bifrost-flex-query
  └── bifrost-trade-core   (write_account_executions_to_db / upsert_account_transactions / connection helpers)
```

Flex token / query_id 由本包 `orchestration.config_rw` 读写
(`public.settings` + `brokerage.settings_flex`)。

## 命令

```
make install-dev
make lint
make test
make db-init
make run-api    # :8791
```

## 修改纪律

- 公开 Flex 队列 / coverage 契约变更需同步 Ops Console catalog
- D10 BLOCKED — 不涉及交易执行路径
