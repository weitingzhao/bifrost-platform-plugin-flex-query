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
- 0.6.1 操作闭环：`GET /flex/ops/check` 自检（判决 + 下一步 + 可按的动作，`ops/diagnose.py` 纯函数）；`POST /flex/ingest/jobs/{id}/run-now` 跳过延后；worker 空闲时对"计划已过 45 分钟仍无任务"的槽自行补入队（按天去重）；worker 心跳表 `ops_jobs.flex_worker_heartbeat`；手动触发默认每账户 1 次请求（`fallback: true` 才放宽）。
- `client/flex_client.py`：Trades 的 `dateTime` 保留时分秒并**按 `FLEX_LOCAL_TZ`（默认 America/New_York）墙钟解释**（0.6.2 起；此前当 UTC 解析，Flex 成交时间偏早 4–5 小时，重拉窗口即更正）；仅日期时取该时区当日零点。Cash transactions 的 `ts` 因是 UNIQUE 键的一部分**保持原样**。
- `get_statement`（0.6.3 起）：只有含 `<FlexStatement>` 的 `<FlexQueryResponse>` 才算报表已生成；`<FlexStatementResponse>`（Warn **或** Fail，1019 生成中是 Warn）、非 XML、无 FlexStatement 都在原轮询预算内重试，耗尽后抛出带 `[ErrorCode]` 的 ValueError 交给 `worker/retry.py` 分类。此前只认 Fail，新窗口首跑会把 1019 当成空窗口记 `ok, 0`；无成交行的 FlexStatement 仍是合法空窗口。
- Cash transactions 窗口（0.10.0 起，TD-88）：`from = min(各账户最后一笔已存现金流日, 昨天 - default days)`，只算有 Flex 成交的账户；超过 IB 单次跨度的窗口按 ≤364 天分段，显式 `from_date`/`to_date` 照样生效并分段。此前固定取最近 default days，停摆超过 30 天的月份永远补不回来（2026-03..07 就是这样丢的）。`/flex/ops/check` 的 `coverage` 项与 `/metrics` 的 `bifrost_flex_coverage_gap_months{account}` 报告「有成交、无现金流」的已结束月份。

## 架构边界

- **Platform core** (`bifrost-platform`): 通用环境治理 — Console proxy `/plugins/flex-query/*`
- **本 repo**: 独立进程、独立 K8s namespace `plugin-flex-query`；Flex HTTPS 客户端 + 编排引擎内化在 `bifrost_flex_query.client` / `orchestration`
- **Trade** (`bifrost-trade-*`): 手动 Flex 按钮走 Trade gateway `/api/plugin/flex-query` → 本 Plugin（配置写入 `POST /flex/config/write`）
- **数据**: 写 `raw_broker.executions_raw_flex` / `raw_broker.transactions`；队列在 Golden Source `ops_jobs.*`
- **只连 Golden Source（0.11.0 起，TD-116）**：query id 读 `raw_broker.settings_flex`、range days 读 `ops_jobs.flex_settings`（单行，与 `raw_broker.settings_flex` 同事务写，TD-74；无行时用默认 30/360）、成交统计读 `raw_broker.executions_raw_flex`（按 `trade_date`）。读失败一律抛错、任务失败重试，**不再回落**（此前经 `bifrost_dev` 的 FDW 视图读，统计读失败会当成 0 行把 trades 扩成 270 天 init 窗口）。导入后的 `last_flex_date_after` 在写入提交后另开连接读（TD-117）。ConfigMap / Deployment 不再有 `trade_postgres` / `FLEX_TRADE_PG_*`；Secret 里的 `trade-pg-*` 键不再被读。Flex token 只在 K8s Secret（Wave 11）— **不连任何 Trade env DB**
## Token source (Wave 11 / 0.5.1)

Read priority for Flex tokens:

1. Env `FLEX_HOST_TOKEN` / `FLEX_SECONDARY_TOKEN` (K8s Secret `bifrost-flex-tokens`, `envFrom`)
2. Empty (`none`)

`GET /flex/config/summary` exposes `source` (`secret` | `none`), the masked last four and `issued_at` / `age_days`. **0.8.0 (TD-83):** `POST /flex/config/write` answers **409** for any `host_token` / `secondary_token` key and never echoes a token; tokens are set only with `make sync-flex-tokens` (Secret `bifrost-flex-tokens`). Trade DB token columns were dropped in core **0.18.0**.

## 依赖

```
bifrost-flex-query
  └── bifrost-trade-core   (write_account_executions_to_db / upsert_account_transactions / connection helpers)
```

Flex token / query_id 由本包 `orchestration.config_rw` 读写（env Secret + Golden Source `raw_broker.settings_flex` query rows）。

## 命令

```
make install-dev
make lint
make test
make test-db    # db-marked tests on a throwaway postgres in Docker (TD-117)
make db-init
make run-api    # :8791
```

## 发版检查

- 版本：`pyproject.toml` 与 `src/bifrost_flex_query/__init__.py` 同步 bump；`make docker-build` 的镜像 tag 直接读 `pyproject.toml`。
- **兄弟目录的 core checkout 必须干净、且在预期的提交上再构建**：`make docker-build` 用 `--build-context core=../bifrost-trade-core`
  （可用 `CORE_DIR=` 覆盖），按磁盘原样安装 core——别的会话未提交、未跟踪的改动都会进镜像。目标会在 `git status --porcelain`
  非空时拒绝构建，并把 core 的 commit 写进镜像 label `io.bifrost.core.sha`；发版说明里记下这个 SHA。
- core 下限 `bifrost-core>=0.58.0`（`upsert_account_transactions` 返回 `(written, skipped)` 并在写失败时抛错，TD-91；
  `get_conn_params` 自 0.34.0）。import 了更新的 core 名字时同步抬高（TD-37）。
- **只用 core 的规范模块路径**（0.8.1，TD-80 C1-a）：accounts 写函数从 `bifrost_core.portfolio.reader.accounts` 导入，
  不经 `bifrost_core.monitor.reader` 的包级再导出；`tests/test_core_alias_imports.py` 拦回退。core 0.46.0（C1-b）删这些别名，
  所以兄弟目录的 core 一旦到 0.46.0，只有 0.8.1 及以后的 flex checkout 能构建（更早的在 import 时失败）。

## 修改纪律

- 公开 Flex 队列 / coverage 契约变更需同步 Ops Console catalog
- D10 BLOCKED — 不涉及交易执行路径
