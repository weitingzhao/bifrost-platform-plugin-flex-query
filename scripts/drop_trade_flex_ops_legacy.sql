-- Drop legacy flex_ops on per-env Trade databases (bifrost_dev / bifrost_stg / bifrost_prod).
-- Authoritative Flex ingest queue lives on bifrost_golden_source.ops_jobs.* only.
-- Run as CNPG superuser after confirming no active workers point at Trade DB for flex_ops.
--
-- Example:
--   psql -U postgres -d bifrost_dev -f scripts/drop_trade_flex_ops_legacy.sql

DROP SCHEMA IF EXISTS flex_ops CASCADE;
