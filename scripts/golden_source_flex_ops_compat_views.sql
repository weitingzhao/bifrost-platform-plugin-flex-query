-- Compatibility views on bifrost_golden_source after ops_jobs pipeline migration.
-- Maps legacy flex_ops.* names to ops_jobs.* (Flex Query Plugin queue + freshness).
-- Idempotent; safe to re-run.
--
-- Full legacy compat (market, brokerage, data_ops, …): bifrost-research/scripts/apply_compat_views.py

CREATE SCHEMA IF NOT EXISTS flex_ops;

CREATE OR REPLACE VIEW flex_ops.job_flex_ingest AS
  SELECT * FROM ops_jobs.job_flex_ingest;

CREATE OR REPLACE VIEW flex_ops.flex_ingest_freshness AS
  SELECT * FROM ops_jobs.flex_ingest_freshness;

-- Historical name before flex_ingest_freshness rename
CREATE OR REPLACE VIEW flex_ops.ingest_freshness AS
  SELECT * FROM ops_jobs.flex_ingest_freshness;

GRANT USAGE ON SCHEMA flex_ops TO analytics_writer, analytics_reader, bifrost;
GRANT SELECT ON ALL TABLES IN SCHEMA flex_ops TO analytics_writer, analytics_reader, bifrost;
