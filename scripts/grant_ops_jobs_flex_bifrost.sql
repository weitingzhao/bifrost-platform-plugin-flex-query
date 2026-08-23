-- Flex Query worker (bifrost role) needs write access to merged ops_jobs flex tables.
GRANT INSERT, UPDATE, DELETE ON ops_jobs.job_flex_ingest TO bifrost;
GRANT INSERT, UPDATE ON ops_jobs.flex_ingest_freshness TO bifrost;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA ops_jobs TO bifrost;
