-- Predictive optimization: Always optimize this table after large batches
--  OPTIMIZE ${catalog_name}.${env}_${schema_name}.dv_run_audit ZORDER BY (run_id) if massive increments

CREATE TABLE IF NOT EXISTS ${catalog_name}.${env}_${schema_name}.dv_run_audit (
  run_id STRING NOT NULL COMMENT 'Unique run identifier (UUID or sequence)',
  started_at TIMESTAMP NOT NULL COMMENT 'When the run started',
  finished_at TIMESTAMP NOT NULL COMMENT 'When the run finished',
  status STRING NOT NULL COMMENT 'Run status',
  reason STRING NOT NULL COMMENT 'Reason if failed',
  config_snapshot STRING NOT NULL COMMENT 'Generator configuration snapshot',
  executed_by STRING NOT NULL COMMENT 'Who executed the run',
  job_id STRING NOT NULL COMMENT 'Associated job ID',
  PRIMARY KEY (run_id)
)
USING DELTA
CLUSTER BY (run_id) -- Liquid clustering on PK column for Databricks Runtime 13+
COMMENT 'Unique run identifier (UUID or sequence)'