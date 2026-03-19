-- Predictive optimization: Always optimize this table after large batches
--  OPTIMIZE ${catalog_name}.${env}_${schema_name}.dv_object_audit ZORDER BY (run_id) if massive increments

CREATE TABLE IF NOT EXISTS ${catalog_name}.${env}_${schema_name}.dv_object_audit (
  run_id STRING NOT NULL COMMENT 'Generator run ID',
  object_type STRING NOT NULL COMMENT 'ENUM: HUB | LINK | SAT',
  object_name STRING NOT NULL COMMENT 'Object name',
  target_table STRING NOT NULL COMMENT 'Target table name',
  driving_dataset STRING NOT NULL COMMENT 'Source dataset',
  rows_read BIGINT NOT NULL COMMENT 'Number of rows read',
  rows_inserted BIGINT NOT NULL COMMENT 'Number of rows inserted',
  rows_updated BIGINT NOT NULL COMMENT 'Number of rows updated',
  started_at TIMESTAMP NOT NULL COMMENT 'When load started',
  finished_at TIMESTAMP NOT NULL COMMENT 'When load finished',
  status STRING NOT NULL COMMENT 'Load status',
  reason STRING NOT NULL COMMENT 'Reason if failed',
  PRIMARY KEY (run_id)
)
USING DELTA
CLUSTER BY (run_id) -- Liquid clustering on PK column for Databricks Runtime 13+
COMMENT 'Generator run ID'