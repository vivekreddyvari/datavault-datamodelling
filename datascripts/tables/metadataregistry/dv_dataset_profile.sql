-- Predictive optimization: Always optimize this table after large batches
--  OPTIMIZE ${catalog_name}.${env}_${schema_name}.dv_dataset_profile ZORDER BY (dataset_name) if massive increments

CREATE TABLE IF NOT EXISTS ${catalog_name}.${env}_${schema_name}.dv_dataset_profile (
  dataset_name STRING NOT NULL COMMENT 'Name of the dataset',
  profiled_at TIMESTAMP NOT NULL COMMENT 'When profile was created',
  columns_json STRING NOT NULL COMMENT 'Full column list with types',
  row_count BIGINT NOT NULL COMMENT 'Approximate row count',
  approx_distinct_json STRING NOT NULL COMMENT 'Per-column cardinality',
  null_pct_json STRING NOT NULL COMMENT 'Per-column nullability percentage',
  run_id STRING NOT NULL COMMENT 'Generator run ID',
  PRIMARY KEY (dataset_name)
)
USING DELTA
CLUSTER BY (dataset_name) -- Liquid clustering on PK column for Databricks Runtime 13+
COMMENT 'Name of the dataset'