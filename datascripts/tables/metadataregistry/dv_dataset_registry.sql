-- Predictive optimization: Always optimize this table after large batches
--  OPTIMIZE ${catalog_name}.${env}_${schema_name}.dv_dataset_registry ZORDER BY (dataset_name) if massive increments

CREATE TABLE IF NOT EXISTS ${catalog_name}.${env}_${schema_name}.dv_dataset_registry (
  dataset_name STRING NOT NULL COMMENT 'Name of the Data set',
  dataset_type STRING NOT NULL COMMENT 'Type of dataset source',
  location STRING NOT NULL COMMENT 'Location of the dataset (Volume path or UC table name)',
  load_type STRING NOT NULL COMMENT 'Full load or Change Data Capture',
  rsrc STRING NOT NULL COMMENT 'Record source identifier (e.g. CRM_CUSTOMERS)',
  cdc_op_col STRING NOT NULL COMMENT 'Column name for CDC operator (I/U/D) if CDC load',
  cdc_ts_col STRING NOT NULL COMMENT 'Commit timestamp column if CDC load',
  is_active BOOLEAN NOT NULL COMMENT 'Whether to process this dataset',
  created_at TIMESTAMP NOT NULL COMMENT 'When the registry entry was created',
  PRIMARY KEY (dataset_name)
)
USING DELTA
CLUSTER BY (dataset_name) -- Liquid clustering on PK column for Databricks Runtime 13+
COMMENT 'Name of the Data set'