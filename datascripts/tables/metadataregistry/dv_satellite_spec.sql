-- Predictive optimization: Always optimize this table after large batches
--  OPTIMIZE ${catalog_name}.${env}_${schema_name}.dv_satellite_spec ZORDER BY (sat_name) if massive increments

CREATE TABLE IF NOT EXISTS ${catalog_name}.${env}_${schema_name}.dv_satellite_spec (
  sat_name STRING NOT NULL COMMENT 'Satellite name (e.g. S_CUSTOMER)',
  parent_type STRING NOT NULL COMMENT 'Type of parent (hub or link)',
  parent_name STRING NOT NULL COMMENT 'Parent hub or link name',
  driving_dataset STRING NOT NULL COMMENT 'Source dataset for this satellite',
  parent_hk_column STRING NOT NULL COMMENT 'Parent hash key column name',
  attribute_columns STRING NOT NULL COMMENT 'Attribute column names',
  target_schema STRING NOT NULL COMMENT 'Target schema',
  target_table STRING NOT NULL COMMENT 'Target table name',
  mode STRING NOT NULL COMMENT 'Satellite mode',
  is_active BOOLEAN NOT NULL COMMENT 'Whether this satellite is active',
  generated_at TIMESTAMP NOT NULL COMMENT 'When spec was generated',
  run_id STRING NOT NULL COMMENT 'Generator run ID',
  PRIMARY KEY (sat_name)
)
USING DELTA
CLUSTER BY (sat_name) -- Liquid clustering on PK column for Databricks Runtime 13+
COMMENT 'Satellite name (e.g. S_CUSTOMER)'