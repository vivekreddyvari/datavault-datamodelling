-- Predictive optimization: Always optimize this table after large batches
--  OPTIMIZE ${catalog_name}.${env}_${schema_name}.dv_hub_spec ZORDER BY (hub_name) if massive increments

CREATE TABLE IF NOT EXISTS ${catalog_name}.${env}_${schema_name}.dv_hub_spec (
  hub_name STRING NOT NULL COMMENT 'Hub name (e.g. H_CUSTOMER)',
  entity_name STRING NOT NULL COMMENT 'Entity name',
  driving_dataset STRING NOT NULL COMMENT 'Source dataset for this hub',
  bk_columns STRING NOT NULL COMMENT 'Business key columns',
  target_schema STRING NOT NULL COMMENT 'Target schema (e.g. SilverDataVault{env}_dv_hub)',
  target_table STRING NOT NULL COMMENT 'Target table name',
  hk_column STRING NOT NULL COMMENT 'Hash key column name',
  hash_storage STRING NOT NULL COMMENT 'How hash key is stored',
  is_active BOOLEAN NOT NULL COMMENT 'Whether this hub is active',
  generated_at TIMESTAMP NOT NULL COMMENT 'When spec was generated',
  run_id STRING NOT NULL COMMENT 'Generator run ID',
  PRIMARY KEY (hub_name)
)
USING DELTA
CLUSTER BY (hub_name) -- Liquid clustering on PK column for Databricks Runtime 13+
COMMENT 'Hub name (e.g. H_CUSTOMER)'