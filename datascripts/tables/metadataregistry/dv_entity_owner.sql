-- Predictive optimization: Always optimize this table after large batches
--  OPTIMIZE ${catalog_name}.${env}_${schema_name}.dv_entity_owner ZORDER BY (entity_name) if massive increments

CREATE TABLE IF NOT EXISTS ${catalog_name}.${env}_${schema_name}.dv_entity_owner (
  entity_name STRING NOT NULL COMMENT 'Entity name (e.g. Customer)',
  owner_dataset STRING NOT NULL COMMENT 'Dataset that owns this entity attributes',
  hub_name STRING NOT NULL COMMENT 'Hub name override (e.g. H_CUSTOMER)',
  bk_columns STRING NOT NULL COMMENT 'Business key columns (JSON array or CSV list)',
  satellite_name STRING NOT NULL COMMENT 'Satellite name override (e.g. S_CUSTOMER)',
  satellite_mode STRING NOT NULL COMMENT 'Satellite attribute mode',
  attr_allowlist STRING NOT NULL COMMENT 'JSON list of allowed attributes',
  attr_denylist STRING NOT NULL COMMENT 'JSON list of attributes to ignore',
  is_active BOOLEAN NOT NULL COMMENT 'Whether this entity is active',
  created_at TIMESTAMP NOT NULL COMMENT 'When the entity ownership was created',
  PRIMARY KEY (entity_name)
)
USING DELTA
CLUSTER BY (entity_name) -- Liquid clustering on PK column for Databricks Runtime 13+
COMMENT 'Entity name (e.g. Customer)'