-- Predictive optimization: Always optimize this table after large batches
--  OPTIMIZE ${catalog_name}.${env}_${schema_name}.dv_link_spec ZORDER BY (link_name) if massive increments

CREATE TABLE IF NOT EXISTS ${catalog_name}.${env}_${schema_name}.dv_link_spec (
  link_name STRING NOT NULL COMMENT 'Link name (e.g. L_CUSTOMER_ORDER)',
  relationship_name STRING NOT NULL COMMENT 'Relationship name from NBR',
  driving_dataset STRING NOT NULL COMMENT 'Source dataset for this link',
  hub_names STRING NOT NULL COMMENT 'Hub participants (e.g. [H_CUSTOMER',
  hk_link_column STRING NOT NULL COMMENT 'Link hash key column name',
  fk_hk_columns STRING NOT NULL COMMENT 'Foreign key column names',
  bk_mapping STRING NOT NULL COMMENT 'Mapping of hub to BK columns',
  target_schema STRING NOT NULL COMMENT 'Target schema',
  target_table STRING NOT NULL COMMENT 'Target table name',
  is_active BOOLEAN NOT NULL COMMENT 'Whether this link is active',
  generated_at TIMESTAMP NOT NULL COMMENT 'When spec was generated',
  run_id STRING NOT NULL COMMENT 'Generator run ID',
  PRIMARY KEY (link_name)
)
USING DELTA
CLUSTER BY (link_name) -- Liquid clustering on PK column for Databricks Runtime 13+
COMMENT 'Link name (e.g. L_CUSTOMER_ORDER)'