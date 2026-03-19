-- Predictive optimization: Always optimize this table after large batches
--  OPTIMIZE ${catalog_name}.${env}_${schema_name}.dv_nbr_input ZORDER BY (relationship_id) if massive increments

CREATE TABLE IF NOT EXISTS ${catalog_name}.${env}_${schema_name}.dv_nbr_input (
  relationship_id STRING NOT NULL COMMENT 'Unique identifier for relationship (UUID or sequence)',
  relationship_name STRING NOT NULL COMMENT 'Relationship name (e.g. Customer places Order)',
  left_entity STRING NOT NULL COMMENT 'Left side entity (e.g. Customer)',
  right_entity STRING NOT NULL COMMENT 'Right side entity (e.g. Order)',
  source_dataset STRING NOT NULL COMMENT 'Driving dataset for relationship (e.g. orders)',
  date_of_entry DATE NOT NULL COMMENT 'When this relationship was entered',
  business_key_map STRING NOT NULL COMMENT 'JSON mapping of entity to BK columns',
  is_active BOOLEAN NOT NULL COMMENT 'Whether this relationship is active',
  created_at TIMESTAMP NOT NULL COMMENT 'When the relationship was created',
  PRIMARY KEY (relationship_id)
)
USING DELTA
CLUSTER BY (relationship_id) -- Liquid clustering on PK column for Databricks Runtime 13+
COMMENT 'Unique identifier for relationship (UUID or sequence)'