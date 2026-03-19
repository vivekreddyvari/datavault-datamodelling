-- Predictive optimization: Always optimize this table after large batches
--  OPTIMIZE ${catalog_name}.${env}_${schema_name}.dv_naming_rules ZORDER BY (rule_name) if massive increments

CREATE TABLE IF NOT EXISTS ${catalog_name}.${env}_${schema_name}.dv_naming_rules (
  rule_name STRING NOT NULL COMMENT 'Rule set name (e.g. default_rules)',
  hub_prefix STRING NOT NULL COMMENT 'Hub table prefix',
  link_prefix STRING NOT NULL COMMENT 'Link table prefix',
  sat_prefix STRING NOT NULL COMMENT 'Satellite table prefix',
  hash_algo STRING NOT NULL COMMENT 'Hash algorithm to use',
  hash_storage STRING NOT NULL COMMENT 'How to store hash keys',
  created_at TIMESTAMP NOT NULL COMMENT 'When rules were created',
  PRIMARY KEY (rule_name)
)
USING DELTA
CLUSTER BY (rule_name) -- Liquid clustering on PK column for Databricks Runtime 13+
COMMENT 'Rule set name (e.g. default_rules)'