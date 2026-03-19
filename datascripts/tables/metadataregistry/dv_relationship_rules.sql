-- Predictive optimization: Always optimize this table after large batches
--  OPTIMIZE ${catalog_name}.${env}_${schema_name}.dv_relationship_rules ZORDER BY (rule_set) if massive increments

CREATE TABLE IF NOT EXISTS ${catalog_name}.${env}_${schema_name}.dv_relationship_rules (
  rule_set STRING NOT NULL COMMENT 'Rule set name',
  allow_pairs STRING NOT NULL COMMENT 'Allowed hub pairs (e.g. Customer-Order;Order-Product)',
  deny_pairs STRING NOT NULL COMMENT 'Denied hub pairs',
  max_hubs_per_link INT NOT NULL COMMENT 'Maximum hubs per link',
  require_nbr_for_links BOOLEAN NOT NULL COMMENT 'Whether NBR is required for links',
  created_at TIMESTAMP NOT NULL COMMENT 'When rules were created',
  PRIMARY KEY (rule_set)
)
USING DELTA
CLUSTER BY (rule_set) -- Liquid clustering on PK column for Databricks Runtime 13+
COMMENT 'Rule set name'