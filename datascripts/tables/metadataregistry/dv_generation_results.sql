-- Predictive optimization: Always optimize this table after large batches
--  OPTIMIZE ${catalog_name}.${env}_${schema_name}.dv_generation_results ZORDER BY (relationship_name) if massive increments

CREATE TABLE IF NOT EXISTS ${catalog_name}.${env}_${schema_name}.dv_generation_results (
  relationship_name STRING NOT NULL COMMENT 'Relationship name',
  left_entity STRING NOT NULL COMMENT 'Left entity name',
  right_entity STRING NOT NULL COMMENT 'Right entity name',
  source_dataset STRING NOT NULL COMMENT 'Source dataset name',
  hubs STRING NOT NULL COMMENT 'Generated hub names',
  links STRING NOT NULL COMMENT 'Generated link names',
  satellites STRING NOT NULL COMMENT 'Generated satellite names',
  status STRING NOT NULL COMMENT 'Generation status',
  reason STRING NOT NULL COMMENT 'Reason if Incomplete or Error',
  timestamp_generated_at TIMESTAMP NOT NULL COMMENT 'When generation occurred',
  run_id STRING NOT NULL COMMENT 'Generator run ID',
  PRIMARY KEY (relationship_name)
)
USING DELTA
CLUSTER BY (relationship_name) -- Liquid clustering on PK column for Databricks Runtime 13+
COMMENT 'Relationship name'