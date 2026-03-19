# Ingestion & metadata framework 

##  Ingest 
### Files from S3 (Source)
- Create A Volume 
- Use COPY INTO for Bronze ingestion (incremental + idempotent)
- COPY INTO is designed to load incrementally and idempotently from cloud storage into Delta tables.


### To Catalog Schema (Target)
- Create schema (Bronze)
- Implement STG DV - Artifacts, possible create HUBS, LINKS and Satellites
- Load all the files


### Ingestion Monitoring
- Create an ops table
- Log all the row count and storage size in the table


- SQL-first ingestion framework for scalable, retry-safe loading.
- Supports thousands of files (e.g., 20 daily CSVs).
- Bronze pattern per entity (e.g., `bronze_erp.customers_raw`).
- Store raw fields + ingestion metadata: `ingest_ts`, `source_file`, `run_id`, optional `rescued_data`/`corrupt_record`.
- Use `COPY INTO ... VALIDATE` for parsing/schema validation.

## Config-driven ingestion (scales to 20+ entities)
- Create `ops.ingest_config` table:
  - `entity`
  - `source_path`
  - `pattern`
  - `file_format_options`
  - `bronze_table`
  - `mode_hint`
  - `business_key_columns`
  - `expected_schema`
  - `dq_rules`
- Driver notebook/SQL job loops config rows and runs `COPY INTO`.

## Logging & validation (all stages)
- Create Delta tables in `ops`:
  - `ops.batch_run`: run metadata
  - `ops.step_run`: step-level stats
  - `ops.file_audit`: file-level audit
  - `ops.dq_results`: data quality results

## Validation approach
- "Hard" parsing/schema checks at Bronze with `COPY INTO ... VALIDATE`.
- "Business validity" checks in Staging, logged in `ops.dq_results`.

## Handling full refresh vs incremental feeds
- Treat each daily file as a candidate snapshot.
- DV satellites insert new versions only when `hashdiff` changes.
