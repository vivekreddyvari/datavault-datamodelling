## Build Steps of DV-Generator

1. READ INPUT REGISTRY
2. DERIVE SPECIFICATIONS
3. EXECUTE LOADS - CREATE DDL/DML
4. RECORD RESULTS - Audit Trails for tracking generation

---

## Autoresearch Methodology Alignment

The `autoresearch` project methodology provides valuable patterns that can be directly applied to DV-Generator:

### Pattern 1: Separation of Code vs. Configuration
**Autoresearch Principle:** Separate stable core, dynamic configuration, and business logic instructions.

**DV-Generator Application:**
```
dv-generator/
├─ dv_generator_core.py (STATIC - CORE LOGIC)
│  ├─ Step1Reader - Query metadata registry
│  ├─ Step2Deriver - Transform relationships to specs
│  ├─ Step3Executor - Execute DDL/DML
│  └─ Step4Auditor - Record results
│
├─ dv_generator_config.py (DYNAMIC - CONFIGURATION)
│  ├─ catalog = 'uc_catalog'
│  ├─ metadata_schema = 'Metadata{env}_metadataregistry'
│  ├─ naming_rules = {'hub_prefix': 'H_', ...}
│  ├─ target_schemas = {'hub': 'SilverDataVault{env}_dv_hub', ...}
│  └─ hash_config = {'algo': 'SHA2_256', 'storage': 'BINARY_32'}
│
├─ GENERATOR_LOGIC.md (INSTRUCTIONS - BUSINESS RULES)
│  ├─ Relationship Interpretation Rules
│  ├─ Attribute Classification Rules
│  ├─ Naming Convention Rules
│  ├─ Error Handling Rules
│  └─ Extension Points for Custom Logic
│
└─ dv_generator_main.py (ORCHESTRATOR)
   └─ Coordinates all 4 steps
```

**Why this matters:** Keeps core generator logic stable while allowing configuration/instruction changes without code modification.

---

### Pattern 2: Single Reviewable Output
**Autoresearch Principle:** All modifications captured in one reviewable artifact.

**DV-Generator Application:**
```
dv-generator/outputs/
├─ generated_specs_{run_id}.sql (SINGLE OUTPUT FILE)
│  ├─ All generated DDL/DML statements
│  ├─ Fully deterministic (same input → same output)
│  ├─ Can be reviewed before execution
│  └─ Includes: RUN_ID, TIMESTAMP, ENVIRONMENT, STATUS
│
├─ Generator Review Process:
│  1. Step 2 outputs: dv_hub_spec, dv_link_spec, dv_satellite_spec (metadata tables)
│  2. Generate SQL file from specs
│  3. Human/AI reviews all CREATE/INSERT statements
│  4. Approve or reject as a unit (all-or-nothing)
│  5. Execute only if approved
│
├─ Output Structure:
│  -- HUBS (3 tables)
│  CREATE TABLE ... H_CUSTOMER
│  INSERT INTO ... H_CUSTOMER SELECT ...
│
│  -- LINKS (2 tables)
│  CREATE TABLE ... L_CUSTOMER_ORDER
│  INSERT INTO ... L_CUSTOMER_ORDER SELECT ...
│
│  -- SATELLITES (3 tables)
│  CREATE TABLE ... S_CUSTOMER
│  INSERT INTO ... S_CUSTOMER SELECT ...
```

**Why this matters:** All generator decisions are captured in one SQL file, making it easy to validate before execution. No hidden side effects. Full traceability via run_id.

---

### Pattern 3: Fixed Execution Scope
**Autoresearch Principle:** Training runs for fixed duration → enables fair comparison. Translate to: Generation runs once per environment → enables reproducibility.

**DV-Generator Application:**
```
├─ Fixed Generation Scope:
│  1. One environment (dev/staging/prod) = one generation run
│  2. One run_id = immutable, atomic audit trail
│  3. All relationships processed together in execution order
│
├─ Run Atomicity:
│  - All Hubs created/loaded together
│  - All Links created/loaded together
│  - All Satellites created/loaded together
│  - Single run_id tracks entire operation
│  - All-or-nothing: either entire run succeeds or fails
│
├─ Benefits:
│  ├─ Predictable, testable generation (same input → same output)
│  ├─ Easy to run in parallel (different environments)
│  ├─ Audit trail is atomic (one run = one set of changes)
│  └─ No partial states (all-or-nothing execution)
```

**Why this matters:** Each generation run is self-contained and reproducible. You can re-run the same environment 10 times and get identical results.

---

### Pattern 4: Metric-Driven Optimization
**Autoresearch Principle:** Single optimization metric (val_bpb) drives decisions. Translate to: Generation quality metrics drive validation.

**DV-Generator Application:**
```
dv-generator/metrics/
├─ Generation Quality Metrics:
│  1. Coverage: % of business relationships → Links
│  2. Completeness: % of entities → Hub + Satellite
│  3. Execution Fidelity: % of DDL/DML succeeds
│  4. Schema Compliance: % match DV2 standards
│  5. Data Quality: Row count ratios (Hubs ≤ Links ≤ Satellites)
│
├─ Metric Logging:
│  - dv_run_audit.rows_inserted (per object)
│  - dv_object_audit.duration_seconds (per table)
│  - dv_object_audit.status (Completed | Error)
│  - dv_generation_results.reason (success details)
│
├─ Validation Loop:
│  1. Generate specs → dv_hub_spec, dv_link_spec, dv_satellite_spec
│  2. Execute → dv_object_audit (per-table metrics)
│  3. Measure → dv_run_audit (aggregated metrics)
│  4. If metric < threshold → alert via dv_generation_results.reason
│  5. Iterate on naming rules or entity definitions
```

**Why this matters:** You have objective criteria for "good generation" beyond just "tables were created."

---

### Pattern 5: Markdown-Driven Intelligence
**Autoresearch Principle:** `program.md` is the "research org code" that guides behavior. Translate to: Business rules documented in markdown, separate from code.

**DV-Generator Application:**
```
dv-generator/
├─ GENERATOR_LOGIC.md (BUSINESS RULES DOCUMENTATION)
│  ├─ Section 1: Relationship Interpretation Rules
│  │  ├─ One-to-Many: "Customer places Order"
│  │  │   └─ Create: H_CUSTOMER, H_ORDER, L_CUSTOMER_ORDER
│  │  │
│  │  └─ Many-to-Many: "Product supplied by Supplier"
│  │      └─ Create: H_PRODUCT, H_SUPPLIER, L_PRODUCT_SUPPLIER
│  │
│  ├─ Section 2: Attribute Classification Rules
│  │  ├─ Allowlist Mode (DEFAULT): Only explicit attributes → Satellite
│  │  ├─ Auto-Add Mode: All attributes → Satellite except denylist
│  │  └─ Locked Mode: Predefined attributes only
│  │
│  ├─ Section 3: Naming Convention Rules
│  │  ├─ Hub naming: H_{ENTITY_NAME}
│  │  ├─ Link naming: L_{LEFT_ENTITY}_{RIGHT_ENTITY}
│  │  └─ Satellite naming: S_{ENTITY_NAME}
│  │
│  ├─ Section 4: Hash Key Generation
│  │  ├─ Algorithm: SHA2_256
│  │  ├─ Format: BINARY_32 or HEX_64
│  │  └─ Concatenation: CONCAT(bk_col1, bk_col2, ...)
│  │
│  ├─ Section 5: Error Handling Rules
│  │  ├─ Missing left_entity: Log error, skip relationship
│  │  ├─ Missing bk_columns: Log error, fail run
│  │  └─ Schema mismatch: Log warning, attempt recovery
│  │
│  └─ Section 6: Extension Points
│     └─ "To add custom table types: modify Step2Deriver class"
│
├─ Benefits:
│  ├─ Non-technical stakeholders can read & suggest changes
│  ├─ Code implementation validates against documented rules
│  ├─ Changes to rules don't require code redeployment
│  ├─ Audit trail (dv_generation_results) shows which rule was applied
│  └─ Clear separation: RULES (markdown) vs. IMPLEMENTATION (code)
```

**Why this matters:** Separates business rules (markdown) from implementation (code). You can iterate on rules without code changes. Rules become part of the data lineage.

---

## Implementation Strategy (Autoresearch-Inspired)

### Layer 1: Static Core (Never Changes)
```python
# dv_generator_core.py
class Step1Reader:
    """Read metadata registry and validate sources"""
    def read_registry(self, spark, catalog, env): pass

class Step2Deriver:
    """Transform relationships into Hub/Link/Satellite specifications"""
    def derive_specs(self, spark, catalog, env, inputs): pass

class Step3Executor:
    """Execute DDL/DML to create and load tables"""
    def execute_loads(self, spark, catalog, env, specs): pass

class Step4Auditor:
    """Record results and audit trails"""
    def record_results(self, spark, catalog, env, run_id, status): pass

class DVGenerator:
    """Main orchestrator"""
    def run(self, spark, catalog, env):
        inputs = Step1Reader().read_registry(spark, catalog, env)
        specs = Step2Deriver().derive_specs(spark, catalog, env, inputs)
        result = Step3Executor().execute_loads(spark, catalog, env, specs)
        Step4Auditor().record_results(spark, catalog, env, result['run_id'], result['status'])
```

### Layer 2: Configuration (Changes as Needed)
```python
# dv_generator_config.py
CONFIG = {
    'catalog': 'uc_catalog',
    'metadata_schema': 'Metadata{env}_metadataregistry',

    'naming_rules': {
        'hub_prefix': 'H_',
        'link_prefix': 'L_',
        'sat_prefix': 'S_',
        'activity_prefix': 'A_'
    },

    'hash_config': {
        'algo': 'SHA2_256',
        'storage': 'BINARY_32'  # or HEX_64
    },

    'target_schemas': {
        'hub': 'SilverDataVault{env}_dv_hub',
        'link': 'SilverDataVault{env}_dv_link',
        'sat': 'SilverDataVault{env}_dv_satellite',
        'activity': 'SilverDataVault{env}_dv_activity'
    },

    'metadata_tables': {
        'dataset_registry': 'dv_dataset_registry',
        'nbr_input': 'dv_nbr_input',
        'entity_owner': 'dv_entity_owner',
        'naming_rules': 'dv_naming_rules',
        'hub_spec': 'dv_hub_spec',
        'link_spec': 'dv_link_spec',
        'sat_spec': 'dv_satellite_spec',
        'generation_results': 'dv_generation_results',
        'run_audit': 'dv_run_audit',
        'object_audit': 'dv_object_audit'
    }
}
```

### Layer 3: Instructions (Business Logic in Markdown)
```markdown
# GENERATOR_LOGIC.md

## Relationship Interpretation Rules

### Rule 1: One-to-Many Relationships
When relationship_name = 'Customer places Order':
- Source: orders table (fact table)
- Left entity: Customer (dimension)
- Right entity: Order (fact)
- Result:
  - Create: H_CUSTOMER (from customers)
  - Create: H_ORDER (from orders)
  - Create: L_CUSTOMER_ORDER (from orders join)

### Rule 2: Many-to-Many Relationships
When relationship_name = 'Product supplied by Supplier':
- Source: product_suppliers table (junction table)
- Left entity: Product
- Right entity: Supplier
- Result:
  - Create: H_PRODUCT, H_SUPPLIER
  - Create: L_PRODUCT_SUPPLIER (symmetric)
  - Create: S_PRODUCT, S_SUPPLIER (separate)

## Attribute Classification Rules

### Allowlist Mode (DEFAULT)
- Configuration: dv_entity_owner.satellite_mode = 'ALLOWLIST'
- Behavior: Only columns in attr_allowlist → Satellite
- Rationale: Security by default, explicit inclusion
- Use case: Sensitive data domains (Finance, HR)

### Auto-Add Mode
- Configuration: dv_entity_owner.satellite_mode = 'AUTO_ADD'
- Behavior: All columns → Satellite except attr_denylist
- Rationale: Exploratory analysis, discover unknowns
- Use case: New data sources, discovery phase

### Locked Mode
- Configuration: dv_entity_owner.satellite_mode = 'LOCKED'
- Behavior: Only predefined columns, no auto-discovery
- Rationale: Strict governance, compliance
- Use case: Regulated domains (PII, GDPR)

## Error Handling Rules

### Critical Errors (FAIL RUN)
- Missing dv_entity_owner entry for relationship entity
- Missing bk_columns in dv_entity_owner
- Business key columns not found in source dataset
- Action: Log error, mark run_audit.status = 'Error', roll back

### Warnings (SKIP RELATIONSHIP)
- Missing relationship.source_dataset
- Schema mismatch between entity_owner and actual dataset
- Action: Log warning, mark relationship in generation_results.reason, continue

### Info Logs (PROCEED)
- Duplicate check passed in hub insert
- Satellite HASHDIFF unchanged (no insert)
- Action: Log metrics in dv_object_audit, continue
```

### Layer 4: Generated Output (Single Reviewable File)
```sql
-- dv-generator/outputs/generated_specs_{run_id}.sql
-- Automatically generated from Step 2 & Step 3

-- ===== GENERATION METADATA =====
-- RUN ID: run_20240320_101500
-- TIMESTAMP: 2024-03-20 10:15:00
-- ENVIRONMENT: dev
-- STATUS: GENERATED (awaiting approval)
-- RELATIONSHIPS: 3
-- HUBS: 4
-- LINKS: 3
-- SATELLITES: 4

-- ===== HUBS (4 tables) =====
-- Hub: H_CUSTOMER
CREATE TABLE IF NOT EXISTS SilverDataVaultdev_dv_hub.H_CUSTOMER (
    HK_CUSTOMER BINARY(32),
    CUSTOMER_ID STRING,
    LOAD_DTS TIMESTAMP,
    RSRC STRING,
    PRIMARY KEY (HK_CUSTOMER)
) USING ICEBERG;

INSERT INTO SilverDataVaultdev_dv_hub.H_CUSTOMER
SELECT DISTINCT
    UNHEX(SHA2(CONCAT(CUSTOMER_ID), 256)) as HK_CUSTOMER,
    CUSTOMER_ID,
    CURRENT_TIMESTAMP() as LOAD_DTS,
    'LANDING' as RSRC
FROM BronzeLandingdev_landing.customers
WHERE HK_CUSTOMER NOT IN (SELECT HK_CUSTOMER FROM SilverDataVaultdev_dv_hub.H_CUSTOMER);

-- ===== LINKS (3 tables) =====
-- Link: L_CUSTOMER_ORDER
CREATE TABLE IF NOT EXISTS SilverDataVaultdev_dv_link.L_CUSTOMER_ORDER (
    HK_CUSTOMER_ORDER BINARY(32),
    HK_CUSTOMER BINARY(32),
    HK_ORDER BINARY(32),
    LOAD_DTS TIMESTAMP,
    RSRC STRING,
    PRIMARY KEY (HK_CUSTOMER_ORDER)
) USING ICEBERG;

INSERT INTO SilverDataVaultdev_dv_link.L_CUSTOMER_ORDER
SELECT DISTINCT
    UNHEX(SHA2(CONCAT(HK_CUSTOMER, HK_ORDER), 256)) as HK_CUSTOMER_ORDER,
    HK_CUSTOMER,
    HK_ORDER,
    CURRENT_TIMESTAMP() as LOAD_DTS,
    'LANDING' as RSRC
FROM BronzeLandingdev_landing.orders
WHERE HK_CUSTOMER_ORDER NOT IN (SELECT HK_CUSTOMER_ORDER FROM SilverDataVaultdev_dv_link.L_CUSTOMER_ORDER);

-- ===== SATELLITES (4 tables) =====
-- Satellite: S_CUSTOMER (attributes: COMPANY_NAME, CONTACT_NAME, CITY, COUNTRY)
CREATE TABLE IF NOT EXISTS SilverDataVaultdev_dv_satellite.S_CUSTOMER (
    HK_CUSTOMER BINARY(32),
    HASHDIFF BINARY(32),
    LOAD_DTS TIMESTAMP,
    COMPANY_NAME STRING,
    CONTACT_NAME STRING,
    CITY STRING,
    COUNTRY STRING,
    RSRC STRING,
    PRIMARY KEY (HK_CUSTOMER, LOAD_DTS)
) USING ICEBERG;

INSERT INTO SilverDataVaultdev_dv_satellite.S_CUSTOMER
WITH new_data AS (
    SELECT
        UNHEX(SHA2(CONCAT(CUSTOMER_ID), 256)) as HK_CUSTOMER,
        UNHEX(SHA2(CONCAT(COMPANY_NAME, CONTACT_NAME, CITY, COUNTRY), 256)) as HASHDIFF,
        COMPANY_NAME,
        CONTACT_NAME,
        CITY,
        COUNTRY,
        CURRENT_TIMESTAMP() as LOAD_DTS,
        'LANDING' as RSRC
    FROM BronzeLandingdev_landing.customers
)
SELECT * FROM new_data
WHERE HASHDIFF NOT IN (
    SELECT HASHDIFF FROM SilverDataVaultdev_dv_satellite.S_CUSTOMER
    WHERE HK_CUSTOMER = new_data.HK_CUSTOMER
);
```

---

## Benefits of This Approach

| Benefit | How Autoresearch Inspired It |
|---------|----------------------------|
| **Stable Core** | Like `prepare.py`, generator_core.py never changes |
| **Reviewable Output** | Like `train.py`, all decisions in one SQL file |
| **Configuration-Driven** | Like `program.md`, business logic in markdown |
| **Reproducible** | Same input → same generated_specs.sql every time |
| **Testable** | Each step has clear input/output contracts |
| **Extensible** | Add new rules to GENERATOR_LOGIC.md without touching code |
| **Auditable** | Full run_id traceability + markdown rules → can explain any decision |

---

## Design Choices

### 1. Metadata-Driven Architecture
**Choice:** All specifications are stored as data in metadata tables, not hardcoded
- **Why:** Enables flexibility, auditability, and re-usability without code changes
- **Impact:** Generator becomes configurable, not programmatic
- **Implementation:** All logic queries metadata tables; no configuration in code

### 2. Ensemble Logical Modelling (ELM) Foundation
**Choice:** All generation follows ELM principles (Core Business Concepts → Natural Business Keys → Artifacts)
- **Why:** Standardizes approach across all data vaults
- **Impact:** Consistent, predictable output regardless of source domain
- **Implementation:** Every relationship in dv_nbr_input must map to at least one Hub and one Link

### 3. Separation of Concerns (4 Steps)
**Choice:** Each step is independent with clear inputs/outputs
```
Step 1: Input → Step 2: Specification → Step 3: Execution → Step 4: Audit
```
- **Why:** Allows testing, debugging, and re-running individual steps
- **Impact:** Modular, maintainable code structure
- **Implementation:** Each step is a separate module/class with clear contract

### 4. Data-Driven Specification Generation
**Choice:** Specifications (Hub/Link/Sat specs) are **generated tables**, not manually created
- **Why:** Eliminates manual specification creation, reduces errors
- **Impact:** Single source of truth; specs derive from business relationships, not vice versa
- **Implementation:** Step 2 outputs are INSERT into dv_hub_spec, dv_link_spec, dv_satellite_spec

### 5. Insert-Only / Append-Only Pattern
**Choice:** Hubs/Links are insert-only; Satellites are append-on-change
- **Why:** Immutable core for audit trail and time-variance
- **Impact:** Simplifies CDC handling, enforces data quality
- **Implementation:** Hubs/Links use INSERT IGNORE or check for duplicates; Satellites use HASHDIFF comparison

### 6. Hash Key Strategy
**Choice:** Business keys are hashed (SHA256) into hash keys (HK_*)
- **Why:** Handles composite keys, enables efficient joins, obscures sensitive data
- **Impact:** Standardized key format across all tables
- **Implementation:** HK = SHA256(CONCAT(bk_col1, bk_col2, ...)) stored as BINARY_32 or HEX_64

### 7. Audit-Trail Centric
**Choice:** All generation recorded in audit tables (dv_generation_results, dv_run_audit, dv_object_audit)
- **Why:** Full traceability of what was generated, when, and why
- **Impact:** Compliance-ready, debugging capability
- **Implementation:** Generate run_id at start; log all steps with run_id, timestamps, status

### 8. Relationship-Based Structure
**Choice:** Generator starts from **relationships** (dv_nbr_input), not individual entities
- **Why:** Captures business logic early; Links are derived from relationships
- **Impact:** Ensures data relationships are intentional, documented
- **Implementation:** Each row in dv_nbr_input generates 1 Link + 2+ Hubs + N Satellites

### 9. Attribute Control via Allowlist/Denylist
**Choice:** Satellite attributes controlled by allowlist/denylist in dv_entity_owner
- **Why:** Prevents accidental exposure of sensitive columns
- **Impact:** Security-by-design, governance-friendly
- **Implementation:** Filter columns from driving_dataset using allowlist/denylist before creating Satellite

### 10. Dependency Resolution
**Choice:** Hubs created first → Links → Satellites (respects FK dependencies)
- **Why:** Ensures referential integrity during execution
- **Impact:** No orphaned tables, correct load order
- **Implementation:** Execute in order: Step 3a (Hubs) → Step 3b (Links) → Step 3c (Satellites)

---

## Implementation Pseudocode

### Step 1: READ INPUT REGISTRY

```python
def step1_read_input_registry(spark, catalog, env):
    """
    Read metadata registry to discover all available data sources
    """
    run_id = generate_uuid()  # Start audit trail

    try:
        # Query dataset registry
        datasets = spark.sql(f"""
            SELECT * FROM {catalog}.Metadata{env}_metadataregistry.dv_dataset_registry
            WHERE is_active = TRUE
        """).collect()

        # Validate datasets exist
        for dataset in datasets:
            location = dataset['location']
            if not path_exists(location):
                log_error(run_id, f"Dataset {dataset['dataset_name']} not found at {location}")
                raise Exception(f"Source not found: {location}")

        # Query relationships registry
        relationships = spark.sql(f"""
            SELECT * FROM {catalog}.Metadata{env}_metadataregistry.dv_nbr_input
            WHERE is_active = TRUE
        """).collect()

        # Query entity ownership
        entity_owners = spark.sql(f"""
            SELECT * FROM {catalog}.Metadata{env}_metadataregistry.dv_entity_owner
            WHERE is_active = TRUE
        """).collect()

        # Query naming rules
        naming_rules = spark.sql(f"""
            SELECT * FROM {catalog}.Metadata{env}_metadataregistry.dv_naming_rules
        """).collect()

        return {
            'run_id': run_id,
            'datasets': datasets,
            'relationships': relationships,
            'entity_owners': entity_owners,
            'naming_rules': naming_rules
        }

    except Exception as e:
        log_error(run_id, f"Step 1 failed: {str(e)}")
        raise
```

---

### Step 2: DERIVE SPECIFICATIONS

```python
def step2_derive_specifications(spark, catalog, env, inputs):
    """
    Transform business relationships into executable specifications
    """
    run_id = inputs['run_id']
    relationships = inputs['relationships']
    entity_owners = inputs['entity_owners']
    naming_rules = inputs['naming_rules'][0]  # Single row

    hub_specs = []
    link_specs = []
    sat_specs = []

    try:
        # LOOP 1: Generate Hub Specifications
        for entity_owner in entity_owners:
            hub_spec = {
                'hub_name': f"{naming_rules['hub_prefix']}{entity_owner['entity_name'].upper()}",
                'entity_name': entity_owner['entity_name'],
                'driving_dataset': entity_owner['owner_dataset'],
                'bk_columns': entity_owner['bk_columns'],  # JSON list
                'hk_column': f"HK_{entity_owner['entity_name'].upper()}",
                'hash_algo': naming_rules['hash_algo'],
                'hash_storage': naming_rules['hash_storage'],
                'target_schema': f"SilverDataVault{env}_dv_hub",
                'is_active': True,
                'generated_at': current_timestamp(),
                'run_id': run_id
            }
            hub_specs.append(hub_spec)

        # LOOP 2: Generate Link Specifications
        for relationship in relationships:
            left_entity = relationship['left_entity']
            right_entity = relationship['right_entity']

            # Get hub names from entity_owners
            left_hub = f"{naming_rules['hub_prefix']}{left_entity.upper()}"
            right_hub = f"{naming_rules['hub_prefix']}{right_entity.upper()}"

            link_name = f"{naming_rules['link_prefix']}{left_entity.upper()}_{right_entity.upper()}"

            # Get business key mappings
            left_bk_cols = get_entity_owner(entity_owners, left_entity)['bk_columns']
            right_bk_cols = get_entity_owner(entity_owners, right_entity)['bk_columns']

            link_spec = {
                'link_name': link_name,
                'relationship_name': relationship['relationship_name'],
                'driving_dataset': relationship['source_dataset'],
                'hub_names': [left_hub, right_hub],
                'hk_link_column': f"HK_{left_entity.upper()}_{right_entity.upper()}",
                'fk_hk_columns': [f"HK_{left_entity.upper()}", f"HK_{right_entity.upper()}"],
                'bk_mapping': {
                    left_entity: left_bk_cols,
                    right_entity: right_bk_cols
                },
                'target_schema': f"SilverDataVault{env}_dv_link",
                'target_table': link_name,
                'is_active': True,
                'generated_at': current_timestamp(),
                'run_id': run_id
            }
            link_specs.append(link_spec)

        # LOOP 3: Generate Satellite Specifications
        for entity_owner in entity_owners:
            # Get all attributes from driving dataset
            driving_dataset = entity_owner['owner_dataset']
            all_columns = get_dataset_schema(spark, driving_dataset)

            # Filter by allowlist/denylist
            allowlist = entity_owner['attr_allowlist']  # JSON list
            denylist = entity_owner['attr_denylist']     # JSON list

            attribute_columns = [col for col in all_columns
                                if (col in allowlist if allowlist else True)
                                and col not in denylist]

            sat_spec = {
                'sat_name': f"{naming_rules['sat_prefix']}{entity_owner['entity_name'].upper()}",
                'parent_type': 'HUB',
                'parent_name': f"{naming_rules['hub_prefix']}{entity_owner['entity_name'].upper()}",
                'driving_dataset': driving_dataset,
                'parent_hk_column': f"HK_{entity_owner['entity_name'].upper()}",
                'attribute_columns': attribute_columns,
                'target_schema': f"SilverDataVault{env}_dv_satellite",
                'target_table': f"{naming_rules['sat_prefix']}{entity_owner['entity_name'].upper()}",
                'mode': entity_owner['satellite_mode'],  # ALLOWLIST | AUTO_ADD | LOCKED
                'is_active': True,
                'generated_at': current_timestamp(),
                'run_id': run_id
            }
            sat_specs.append(sat_spec)

        # WRITE SPECIFICATIONS TO METADATA TABLES
        spark.createDataFrame(hub_specs).write.insertInto(
            f"{catalog}.Metadata{env}_metadataregistry.dv_hub_spec"
        )
        spark.createDataFrame(link_specs).write.insertInto(
            f"{catalog}.Metadata{env}_metadataregistry.dv_link_spec"
        )
        spark.createDataFrame(sat_specs).write.insertInto(
            f"{catalog}.Metadata{env}_metadataregistry.dv_satellite_spec"
        )

        return {
            'run_id': run_id,
            'hub_specs': hub_specs,
            'link_specs': link_specs,
            'sat_specs': sat_specs
        }

    except Exception as e:
        log_error(run_id, f"Step 2 failed: {str(e)}")
        raise
```

---

### Step 3: EXECUTE LOADS (Dependency-Ordered)

```python
def step3_execute_loads(spark, catalog, env, specs):
    """
    Create and populate Hub, Link, and Satellite tables in dependency order
    """
    run_id = specs['run_id']

    try:
        # STEP 3A: CREATE AND LOAD HUBS (no dependencies)
        for hub_spec in specs['hub_specs']:
            execute_hub_load(spark, catalog, env, hub_spec, run_id)

        # STEP 3B: CREATE AND LOAD LINKS (depends on hubs existing)
        for link_spec in specs['link_specs']:
            execute_link_load(spark, catalog, env, link_spec, run_id)

        # STEP 3C: CREATE AND LOAD SATELLITES (depends on hubs/links existing)
        for sat_spec in specs['sat_specs']:
            execute_satellite_load(spark, catalog, env, sat_spec, run_id)

        return {'run_id': run_id, 'status': 'Completed'}

    except Exception as e:
        log_error(run_id, f"Step 3 failed: {str(e)}")
        raise


def execute_hub_load(spark, catalog, env, hub_spec, run_id):
    """
    Execute Hub creation and load (INSERT-ONLY)
    """
    hub_name = hub_spec['hub_name']
    driving_dataset = hub_spec['driving_dataset']
    bk_columns = hub_spec['bk_columns']
    hk_column = hub_spec['hk_column']

    # CREATE HUB TABLE (if not exists)
    create_hub_ddl = f"""
    CREATE TABLE IF NOT EXISTS {hub_spec['target_schema']}.{hub_name} (
        {hk_column} BINARY(32),
        {', '.join(bk_columns)} STRING,
        LOAD_DTS TIMESTAMP,
        RSRC STRING,
        PRIMARY KEY ({hk_column})
    )
    USING ICEBERG
    """
    spark.sql(create_hub_ddl)

    # LOAD DATA (INSERT-ONLY: skip duplicates)
    hash_expr = f"SHA2(CONCAT({', '.join(bk_columns)}), 256)"

    load_hub_dml = f"""
    INSERT INTO {hub_spec['target_schema']}.{hub_name}
    SELECT DISTINCT
        UNHEX({hash_expr}) as {hk_column},
        {', '.join(bk_columns)},
        CURRENT_TIMESTAMP() as LOAD_DTS,
        'LANDING' as RSRC
    FROM {driving_dataset}
    WHERE {hk_column} NOT IN (
        SELECT {hk_column} FROM {hub_spec['target_schema']}.{hub_name}
    )
    """

    rows_inserted = spark.sql(load_hub_dml).count()
    log_object_audit(run_id, 'HUB', hub_name, rows_inserted)


def execute_link_load(spark, catalog, env, link_spec, run_id):
    """
    Execute Link creation and load (INSERT-ONLY with FK validation)
    """
    link_name = link_spec['link_name']
    driving_dataset = link_spec['driving_dataset']
    hub_names = link_spec['hub_names']
    fk_hk_columns = link_spec['fk_hk_columns']
    hk_link_column = link_spec['hk_link_column']
    bk_mapping = link_spec['bk_mapping']

    # BUILD HASH JOIN EXPRESSIONS FOR EACH HUB
    join_exprs = []
    for hub_name, fk_hk_col in zip(hub_names, fk_hk_columns):
        entity_name = hub_name.replace('H_', '')
        bk_cols = bk_mapping[entity_name]
        hash_expr = f"SHA2(CONCAT({bk_cols}), 256)"
        join_exprs.append(f"{fk_hk_col} = {hash_expr}")

    # CREATE LINK TABLE
    create_link_ddl = f"""
    CREATE TABLE IF NOT EXISTS {link_spec['target_schema']}.{link_name} (
        {hk_link_column} BINARY(32),
        {', '.join(fk_hk_columns)} BINARY(32),
        LOAD_DTS TIMESTAMP,
        RSRC STRING,
        PRIMARY KEY ({hk_link_column})
    )
    USING ICEBERG
    """
    spark.sql(create_link_ddl)

    # LOAD DATA (JOIN to hubs for FK validation)
    load_link_dml = f"""
    INSERT INTO {link_spec['target_schema']}.{link_name}
    SELECT DISTINCT
        SHA2(CONCAT({', '.join(fk_hk_columns)}), 256) as {hk_link_column},
        {', '.join(fk_hk_columns)},
        CURRENT_TIMESTAMP() as LOAD_DTS,
        'LANDING' as RSRC
    FROM {driving_dataset}
    WHERE {' AND '.join(join_exprs)}  -- FK validation
    """

    rows_inserted = spark.sql(load_link_dml).count()
    log_object_audit(run_id, 'LINK', link_name, rows_inserted)


def execute_satellite_load(spark, catalog, env, sat_spec, run_id):
    """
    Execute Satellite creation and load (APPEND-ON-CHANGE using HASHDIFF)
    """
    sat_name = sat_spec['sat_name']
    parent_hk_column = sat_spec['parent_hk_column']
    attribute_columns = sat_spec['attribute_columns']

    # COMPUTE HASHDIFF on attributes
    hashdiff_expr = f"SHA2(CONCAT({', '.join(attribute_columns)}), 256)"

    # CREATE SATELLITE TABLE
    create_sat_ddl = f"""
    CREATE TABLE IF NOT EXISTS {sat_spec['target_schema']}.{sat_name} (
        {parent_hk_column} BINARY(32),
        HASHDIFF BINARY(32),
        LOAD_DTS TIMESTAMP,
        {', '.join(attribute_columns)} STRING,
        RSRC STRING,
        PRIMARY KEY ({parent_hk_column}, LOAD_DTS)
    )
    USING ICEBERG
    """
    spark.sql(create_sat_ddl)

    # LOAD DATA (APPEND-ON-CHANGE: only if HASHDIFF changed)
    load_sat_dml = f"""
    WITH new_data AS (
        SELECT
            SHA2(CONCAT({parent_hk_column}_bk), 256) as {parent_hk_column},
            {hashdiff_expr} as HASHDIFF,
            {', '.join(attribute_columns)},
            CURRENT_TIMESTAMP() as LOAD_DTS,
            'LANDING' as RSRC
        FROM {sat_spec['driving_dataset']}
    )
    INSERT INTO {sat_spec['target_schema']}.{sat_name}
    SELECT * FROM new_data
    WHERE HASHDIFF NOT IN (
        SELECT HASHDIFF FROM {sat_spec['target_schema']}.{sat_name}
        WHERE {parent_hk_column} = new_data.{parent_hk_column}
    )
    """

    rows_inserted = spark.sql(load_sat_dml).count()
    log_object_audit(run_id, 'SAT', sat_name, rows_inserted)
```

---

### Step 4: RECORD RESULTS

```python
def step4_record_results(spark, catalog, env, run_id, status, error_msg=None):
    """
    Log all generation metadata for audit trail and debugging
    """

    # RECORD RUN AUDIT
    run_audit = spark.createDataFrame([{
        'run_id': run_id,
        'started_at': run_start_time,
        'finished_at': current_timestamp(),
        'status': status,
        'reason': error_msg,
        'config_snapshot': json.dumps({'env': env}),
        'executed_by': get_current_user(),
        'job_id': get_job_id()
    }])

    run_audit.write.insertInto(
        f"{catalog}.Metadata{env}_metadataregistry.dv_run_audit"
    )

    # RECORD GENERATION RESULTS (from dv_object_audit)
    object_audits = spark.sql(f"""
        SELECT * FROM {catalog}.Metadata{env}_metadataregistry.dv_object_audit
        WHERE run_id = '{run_id}'
    """)

    generation_results = spark.sql(f"""
        SELECT
            relationship_name,
            STRING_AGG(DISTINCT hub_names, ',') as hubs,
            STRING_AGG(DISTINCT link_names, ',') as links,
            STRING_AGG(DISTINCT sat_names, ',') as satellites,
            '{status}' as status,
            '{error_msg}' as reason,
            CURRENT_TIMESTAMP() as timestamp_generated_at,
            '{run_id}' as run_id
        FROM {catalog}.Metadata{env}_metadataregistry.dv_generation_results
        WHERE run_id = '{run_id}'
        GROUP BY relationship_name
    """)

    generation_results.write.insertInto(
        f"{catalog}.Metadata{env}_metadataregistry.dv_generation_results"
    )

    print(f"✓ Generation Complete: run_id={run_id}, status={status}")
```

---

## Complete Orchestration Flow

```python
def run_dv_generator(spark, catalog, env):
    """
    Main entry point: orchestrate all 4 steps
    """
    try:
        # STEP 1: READ INPUT REGISTRY
        inputs = step1_read_input_registry(spark, catalog, env)
        print(f"✓ Step 1 Complete: {len(inputs['datasets'])} datasets, {len(inputs['relationships'])} relationships")

        # STEP 2: DERIVE SPECIFICATIONS
        specs = step2_derive_specifications(spark, catalog, env, inputs)
        print(f"✓ Step 2 Complete: {len(specs['hub_specs'])} hubs, {len(specs['link_specs'])} links, {len(specs['sat_specs'])} satellites")

        # STEP 3: EXECUTE LOADS
        result = step3_execute_loads(spark, catalog, env, specs)
        print(f"✓ Step 3 Complete: All tables created and loaded")

        # STEP 4: RECORD RESULTS
        step4_record_results(spark, catalog, env, result['run_id'], 'Completed')
        print(f"✓ Step 4 Complete: Audit trails recorded")

    except Exception as e:
        step4_record_results(spark, catalog, env, inputs['run_id'], 'Error', str(e))
        raise
```

---

This pseudocode translates all design choices into concrete implementation steps. Each function is modular and can be tested independently. 