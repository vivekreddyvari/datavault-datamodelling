## DV-Generator

### What it does?

DV-Generator automates the creation of Data Vault artifacts following Ensemble Logical Modelling (ELM) principles and enterprise data warehousing (EDW) fundamentals. It focuses on two core concepts:

1. **Core Business Concepts** - identifying the key entities in your business domain
2. **Natural Business Keys** - mapping the unique identifiers for each business concept

Based on these inputs, DV-Generator automatically creates the following Data Vault components:

- **Hubs** - contain only the business key with a sequence ID or hash key
- **Satellites** - hold descriptive information (context) and historical data for a Hub. Multiple satellites can describe a single Hub, but each satellite describes only one Hub
- **Links** - establish and represent the unique, specific business relationships between business keys

This utility generates the tables needed to optimally structure your data vault layer. 

### How it works?

DV-Generator follows the Ensemble Logical Modelling (ELM) approach with a metadata-driven architecture. The workflow consists of:

1. **Define Core Business Concepts** - identify the key business entities and their natural business keys
2. **Generate Artifacts** - the utility automatically creates Hubs, Satellites, and Links based on your business concepts
3. **Stage Data** - structure raw source data into the generated tables
4. **Apply Transformations** - implement business logic to transform and load data into the Data Vault
5. **Create Facts and Dimensions** - generate analytical layer objects for reporting and analysis

---

### Metadata-Driven DV-Generator Workflow

The generator operates on a 4-step metadata-driven process:

#### Step 1: READ INPUT REGISTRY
```
├─ Query: dv_dataset_registry
├─ Result: List of sources (customers, orders, products)
└─ Purpose: Know what data exists
```
**Action:** Query the metadata registry to identify all available data sources and their locations.

---

#### Step 2: DERIVE SPECIFICATIONS
```
├─ Input: dv_nbr_input + dv_entity_owner + dv_naming_rules
├─ Logic:
│  └─ For each relationship in dv_nbr_input:
│     ├─ Get hub names from dv_entity_owner
│     ├─ Get naming rules from dv_naming_rules
│     └─ Generate hub_spec, link_spec, sat_spec
├─ Output: Populate dv_hub_spec, dv_link_spec, dv_satellite_spec
└─ Benefit: All specs codified as data
```
**Action:** Transform business relationships and entity ownership into executable specifications. This is the core intelligence step.

**Key Inputs:**
- `dv_nbr_input` - Natural Business Relationships (which entities relate to each other)
- `dv_entity_owner` - Entity-to-dataset mappings and attribute definitions
- `dv_naming_rules` - Naming conventions (prefixes, hash algorithms)

**Key Outputs:**
- `dv_hub_spec` - Complete Hub specifications (business keys, hash keys, target tables)
- `dv_link_spec` - Link specifications (relationships between hubs)
- `dv_satellite_spec` - Satellite specifications (attributes, parent hubs, SCD2 tracking)

---

#### Step 3: EXECUTE LOADS
```
├─ Input: dv_hub_spec, dv_link_spec, dv_satellite_spec
├─ Logic: For each spec, execute corresponding load function
├─ Output: Create/populate H_*, L_*, S_* tables
└─ Benefit: Automated table creation
```
**Action:** Generate and execute the DDL/DML to create and populate the Data Vault tables.

**Process:**
1. Create Hub tables from `dv_hub_spec`
2. Create Link tables from `dv_link_spec` (depends on hubs being created)
3. Create Satellite tables from `dv_satellite_spec` (depends on hubs/links being created)
4. Load data into each table following Data Vault patterns (insert-only for hubs/links, append-on-change for satellites)

---

#### Step 4: RECORD RESULTS
```
├─ Output: dv_generation_results, dv_run_audit, dv_object_audit
├─ Purpose: Audit trail + metrics
└─ Benefit: Know what was generated, any errors
```
**Action:** Log all generation metadata for traceability and debugging.

**Audit Tables:**
- `dv_generation_results` - What was generated and why
- `dv_run_audit` - When and who executed the generation
- `dv_object_audit` - Per-object metrics (rows inserted, duration, status) 


### Final Result