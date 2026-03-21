# DV-Generator: Data Vault Automated Generation Tool

A metadata-driven utility that automatically generates Data Vault 2.0 Hub, Link, and Satellite tables from business relationship definitions.

## 📋 Documentation

- **[concept.md](concept.md)** - What DV-Generator does (high-level overview)
- **[algorithm.md](algorithm.md)** - How DV-Generator works (detailed 4-step algorithm)
- **[FOLDER_STRUCTURE.md](FOLDER_STRUCTURE.md)** - Directory organization and file purposes
- **[STRUCTURE_CREATED.md](STRUCTURE_CREATED.md)** - Status of folder creation and next steps

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- PySpark 3.0+
- Databricks environment with Unity Catalog
- Metadata registry tables populated (dv_dataset_registry, dv_nbr_input, dv_entity_owner, dv_naming_rules)

### Installation
```bash
# Install dependencies
pip install -r requirements.txt
```

### Usage
```bash
# Run the generator for dev environment
python main.py --catalog uc_catalog --env dev

# Run for production
python main.py --catalog uc_catalog --env prod
```

## 🏗️ Architecture

### 4-Step Workflow

```
Step 1: READ INPUT REGISTRY
├─ Query metadata registry
├─ Validate sources exist
└─ Output: datasets, relationships, entities

Step 2: DERIVE SPECIFICATIONS
├─ Transform relationships to specs
├─ Generate Hub/Link/Satellite definitions
└─ Output: dv_hub_spec, dv_link_spec, dv_satellite_spec

Step 3: EXECUTE LOADS
├─ Create Hub tables (insert-only)
├─ Create Link tables (with FK validation)
└─ Create Satellite tables (append-on-change)

Step 4: RECORD RESULTS
├─ Log generation results
├─ Log per-run audit trail
└─ Log per-object execution metrics
```

### Layer Organization

**Layer 1: Static Core** (`core/`)
- Never changes, implements the algorithm
- Step1Reader, Step2Deriver, Step3Executor, Step4Auditor
- DVGenerator orchestrator

**Layer 2: Dynamic Configuration** (`config/`)
- Changes per environment
- Naming rules, schema definitions, metadata table mappings
- Can be overridden without code redeployment

**Layer 3: Utilities** (`utils/`)
- Reusable helpers (logging, hashing, SQL building)
- Used across all steps

**Layer 4: Inputs/Outputs** (`inputs/`, `outputs/`)
- Read metadata, write specifications
- Generate output SQL files

## 📁 Folder Structure

```
dv-generator/
├── core/                    # Layer 1: 4-step algorithm
│   ├── dv_generator_core.py  # Main orchestrator
│   ├── step1_reader.py       # Read metadata
│   ├── step2_deriver.py      # Derive specs
│   ├── step3_executor.py     # Execute DDL/DML
│   └── step4_auditor.py      # Record results
│
├── config/                  # Layer 2: Configuration
│   └── dv_generator_config.py # Settings, naming rules, schemas
│
├── utils/                   # Reusable utilities
│   └── logger.py            # Logging
│
├── inputs/                  # Input handling
│   └── (TODO: metadata readers)
│
├── outputs/                 # Output generation
│   ├── generated_specs/     # Generated SQL files
│   └── (TODO: spec/audit writers)
│
├── tests/                   # Unit & integration tests
│   ├── unit/
│   ├── integration/
│   └── fixtures/
│
├── examples/                # Usage examples
│   └── notebooks/
│
└── main.py                  # CLI entry point
```

## 🔧 Configuration

Edit `config/dv_generator_config.py` to customize:

```python
CONFIG = {
    "catalog": "uc_catalog",                    # Unity Catalog name
    "naming_rules": {
        "hub_prefix": "H_",                     # Hub naming prefix
        "link_prefix": "L_",                    # Link naming prefix
        "sat_prefix": "S_",                     # Satellite naming prefix
    },
    "hash_config": {
        "algo": "SHA2_256",                     # Hashing algorithm
        "storage": "BINARY_32",                 # Binary or Hex format
    },
    "target_schemas": {
        "hub": "SilverDataVault{env}_dv_hub",  # Hub schema
        "link": "SilverDataVault{env}_dv_link",
        "sat": "SilverDataVault{env}_dv_satellite",
    }
}
```

## 📊 Data Flow

```
Metadata Registry Input
├─ dv_dataset_registry          (What data sources exist)
├─ dv_nbr_input                 (What relationships exist)
├─ dv_entity_owner              (Entity-to-dataset mappings)
├─ dv_naming_rules              (Naming conventions)
└─ dv_relationship_rules        (Link validation rules)
     │
     ▼
Step 1: READ INPUT REGISTRY
     │
     ▼
Step 2: DERIVE SPECIFICATIONS
     │
     ├─ dv_hub_spec             (Generated Hub definitions)
     ├─ dv_link_spec            (Generated Link definitions)
     └─ dv_satellite_spec       (Generated Satellite definitions)
     │
     ▼
Step 3: EXECUTE LOADS
     │
     ├─ H_* tables created      (Hub tables)
     ├─ L_* tables created      (Link tables)
     └─ S_* tables created      (Satellite tables)
     │
     ▼
Step 4: RECORD RESULTS
     │
     ├─ dv_generation_results   (What was generated)
     ├─ dv_run_audit            (When and who executed)
     └─ dv_object_audit         (Per-table metrics)
```

## 🧪 Testing

```bash
# Run all tests
pytest tests/ -v

# Run unit tests only
pytest tests/unit/ -v

# Run integration tests
pytest tests/integration/ -v

# Run with coverage
pytest tests/ --cov=core --cov=config --cov=utils
```

## 📝 GENERATOR_LOGIC.md

Business rules are documented in [GENERATOR_LOGIC.md](GENERATOR_LOGIC.md):
- Relationship interpretation rules (one-to-many, many-to-many)
- Attribute classification rules (allowlist, auto-add, locked)
- Naming convention rules
- Error handling rules
- Extension points for custom logic

## 🔐 Security & Governance

- **Audit Trail**: Every generation is tracked with unique run_id
- **Attribute Control**: Allowlist/denylist prevents sensitive data exposure
- **Validation**: All inputs validated before execution
- **Error Handling**: Failures logged with full context
- **Traceability**: All decisions recorded for compliance

## 📚 Examples

See `examples/` directory for:
- `simple_relationship.py` - Basic 2-entity example
- `complex_many_to_many.py` - Advanced relationship example
- `notebooks/01_run_generator.ipynb` - Jupyter notebook walkthrough

## 🐛 Troubleshooting

### Missing Metadata
```
Error: No active datasets found in dv_dataset_registry
→ Ensure dv_dataset_registry is populated
```

### Schema Mismatch
```
Error: Business key columns not found in source dataset
→ Verify dv_entity_owner.bk_columns match actual dataset columns
```

### FK Validation Failed
```
Error: Hub record not found for link foreign key
→ Ensure Hubs are created before Links are loaded
```

## 🤝 Contributing

1. Update `GENERATOR_LOGIC.md` for new business rules
2. Implement in corresponding step class
3. Add unit tests in `tests/unit/`
4. Add integration tests in `tests/integration/`

## 📄 License

[Your license here]

---

**Status**: Schema skeleton created. Core implementation in progress.

**Next Steps**:
1. Complete Step 1-4 implementation
2. Add utilities (hash_utils, sql_builder, validators)
3. Add input/output handlers
4. Write comprehensive tests
5. Create usage examples
