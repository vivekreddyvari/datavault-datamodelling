# DV-Generator Folder Structure

Based on the algorithm.md, here's the recommended folder structure for organizing the generator codebase:

```
dv-generator/
│
├── 📋 Documentation
│   ├── concept.md                          # What DV-Generator does (high-level)
│   ├── algorithm.md                        # How DV-Generator works (detailed)
│   ├── FOLDER_STRUCTURE.md                 # This file
│   ├── GENERATOR_LOGIC.md                  # Business rules (patterns & extensions)
│   └── API_REFERENCE.md                    # Function/class documentation
│
├── 🔧 Core Layer (Layer 1 - Static)
│   └── core/
│       ├── __init__.py
│       ├── dv_generator_core.py            # Main orchestrator class
│       ├── step1_reader.py                 # Step 1: Read Input Registry
│       ├── step2_deriver.py                # Step 2: Derive Specifications
│       ├── step3_executor.py               # Step 3: Execute Loads (DDL/DML)
│       ├── step4_auditor.py                # Step 4: Record Results
│       └── exceptions.py                   # Custom exception classes
│
├── ⚙️ Configuration Layer (Layer 2 - Dynamic)
│   └── config/
│       ├── __init__.py
│       ├── dv_generator_config.py          # Main config constants
│       ├── naming_rules.py                 # Hub/Link/Sat naming conventions
│       ├── hash_config.py                  # Hash algorithm & storage config
│       ├── schema_config.py                # Target schema definitions
│       └── metadata_tables.py              # Metadata table mappings
│
├── 📦 Utilities & Helpers
│   └── utils/
│       ├── __init__.py
│       ├── logger.py                       # Logging setup
│       ├── spark_utils.py                  # Spark-specific utilities
│       ├── hash_utils.py                   # Hash key generation helpers
│       ├── sql_builder.py                  # SQL generation utilities
│       ├── validators.py                   # Input validation functions
│       └── decorators.py                   # Retry, timing, logging decorators
│
├── 🗂️ Input/Output
│   ├── inputs/
│   │   ├── __init__.py
│   │   ├── metadata_reader.py              # Read from metadata tables
│   │   └── dataset_validator.py            # Validate source datasets
│   │
│   └── outputs/
│       ├── __init__.py
│       ├── spec_writer.py                  # Write to dv_*_spec tables
│       ├── audit_writer.py                 # Write to dv_*_audit tables
│       ├── sql_generator.py                # Generate SQL files
│       └── generated_specs/                # Folder for SQL output files
│           └── .gitkeep
│
├── 🧪 Tests
│   └── tests/
│       ├── __init__.py
│       ├── unit/
│       │   ├── test_step1_reader.py
│       │   ├── test_step2_deriver.py
│       │   ├── test_step3_executor.py
│       │   └── test_step4_auditor.py
│       ├── integration/
│       │   ├── test_end_to_end.py
│       │   └── test_metadata_flow.py
│       ├── fixtures/
│       │   ├── sample_metadata.py           # Test data
│       │   ├── sample_datasets.py
│       │   └── expected_outputs.py
│       └── conftest.py                     # Pytest configuration
│
├── 📚 Examples & Recipes
│   └── examples/
│       ├── simple_relationship.py          # Simple 2-entity relationship
│       ├── complex_many_to_many.py         # Many-to-many example
│       ├── with_custom_rules.py            # Custom naming/attribute rules
│       └── notebooks/
│           ├── 01_run_generator.ipynb      # Jupyter notebook example
│           └── 02_review_output.ipynb
│
├── 🚀 Entrypoints
│   ├── main.py                             # Main entry point (CLI or batch)
│   ├── run_generator.py                    # Orchestration script
│   └── notebooks/
│       └── generator_notebook.ipynb        # Databricks notebook wrapper
│
├── 📝 Configuration Files
│   ├── requirements.txt                    # Python dependencies
│   ├── setup.py                            # Package setup
│   ├── .env.example                        # Environment variables template
│   └── logging.config                      # Logging configuration
│
└── 📖 README.md                             # Getting started guide

```

---

## Folder Descriptions

### 1. **core/** - Layer 1: Static Core Logic
**Purpose:** Implements the 4-step algorithm (never changes, no AI modifications)

**Files:**
- `dv_generator_core.py` - Main DVGenerator class that orchestrates all steps
- `step1_reader.py` - Reads metadata registry, validates sources
- `step2_deriver.py` - Transforms relationships into specifications
- `step3_executor.py` - Generates and executes DDL/DML
- `step4_auditor.py` - Records results in audit tables
- `exceptions.py` - Custom exceptions (MissingEntityError, InvalidRelationshipError, etc.)

**Dependencies:** config/*, utils/*

---

### 2. **config/** - Layer 2: Dynamic Configuration
**Purpose:** Centralized configuration (changes without code redeployment)

**Files:**
- `dv_generator_config.py` - Main CONFIG dict with all settings
- `naming_rules.py` - Hub/Link/Sat prefix definitions
- `hash_config.py` - Hash algorithm (SHA2_256) and storage format (BINARY_32)
- `schema_config.py` - Target schema names for each environment
- `metadata_tables.py` - Maps logical names to physical table names

**Why separate files?**
- Each config domain is independent
- Easy to override per environment (dev/staging/prod)
- Can be loaded from environment variables

---

### 3. **utils/** - Reusable Helpers
**Purpose:** Utility functions used across all steps

**Files:**
- `logger.py` - Consistent logging across the generator
- `spark_utils.py` - Spark session creation, SQL execution wrappers
- `hash_utils.py` - SHA256 hashing, hash key generation
- `sql_builder.py` - DDL/DML construction (CREATE TABLE, INSERT, etc.)
- `validators.py` - Validate inputs (entities exist, BK columns present, etc.)
- `decorators.py` - Timing, retry logic, error handling decorators

---

### 4. **inputs/** - Input Handling
**Purpose:** Read and validate input data

**Files:**
- `metadata_reader.py` - Query dv_dataset_registry, dv_entity_owner, etc.
- `dataset_validator.py` - Verify datasets exist, columns are present

---

### 5. **outputs/** - Output Generation
**Purpose:** Write specifications and results

**Files:**
- `spec_writer.py` - INSERT into dv_hub_spec, dv_link_spec, dv_satellite_spec
- `audit_writer.py` - INSERT into dv_generation_results, dv_run_audit, dv_object_audit
- `sql_generator.py` - Generate human-readable SQL file from specs
- `generated_specs/` - Folder to store output SQL files

---

### 6. **tests/** - Testing
**Purpose:** Unit, integration, and fixture tests

**Structure:**
- `unit/` - Test individual steps in isolation
- `integration/` - Test full end-to-end workflow
- `fixtures/` - Reusable test data and mock metadata

---

### 7. **examples/** - Usage Examples
**Purpose:** Show how to use the generator

**Files:**
- `simple_relationship.py` - Minimal working example
- `complex_many_to_many.py` - Advanced example
- `notebooks/` - Jupyter/Databricks notebooks showing the generator in action

---

### 8. **Root Level**
- `main.py` - CLI entry point (e.g., `python main.py --env dev`)
- `run_generator.py` - Orchestration script that ties everything together
- `requirements.txt` - Python dependencies (pyspark, pytest, etc.)
- `README.md` - Getting started guide

---

## File Organization Strategy

### By Layer (Algorithm)
```
core/           → Step1Reader, Step2Deriver, Step3Executor, Step4Auditor
config/         → Layer 2 (naming rules, schemas, etc.)
inputs/         → Read metadata (used by Step1)
outputs/        → Write specs/audit (used by Step2, Step3, Step4)
utils/          → Shared helpers used by all layers
```

### By Concern (Separation of Concerns)
```
core/           → Business logic (the algorithm)
config/         → Settings (what to use)
utils/          → Mechanics (how to do things)
tests/          → Validation (does it work?)
examples/       → Documentation (how to use it?)
```

---

## Import Patterns

**Minimal Imports:**
```python
# In main.py or run_generator.py
from core.dv_generator_core import DVGenerator
from config.dv_generator_config import CONFIG

generator = DVGenerator(CONFIG)
generator.run(spark, catalog='uc_catalog', env='dev')
```

**Step 1 Imports:**
```python
# In core/step1_reader.py
from inputs.metadata_reader import MetadataReader
from utils.validators import validate_datasets
from utils.logger import get_logger
```

**Step 2 Imports:**
```python
# In core/step2_deriver.py
from config.naming_rules import NamingRules
from config.hash_config import HashConfig
from utils.sql_builder import build_hub_spec
from outputs.spec_writer import write_hub_spec
```

---

## Conventions

### Class Naming
- Readers: `MetadataReader`, `DatasetValidator`
- Builders: `SQLBuilder`, `SpecBuilder`
- Writers: `SpecWriter`, `AuditWriter`
- Managers: `DVGenerator`, `ConfigManager`

### Function Naming
- Queries: `read_*()`, `query_*()`
- Transforms: `derive_*()`, `build_*()`
- Executes: `execute_*()`, `run_*()`
- Writes: `write_*()`, `log_*()`

### File Naming
- Classes: `class_name.py` (e.g., `metadata_reader.py`)
- Test files: `test_module_name.py`
- Config files: `config_name.py`

---

## Next Steps

1. **Create the folder structure** in your repo
2. **Start with core/dv_generator_core.py** - Main orchestrator
3. **Implement Step 1** in core/step1_reader.py
4. **Implement Step 2** in core/step2_deriver.py
5. **Implement Step 3** in core/step3_executor.py
6. **Implement Step 4** in core/step4_auditor.py
7. **Add tests** as you go in tests/unit/
8. **Create examples** in examples/

---
