"""
DV-Generator Configuration
Layer 2: Dynamic configuration (can change without code redeployment)
"""

import os

# Main configuration dictionary
CONFIG = {
    # ===== CATALOG & SCHEMA SETTINGS =====
    "catalog": os.getenv("DV_CATALOG", "uc_catalog"),
    "metadata_schema": "Metadata{env}_metadataregistry",  # Format: MetadataDEV_metadataregistry

    # ===== NAMING CONVENTIONS =====
    "naming_rules": {
        "hub_prefix": "H_",
        "link_prefix": "L_",
        "sat_prefix": "S_",
        "activity_prefix": "A_",
    },

    # ===== HASH CONFIGURATION =====
    "hash_config": {
        "algo": "SHA2_256",  # Hashing algorithm
        "storage": "BINARY_32",  # Storage format: BINARY_32 or HEX_64
    },

    # ===== TARGET SCHEMAS =====
    "target_schemas": {
        "hub": "SilverDataVault{env}_dv_hub",  # Format: SilverDataVaultdev_dv_hub
        "link": "SilverDataVault{env}_dv_link",
        "sat": "SilverDataVault{env}_dv_satellite",
        "activity": "SilverDataVault{env}_dv_activity",
    },

    # ===== METADATA TABLE MAPPINGS =====
    "metadata_tables": {
        # Input registries (populated manually)
        "dataset_registry": "dv_dataset_registry",
        "nbr_input": "dv_nbr_input",
        "entity_owner": "dv_entity_owner",
        "naming_rules": "dv_naming_rules",
        "relationship_rules": "dv_relationship_rules",

        # Generated specification tables (auto-populated by Step2)
        "hub_spec": "dv_hub_spec",
        "link_spec": "dv_link_spec",
        "sat_spec": "dv_satellite_spec",
        "dataset_profile": "dv_dataset_profile",

        # Audit & results tables (populated by Step4)
        "generation_results": "dv_generation_results",
        "run_audit": "dv_run_audit",
        "object_audit": "dv_object_audit",
    },

    # ===== EXECUTION SETTINGS =====
    "execution": {
        "max_retries": 3,  # Number of retries for failed operations
        "timeout_seconds": 3600,  # Timeout for long-running operations
        "batch_size": 10000,  # Batch size for INSERT operations
        "generate_sql_file": True,  # Generate human-readable SQL output file
    },

    # ===== LOGGING SETTINGS =====
    "logging": {
        "level": os.getenv("DV_LOG_LEVEL", "INFO"),
        "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        "output_dir": "./logs",
    },
}


def get_config() -> dict:
    """
    Get the main configuration dictionary.

    Returns:
        CONFIG dict
    """
    return CONFIG


def get_metadata_schema(env: str) -> str:
    """
    Get the metadata schema name for a specific environment.

    Args:
        env: Environment (dev/staging/prod)

    Returns:
        Formatted schema name (e.g., Metadatadev_metadataregistry)
    """
    template = CONFIG["metadata_schema"]
    return template.format(env=env)


def get_target_schema(schema_type: str, env: str) -> str:
    """
    Get the target schema name for a specific environment.

    Args:
        schema_type: Type of schema (hub/link/sat/activity)
        env: Environment (dev/staging/prod)

    Returns:
        Formatted schema name
    """
    template = CONFIG["target_schemas"].get(schema_type)
    if not template:
        raise ValueError(f"Unknown schema type: {schema_type}")
    return template.format(env=env)
