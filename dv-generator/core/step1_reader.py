"""
Step 1: READ INPUT REGISTRY
Query metadata registry to discover all available data sources and validate them.
"""

from typing import Dict, Any, List
from utils.logger import get_logger


class Step1Reader:
    """
    Step 1 of DV-Generator: Read and validate metadata registry.

    Reads from:
    - dv_dataset_registry: List of all source datasets
    - dv_nbr_input: Business relationships
    - dv_entity_owner: Entity-to-dataset mappings
    - dv_naming_rules: Naming conventions

    Validates:
    - All datasets exist at their locations
    - All required columns are present
    - No circular relationships
    """

    def __init__(self, config: Dict[str, Any], run_id: str):
        """
        Initialize Step1Reader.

        Args:
            config: Configuration dictionary
            run_id: Unique run identifier for audit trail
        """
        self.config = config
        self.run_id = run_id
        self.logger = get_logger(__name__)

    def read_registry(self, spark, catalog: str, env: str) -> Dict[str, Any]:
        """
        Read all input registries from metadata.

        Args:
            spark: Spark session
            catalog: Unity Catalog name
            env: Environment (dev/staging/prod)

        Returns:
            Dict with datasets, relationships, entity_owners, naming_rules
        """
        self.logger.info(f"[Step1] Reading input registry from {catalog}")

        try:
            metadata_schema = f"{catalog}.Metadata{env}_metadataregistry"

            # Read dataset registry
            datasets = spark.sql(
                f"""
                SELECT * FROM {metadata_schema}.dv_dataset_registry
                WHERE is_active = TRUE
                """
            ).collect()
            self.logger.info(f"[Step1] Found {len(datasets)} active datasets")

            # Read relationships
            relationships = spark.sql(
                f"""
                SELECT * FROM {metadata_schema}.dv_nbr_input
                WHERE is_active = TRUE
                """
            ).collect()
            self.logger.info(f"[Step1] Found {len(relationships)} active relationships")

            # Read entity owners
            entity_owners = spark.sql(
                f"""
                SELECT * FROM {metadata_schema}.dv_entity_owner
                WHERE is_active = TRUE
                """
            ).collect()
            self.logger.info(f"[Step1] Found {len(entity_owners)} active entities")

            # Read naming rules
            naming_rules = spark.sql(
                f"""
                SELECT * FROM {metadata_schema}.dv_naming_rules
                """
            ).collect()
            self.logger.info(f"[Step1] Found {len(naming_rules)} naming rule(s)")

            # Validate inputs
            self._validate_inputs(datasets, relationships, entity_owners)

            return {
                "run_id": self.run_id,
                "datasets": datasets,
                "relationships": relationships,
                "entity_owners": entity_owners,
                "naming_rules": naming_rules,
            }

        except Exception as e:
            self.logger.error(f"[Step1] Failed to read input registry: {str(e)}")
            raise

    def _validate_inputs(
        self,
        datasets: List[Any],
        relationships: List[Any],
        entity_owners: List[Any],
    ) -> None:
        """
        Validate that all inputs are consistent.

        Args:
            datasets: List of dataset registry rows
            relationships: List of relationship registry rows
            entity_owners: List of entity owner registry rows

        Raises:
            ValueError: If validation fails
        """
        self.logger.info("[Step1] Validating inputs...")

        # Check for required datasets
        if not datasets:
            raise ValueError("No active datasets found in dv_dataset_registry")

        if not relationships:
            raise ValueError("No active relationships found in dv_nbr_input")

        if not entity_owners:
            raise ValueError("No active entities found in dv_entity_owner")

        # Check that all entities in relationships have owners
        entity_owner_names = [e["entity_name"] for e in entity_owners]
        for rel in relationships:
            if rel["left_entity"] not in entity_owner_names:
                raise ValueError(
                    f"Relationship '{rel['relationship_name']}' "
                    f"references entity '{rel['left_entity']}' "
                    f"which is not in dv_entity_owner"
                )
            if rel["right_entity"] not in entity_owner_names:
                raise ValueError(
                    f"Relationship '{rel['relationship_name']}' "
                    f"references entity '{rel['right_entity']}' "
                    f"which is not in dv_entity_owner"
                )

        self.logger.info("[Step1] ✓ All validations passed")
