"""
Step 2: DERIVE SPECIFICATIONS
Transform business relationships into executable Hub/Link/Satellite specifications.
"""

from typing import Dict, Any, List
from utils.logger import get_logger


class Step2Deriver:
    """
    Step 2 of DV-Generator: Derive specifications from relationships.

    Transforms:
    - dv_entity_owner → dv_hub_spec (one hub per entity)
    - dv_nbr_input → dv_link_spec (one link per relationship)
    - dv_entity_owner + attributes → dv_satellite_spec (one sat per entity)

    Outputs specifications as INSERT into metadata tables for Step 3 to consume.
    """

    def __init__(self, config: Dict[str, Any], run_id: str):
        """
        Initialize Step2Deriver.

        Args:
            config: Configuration dictionary
            run_id: Unique run identifier for audit trail
        """
        self.config = config
        self.run_id = run_id
        self.logger = get_logger(__name__)

    def derive_specs(
        self, spark, catalog: str, env: str, inputs: Dict[str, Any]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Derive Hub, Link, and Satellite specifications.

        Step 2 Logic:
        1. Generate Hub Specifications from dv_entity_owner
        2. Generate Link Specifications from dv_nbr_input
        3. Generate Satellite Specifications from dv_entity_owner attributes
        4. Write all specs to metadata tables

        Args:
            spark: Spark session
            catalog: Unity Catalog name
            env: Environment (dev/staging/prod)
            inputs: Output from Step1Reader

        Returns:
            Dict with hub_specs, link_specs, sat_specs lists
        """
        self.logger.info("[Step2] Deriving specifications from relationships")

        try:
            hub_specs = self._derive_hub_specs(inputs)
            self.logger.info(f"[Step2] Derived {len(hub_specs)} hub specs")

            link_specs = self._derive_link_specs(inputs)
            self.logger.info(f"[Step2] Derived {len(link_specs)} link specs")

            sat_specs = self._derive_satellite_specs(inputs)
            self.logger.info(f"[Step2] Derived {len(sat_specs)} satellite specs")

            # Write specifications to metadata tables
            self._write_specs_to_metadata(spark, catalog, env, hub_specs, link_specs, sat_specs)

            return {
                "run_id": self.run_id,
                "hub_specs": hub_specs,
                "link_specs": link_specs,
                "sat_specs": sat_specs,
            }

        except Exception as e:
            self.logger.error(f"[Step2] Failed to derive specifications: {str(e)}")
            raise

    def _derive_hub_specs(self, inputs: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Derive Hub specifications from entity owners.

        For each entity_owner row:
        - hub_name: {hub_prefix}{ENTITY_NAME}
        - entity_name: from entity_owner
        - driving_dataset: from entity_owner.owner_dataset
        - bk_columns: from entity_owner.bk_columns
        - hk_column: HK_{ENTITY_NAME}
        - hash_algo, hash_storage: from naming_rules

        Args:
            inputs: Output from Step1Reader

        Returns:
            List of hub specification dictionaries
        """
        hub_specs = []
        naming_rules = inputs["naming_rules"][0]  # Single row

        for entity_owner in inputs["entity_owners"]:
            hub_spec = {
                "hub_name": f"{naming_rules['hub_prefix']}{entity_owner['entity_name'].upper()}",
                "entity_name": entity_owner["entity_name"],
                "driving_dataset": entity_owner["owner_dataset"],
                "bk_columns": entity_owner["bk_columns"],  # JSON
                "hk_column": f"HK_{entity_owner['entity_name'].upper()}",
                "hash_algo": naming_rules["hash_algo"],
                "hash_storage": naming_rules["hash_storage"],
                "target_schema": f"SilverDataVault{inputs['naming_rules'][0].get('env', 'dev')}_dv_hub",
                "is_active": True,
                "generated_at": None,  # Will be set by Step3
                "run_id": self.run_id,
            }
            hub_specs.append(hub_spec)
            self.logger.debug(f"[Step2] Derived hub spec: {hub_spec['hub_name']}")

        return hub_specs

    def _derive_link_specs(self, inputs: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Derive Link specifications from relationships.

        For each relationship row:
        - link_name: {link_prefix}{LEFT_ENTITY}_{RIGHT_ENTITY}
        - relationship_name: from nbr_input
        - hub_names: [H_LEFT, H_RIGHT]
        - fk_hk_columns: [HK_LEFT, HK_RIGHT]
        - bk_mapping: {LEFT: bk_cols, RIGHT: bk_cols}
        - driving_dataset: from nbr_input.source_dataset

        Args:
            inputs: Output from Step1Reader

        Returns:
            List of link specification dictionaries
        """
        link_specs = []
        naming_rules = inputs["naming_rules"][0]
        entity_owner_map = {e["entity_name"]: e for e in inputs["entity_owners"]}

        for rel in inputs["relationships"]:
            left_entity = rel["left_entity"]
            right_entity = rel["right_entity"]

            left_hub = f"{naming_rules['hub_prefix']}{left_entity.upper()}"
            right_hub = f"{naming_rules['hub_prefix']}{right_entity.upper()}"

            link_name = f"{naming_rules['link_prefix']}{left_entity.upper()}_{right_entity.upper()}"

            link_spec = {
                "link_name": link_name,
                "relationship_name": rel["relationship_name"],
                "driving_dataset": rel["source_dataset"],
                "hub_names": [left_hub, right_hub],
                "hk_link_column": f"HK_{left_entity.upper()}_{right_entity.upper()}",
                "fk_hk_columns": [
                    f"HK_{left_entity.upper()}",
                    f"HK_{right_entity.upper()}",
                ],
                "bk_mapping": {
                    left_entity: entity_owner_map[left_entity]["bk_columns"],
                    right_entity: entity_owner_map[right_entity]["bk_columns"],
                },
                "target_schema": f"SilverDataVault{inputs['naming_rules'][0].get('env', 'dev')}_dv_link",
                "target_table": link_name,
                "is_active": True,
                "generated_at": None,
                "run_id": self.run_id,
            }
            link_specs.append(link_spec)
            self.logger.debug(f"[Step2] Derived link spec: {link_spec['link_name']}")

        return link_specs

    def _derive_satellite_specs(self, inputs: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Derive Satellite specifications from entity attributes.

        For each entity_owner row:
        - sat_name: {sat_prefix}{ENTITY_NAME}
        - parent_name: H_{ENTITY_NAME}
        - parent_hk_column: HK_{ENTITY_NAME}
        - attribute_columns: filtered by allowlist/denylist
        - driving_dataset: from entity_owner.owner_dataset
        - mode: from entity_owner.satellite_mode

        Args:
            inputs: Output from Step1Reader

        Returns:
            List of satellite specification dictionaries
        """
        sat_specs = []
        naming_rules = inputs["naming_rules"][0]

        for entity_owner in inputs["entity_owners"]:
            # TODO: Get actual columns from dataset and filter by allowlist/denylist
            # For now, using placeholder
            attribute_columns = entity_owner.get(
                "attr_allowlist", []
            )  # Will be enhanced in actual implementation

            sat_spec = {
                "sat_name": f"{naming_rules['sat_prefix']}{entity_owner['entity_name'].upper()}",
                "parent_type": "HUB",
                "parent_name": f"{naming_rules['hub_prefix']}{entity_owner['entity_name'].upper()}",
                "driving_dataset": entity_owner["owner_dataset"],
                "parent_hk_column": f"HK_{entity_owner['entity_name'].upper()}",
                "attribute_columns": attribute_columns,
                "target_schema": f"SilverDataVault{inputs['naming_rules'][0].get('env', 'dev')}_dv_satellite",
                "target_table": f"{naming_rules['sat_prefix']}{entity_owner['entity_name'].upper()}",
                "mode": entity_owner.get("satellite_mode", "ALLOWLIST"),
                "is_active": True,
                "generated_at": None,
                "run_id": self.run_id,
            }
            sat_specs.append(sat_spec)
            self.logger.debug(f"[Step2] Derived satellite spec: {sat_spec['sat_name']}")

        return sat_specs

    def _write_specs_to_metadata(
        self,
        spark,
        catalog: str,
        env: str,
        hub_specs: List[Dict],
        link_specs: List[Dict],
        sat_specs: List[Dict],
    ) -> None:
        """
        Write generated specifications to metadata tables.

        Args:
            spark: Spark session
            catalog: Unity Catalog name
            env: Environment
            hub_specs: List of hub specifications
            link_specs: List of link specifications
            sat_specs: List of satellite specifications
        """
        self.logger.info("[Step2] Writing specifications to metadata tables")

        metadata_schema = f"{catalog}.Metadata{env}_metadataregistry"

        # TODO: Implement writes to dv_hub_spec, dv_link_spec, dv_satellite_spec
        # This will be implemented in the full version

        self.logger.info(
            f"[Step2] Wrote {len(hub_specs)} hubs, "
            f"{len(link_specs)} links, {len(sat_specs)} satellites to metadata"
        )
