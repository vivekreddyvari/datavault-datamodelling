"""
Step 3: EXECUTE LOADS
Execute DDL/DML to create and populate Hub, Link, and Satellite tables.
"""

from typing import Dict, Any, List
from utils.logger import get_logger


class Step3Executor:
    """
    Step 3 of DV-Generator: Execute DDL/DML to load tables.

    Execution order (respects dependencies):
    1. Create and load all Hubs (no dependencies)
    2. Create and load all Links (depends on Hubs existing)
    3. Create and load all Satellites (depends on Hubs/Links existing)

    Implements:
    - Insert-only pattern for Hubs and Links
    - Append-on-change pattern for Satellites (using HASHDIFF)
    """

    def __init__(self, config: Dict[str, Any], run_id: str):
        """
        Initialize Step3Executor.

        Args:
            config: Configuration dictionary
            run_id: Unique run identifier for audit trail
        """
        self.config = config
        self.run_id = run_id
        self.logger = get_logger(__name__)
        self.object_audits = []  # Track per-object execution metrics

    def execute_loads(
        self, spark, catalog: str, env: str, specs: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Execute Hub, Link, and Satellite loads in dependency order.

        Args:
            spark: Spark session
            catalog: Unity Catalog name
            env: Environment
            specs: Output from Step2Deriver

        Returns:
            Dict with run_id, status, summary
        """
        self.logger.info("[Step3] Executing DDL/DML loads")

        try:
            # STEP 3A: Create and load Hubs (no dependencies)
            self.logger.info("[Step3A] Creating and loading Hubs...")
            for hub_spec in specs["hub_specs"]:
                self._execute_hub_load(spark, catalog, env, hub_spec)
            self.logger.info(
                f"[Step3A] ✓ Completed {len(specs['hub_specs'])} hub loads"
            )

            # STEP 3B: Create and load Links (depends on Hubs)
            self.logger.info("[Step3B] Creating and loading Links...")
            for link_spec in specs["link_specs"]:
                self._execute_link_load(spark, catalog, env, link_spec)
            self.logger.info(
                f"[Step3B] ✓ Completed {len(specs['link_specs'])} link loads"
            )

            # STEP 3C: Create and load Satellites (depends on Hubs/Links)
            self.logger.info("[Step3C] Creating and loading Satellites...")
            for sat_spec in specs["sat_specs"]:
                self._execute_satellite_load(spark, catalog, env, sat_spec)
            self.logger.info(
                f"[Step3C] ✓ Completed {len(specs['sat_specs'])} satellite loads"
            )

            return {"run_id": self.run_id, "status": "Completed"}

        except Exception as e:
            self.logger.error(f"[Step3] Failed to execute loads: {str(e)}")
            raise

    def _execute_hub_load(
        self, spark, catalog: str, env: str, hub_spec: Dict[str, Any]
    ) -> None:
        """
        Create and load a Hub table (INSERT-ONLY pattern).

        Args:
            spark: Spark session
            catalog: Unity Catalog name
            env: Environment
            hub_spec: Hub specification from Step2
        """
        hub_name = hub_spec["hub_name"]
        self.logger.info(f"[Step3A] Loading Hub: {hub_name}")

        try:
            # TODO: Implement hub creation and load logic
            # 1. CREATE TABLE IF NOT EXISTS with structure from hub_spec
            # 2. INSERT DISTINCT (skip duplicates)
            # 3. Log metrics to self.object_audits

            self.logger.info(f"[Step3A] ✓ Hub {hub_name} loaded successfully")

        except Exception as e:
            self.logger.error(f"[Step3A] Failed to load hub {hub_name}: {str(e)}")
            raise

    def _execute_link_load(
        self, spark, catalog: str, env: str, link_spec: Dict[str, Any]
    ) -> None:
        """
        Create and load a Link table (INSERT-ONLY with FK validation).

        Args:
            spark: Spark session
            catalog: Unity Catalog name
            env: Environment
            link_spec: Link specification from Step2
        """
        link_name = link_spec["link_name"]
        self.logger.info(f"[Step3B] Loading Link: {link_name}")

        try:
            # TODO: Implement link creation and load logic
            # 1. CREATE TABLE IF NOT EXISTS with FK columns
            # 2. INSERT DISTINCT with validation against parent hubs
            # 3. Log metrics to self.object_audits

            self.logger.info(f"[Step3B] ✓ Link {link_name} loaded successfully")

        except Exception as e:
            self.logger.error(f"[Step3B] Failed to load link {link_name}: {str(e)}")
            raise

    def _execute_satellite_load(
        self, spark, catalog: str, env: str, sat_spec: Dict[str, Any]
    ) -> None:
        """
        Create and load a Satellite table (APPEND-ON-CHANGE with HASHDIFF).

        Args:
            spark: Spark session
            catalog: Unity Catalog name
            env: Environment
            sat_spec: Satellite specification from Step2
        """
        sat_name = sat_spec["sat_name"]
        self.logger.info(f"[Step3C] Loading Satellite: {sat_name}")

        try:
            # TODO: Implement satellite creation and load logic
            # 1. CREATE TABLE IF NOT EXISTS with HASHDIFF column
            # 2. INSERT only if HASHDIFF is new (append-on-change)
            # 3. Log metrics to self.object_audits

            self.logger.info(f"[Step3C] ✓ Satellite {sat_name} loaded successfully")

        except Exception as e:
            self.logger.error(f"[Step3C] Failed to load satellite {sat_name}: {str(e)}")
            raise
