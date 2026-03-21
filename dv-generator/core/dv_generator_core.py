"""
DV-Generator Core: Main orchestrator for the 4-step Data Vault generation workflow
"""

import uuid
from datetime import datetime
from typing import Dict, Any

from core.step1_reader import Step1Reader
from core.step2_deriver import Step2Deriver
from core.step3_executor import Step3Executor
from core.step4_auditor import Step4Auditor
from utils.logger import get_logger


class DVGenerator:
    """
    Main orchestrator for DV-Generator.
    Coordinates all 4 steps: Read → Derive → Execute → Audit
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the generator with configuration.

        Args:
            config: Configuration dictionary from dv_generator_config.CONFIG
        """
        self.config = config
        self.logger = get_logger(__name__)
        self.run_id = None
        self.run_start_time = None

    def run(self, spark, catalog: str, env: str) -> Dict[str, Any]:
        """
        Execute the complete DV-Generator workflow.

        Step 1: READ INPUT REGISTRY
        ├─ Query: dv_dataset_registry, dv_nbr_input, dv_entity_owner, dv_naming_rules
        ├─ Validate: All sources exist
        └─ Output: inputs dict

        Step 2: DERIVE SPECIFICATIONS
        ├─ Input: Relationships, entities, naming rules
        ├─ Logic: Generate Hub/Link/Satellite specs
        └─ Output: dv_hub_spec, dv_link_spec, dv_satellite_spec

        Step 3: EXECUTE LOADS
        ├─ Order: Hubs → Links → Satellites (respects dependencies)
        ├─ Action: Execute DDL/DML
        └─ Output: Created/populated tables

        Step 4: RECORD RESULTS
        ├─ Action: Log generation results
        └─ Output: dv_generation_results, dv_run_audit, dv_object_audit

        Args:
            spark: Spark session
            catalog: Unity Catalog name (e.g., 'uc_catalog')
            env: Environment (dev/staging/prod)

        Returns:
            Dict with run_id, status, summary
        """
        self.run_id = str(uuid.uuid4())
        self.run_start_time = datetime.now()

        self.logger.info(f"=== DV-GENERATOR START ===")
        self.logger.info(f"Run ID: {self.run_id}")
        self.logger.info(f"Catalog: {catalog}")
        self.logger.info(f"Environment: {env}")

        try:
            # STEP 1: READ INPUT REGISTRY
            self.logger.info("Step 1: READ INPUT REGISTRY")
            step1 = Step1Reader(self.config, self.run_id)
            inputs = step1.read_registry(spark, catalog, env)
            self.logger.info(
                f"✓ Step 1 Complete: "
                f"{len(inputs['datasets'])} datasets, "
                f"{len(inputs['relationships'])} relationships"
            )

            # STEP 2: DERIVE SPECIFICATIONS
            self.logger.info("Step 2: DERIVE SPECIFICATIONS")
            step2 = Step2Deriver(self.config, self.run_id)
            specs = step2.derive_specs(spark, catalog, env, inputs)
            self.logger.info(
                f"✓ Step 2 Complete: "
                f"{len(specs['hub_specs'])} hub specs, "
                f"{len(specs['link_specs'])} link specs, "
                f"{len(specs['sat_specs'])} satellite specs"
            )

            # STEP 3: EXECUTE LOADS
            self.logger.info("Step 3: EXECUTE LOADS")
            step3 = Step3Executor(self.config, self.run_id)
            result = step3.execute_loads(spark, catalog, env, specs)
            self.logger.info(f"✓ Step 3 Complete: All tables created and loaded")

            # STEP 4: RECORD RESULTS
            self.logger.info("Step 4: RECORD RESULTS")
            step4 = Step4Auditor(self.config, self.run_id)
            step4.record_results(spark, catalog, env, "Completed", None)
            self.logger.info(f"✓ Step 4 Complete: Audit trails recorded")

            self.logger.info(f"=== DV-GENERATOR SUCCESS ===")
            return {
                "run_id": self.run_id,
                "status": "Completed",
                "hubs_created": len(specs["hub_specs"]),
                "links_created": len(specs["link_specs"]),
                "satellites_created": len(specs["sat_specs"]),
                "duration_seconds": (
                    datetime.now() - self.run_start_time
                ).total_seconds(),
            }

        except Exception as e:
            self.logger.error(f"DV-GENERATOR FAILED: {str(e)}", exc_info=True)

            # Record failure
            try:
                step4 = Step4Auditor(self.config, self.run_id)
                step4.record_results(spark, catalog, env, "Error", str(e))
            except Exception as audit_error:
                self.logger.error(f"Failed to record error audit: {str(audit_error)}")

            return {
                "run_id": self.run_id,
                "status": "Error",
                "error": str(e),
                "duration_seconds": (
                    datetime.now() - self.run_start_time
                ).total_seconds(),
            }
