"""
DV-Generator Main Entry Point
Orchestrates the entire Data Vault generation workflow.
"""

import argparse
from pyspark.sql import SparkSession
from core.dv_generator_core import DVGenerator
from config.dv_generator_config import get_config
from utils.logger import get_logger


def main():
    """
    Main entry point for DV-Generator.

    Usage:
        python main.py --catalog uc_catalog --env dev
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="DV-Generator: Automated Data Vault Table Generation"
    )
    parser.add_argument(
        "--catalog",
        type=str,
        default="uc_catalog",
        help="Unity Catalog name (default: uc_catalog)",
    )
    parser.add_argument(
        "--env",
        type=str,
        required=True,
        choices=["dev", "staging", "prod"],
        help="Environment (dev/staging/prod)",
    )

    args = parser.parse_args()

    # Initialize logger
    logger = get_logger(__name__)
    logger.info("=" * 80)
    logger.info("DV-GENERATOR STARTING")
    logger.info(f"Catalog: {args.catalog}")
    logger.info(f"Environment: {args.env}")
    logger.info("=" * 80)

    try:
        # Initialize Spark session
        spark = SparkSession.builder.appName("DV-Generator").getOrCreate()
        logger.info("✓ Spark session created")

        # Get configuration
        config = get_config()

        # Create and run generator
        generator = DVGenerator(config)
        result = generator.run(spark, catalog=args.catalog, env=args.env)

        # Log results
        logger.info("=" * 80)
        logger.info("DV-GENERATOR RESULTS")
        logger.info(f"Status: {result['status']}")
        logger.info(f"Run ID: {result['run_id']}")
        if result['status'] == 'Completed':
            logger.info(f"Hubs created: {result.get('hubs_created', 0)}")
            logger.info(f"Links created: {result.get('links_created', 0)}")
            logger.info(f"Satellites created: {result.get('satellites_created', 0)}")
        else:
            logger.error(f"Error: {result.get('error', 'Unknown error')}")
        logger.info(f"Duration: {result['duration_seconds']:.2f} seconds")
        logger.info("=" * 80)

        return 0 if result['status'] == 'Completed' else 1

    except Exception as e:
        logger.error(f"DV-GENERATOR FAILED: {str(e)}", exc_info=True)
        return 1


if __name__ == "__main__":
    exit(main())
