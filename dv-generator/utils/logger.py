"""
Logging utility for DV-Generator
Provides consistent logging across all modules.
"""

import logging
import os
from config.dv_generator_config import get_config

# Get logger configuration
CONFIG = get_config()
LOG_CONFIG = CONFIG.get("logging", {})
LOG_LEVEL = LOG_CONFIG.get("level", "INFO")
LOG_FORMAT = LOG_CONFIG.get(
    "format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
LOG_OUTPUT_DIR = LOG_CONFIG.get("output_dir", "./logs")

# Create logs directory if it doesn't exist
os.makedirs(LOG_OUTPUT_DIR, exist_ok=True)


def get_logger(name: str) -> logging.Logger:
    """
    Get a configured logger instance.

    Args:
        name: Logger name (typically __name__)

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)

    # Set level
    logger.setLevel(LOG_LEVEL)

    # Skip if already configured (avoid duplicate handlers)
    if logger.hasHandlers():
        return logger

    # Create formatter
    formatter = logging.Formatter(LOG_FORMAT)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler
    log_file = os.path.join(LOG_OUTPUT_DIR, "dv_generator.log")
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
