# File location: src/config/test_config.py
import logging
from pathlib import Path
from settings import Config, get_config

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    try:
        logger.debug("Testing configuration loading...")
        config = get_config()
        logger.debug(f"Configuration loaded successfully:")
        logger.debug(f"API URL: {config.api.base_url}")
        logger.debug(f"Database: {config.database.database}")
        logger.debug(f"Polling interval: {config.polling_interval}")
        logger.debug(f"Polling interval: {config.logging.level}")
    except Exception as e:
        logger.error(f"Failed to load configuration: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise