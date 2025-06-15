# File location: scripts/run_with_config.py
"""
Script to run the application with different configuration files
"""
import asyncio
import sys
import argparse
import logging
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from main import OrderSyncApplication
from src.config.settings import get_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def run_with_config(config_file: str):
    """
    Run the application with a specific config file
    
    Args:
        config_file: Name of the config file (e.g., 'config-resync.yaml')
    """
    try:
        # Build config path
        config_path = Path(__file__).parent.parent / 'config' / config_file
        
        if not config_path.exists():
            logger.error(f"Config file not found: {config_path}")
            return
        
        logger.info(f"Running with config file: {config_path}")
        
        # Load config to check settings
        config = get_config(config_path)
        
        # Show important settings
        logger.info(f"Configuration loaded:")
        logger.info(f"  - Database: {config.database.database}")
        logger.info(f"  - API page size: {config.api.page_size}")
        logger.info(f"  - Skip duplicate checks: {config.sync.skip_duplicate_checks}")
        logger.info(f"  - Polling interval: {config.sync.polling_interval}s")
        
        if config.sync.skip_duplicate_checks:
            logger.warning("=" * 60)
            logger.warning("WARNING: Duplicate checks are DISABLED!")
            logger.warning("Orders will be reprocessed even if they exist.")
            logger.warning("=" * 60)
            
            # Get user confirmation
            response = input("\nAre you sure you want to continue? (yes/no): ")
            if response.lower() != 'yes':
                logger.info("Operation cancelled")
                return
        
        # Create and run application
        app = OrderSyncApplication()
        await app.run()
        
    except KeyboardInterrupt:
        logger.info("Application terminated by user")
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run sync with specific config file")
    parser.add_argument("config", help="Config file name (e.g., config.yaml, config-resync.yaml)")
    
    args = parser.parse_args()
    
    asyncio.run(run_with_config(args.config))