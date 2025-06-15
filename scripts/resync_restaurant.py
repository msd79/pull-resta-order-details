# File location: scripts/resync_restaurant.py
"""
Utility script to resync orders for a specific restaurant from a specific date
"""
import argparse
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import logging

from src.config.settings import get_config
from src.services.order_tracker_v2 import OrderTrackerServiceV2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def resync_restaurant(restaurant_id: int, days_back: int = None, from_date: str = None):
    """
    Resync orders for a specific restaurant
    
    Args:
        restaurant_id: The restaurant ID to resync
        days_back: Number of days to go back (e.g., 30 for last month)
        from_date: Specific date to start from (format: YYYY-MM-DD)
    """
    try:
        # Load configuration
        config = get_config()
        
        # Create database connection
        engine = create_engine(config.database.connection_string)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Initialize tracker service
        tracker_service = OrderTrackerServiceV2(session)
        
        if days_back:
            # Calculate target date
            target_date = datetime.now() - timedelta(days=days_back)
            logger.info(f"Setting checkpoint to {days_back} days ago: {target_date}")
            tracker_service.set_checkpoint_to_date(restaurant_id, target_date)
            
        elif from_date:
            # Parse specific date
            target_date = datetime.strptime(from_date, "%Y-%m-%d")
            logger.info(f"Setting checkpoint to specific date: {target_date}")
            tracker_service.set_checkpoint_to_date(restaurant_id, target_date)
            
        else:
            # Full resync
            logger.info(f"Resetting checkpoint for full resync")
            tracker_service.reset_checkpoint(restaurant_id)
        
        logger.info(f"Checkpoint updated for restaurant {restaurant_id}")
        logger.info("You can now run the main sync process and it will pick up from the new checkpoint")
        
        # Show current checkpoint
        checkpoint = tracker_service.get_sync_checkpoint(restaurant_id, f"Restaurant {restaurant_id}")
        if checkpoint:
            logger.info(f"Current checkpoint: Order ID {checkpoint[0]}, Date {checkpoint[1]}")
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise
    finally:
        session.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Resync orders for a specific restaurant")
    parser.add_argument("restaurant_id", type=int, help="Restaurant ID to resync")
    
    # Mutually exclusive group for date options
    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument("--days-back", type=int, 
                           help="Number of days to go back (e.g., 30 for last month)")
    date_group.add_argument("--from-date", type=str,
                           help="Specific date to start from (format: YYYY-MM-DD)")
    date_group.add_argument("--full", action="store_true",
                           help="Full resync from beginning")
    
    args = parser.parse_args()
    
    if args.full:
        resync_restaurant(args.restaurant_id)
    else:
        resync_restaurant(args.restaurant_id, args.days_back, args.from_date)