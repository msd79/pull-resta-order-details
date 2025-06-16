# File location: scripts/update_tracker_to_january_2025.py
"""
Update script to set the last_order_id in order_sync_tracker table 
to the first order of January 1st, 2025 for each restaurant.
"""

import logging
from datetime import datetime
from sqlalchemy import create_engine, and_, func
from sqlalchemy.orm import sessionmaker
from src.config.settings import get_config
from src.database.models import Order
from src.services.order_tracker_v2 import OrderSyncTracker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def update_to_january_first_orders():
    """
    Update all order_sync_tracker records to set the checkpoint 
    to the first order of January 1st, 2025 for each restaurant.
    """
    try:
        # Load configuration
        config = get_config()
        
        # Create database connection
        engine = create_engine(config.database.connection_string)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        logger.info("Starting update to set checkpoints to January 1st, 2025...")
        
        # Define the date range for January 1st, 2025
        jan_1_start = datetime(2025, 1, 1, 0, 0, 0)
        jan_1_end = datetime(2025, 1, 1, 23, 59, 59)
        
        # Get all existing tracker records
        trackers = session.query(OrderSyncTracker).all()
        
        updated_count = 0
        no_orders_count = 0
        
        for tracker in trackers:
            logger.info(f"Processing restaurant {tracker.restaurant_name} (ID: {tracker.restaurant_id})...")
            
            # Find the first order of January 1st, 2025 for this restaurant
            first_jan_order = session.query(
                Order.id,
                Order.creation_date
            ).filter(
                and_(
                    Order.restaurant_id == tracker.restaurant_id,
                    Order.creation_date >= jan_1_start,
                    Order.creation_date <= jan_1_end
                )
            ).order_by(
                Order.creation_date.asc()
            ).first()
            
            if first_jan_order:
                # Update the tracker with the first January order
                old_order_id = tracker.last_order_id
                tracker.last_order_id = first_jan_order[0]
                tracker.last_order_date = first_jan_order[1]
                tracker.last_sync_date = datetime.now()
                
                # Update total orders synced to reflect orders up to this point
                tracker.total_orders_synced = session.query(func.count(Order.id)).filter(
                    and_(
                        Order.restaurant_id == tracker.restaurant_id,
                        Order.id <= first_jan_order[0]
                    )
                ).scalar()
                
                updated_count += 1
                logger.info(f"Updated {tracker.restaurant_name}: "
                           f"Order ID {old_order_id} -> {first_jan_order[0]}, "
                           f"Date: {first_jan_order[1]}")
            else:
                # No orders found for January 1st - check if there are any orders after that
                logger.warning(f"No orders found for {tracker.restaurant_name} on January 1st, 2025.")
                
                earliest_order_after = session.query(
                    Order.id,
                    Order.creation_date
                ).filter(
                    and_(
                        Order.restaurant_id == tracker.restaurant_id,
                        Order.creation_date > jan_1_end
                    )
                ).order_by(
                    Order.creation_date.asc()
                ).first()
                
                if earliest_order_after:
                    # Use the earliest order after January 1st
                    old_order_id = tracker.last_order_id
                    tracker.last_order_id = earliest_order_after[0]
                    tracker.last_order_date = earliest_order_after[1]
                    tracker.last_sync_date = datetime.now()
                    
                    tracker.total_orders_synced = session.query(func.count(Order.id)).filter(
                        and_(
                            Order.restaurant_id == tracker.restaurant_id,
                            Order.id <= earliest_order_after[0]
                        )
                    ).scalar()
                    
                    updated_count += 1
                    logger.info(f"Updated {tracker.restaurant_name} with earliest order after Jan 1st: "
                               f"Order ID {old_order_id} -> {earliest_order_after[0]}, "
                               f"Date: {earliest_order_after[1]}")
                else:
                    no_orders_count += 1
                    logger.warning(f"No orders found for {tracker.restaurant_name} on or after January 1st, 2025. "
                                  "Keeping existing values.")
        
        # Commit all updates
        session.commit()
        
        # Summary
        logger.info(f"\nUpdate complete!")
        logger.info(f"- Total restaurants processed: {len(trackers)}")
        logger.info(f"- Successfully updated: {updated_count}")
        logger.info(f"- No January orders found: {no_orders_count}")
        
        # Show final state
        logger.info("\nFinal checkpoint summary:")
        trackers = session.query(OrderSyncTracker).order_by(OrderSyncTracker.restaurant_name).all()
        for tracker in trackers:
            logger.info(f"- {tracker.restaurant_name}: Order ID {tracker.last_order_id}, "
                       f"Date: {tracker.last_order_date.strftime('%Y-%m-%d %H:%M:%S')}, "
                       f"Total Synced: {tracker.total_orders_synced}")
        
    except Exception as e:
        logger.error(f"Update failed: {str(e)}")
        raise
    finally:
        session.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Update order tracker to January 2025 starting point")
    parser.add_argument("--dry-run", action="store_true", 
                       help="Show what would be updated without making changes")
    parser.add_argument("--restaurant-id", type=int, 
                       help="Update only a specific restaurant ID")
    
    args = parser.parse_args()
    
    if args.dry_run:
        logger.info("DRY RUN MODE - No changes will be made")
        # You could implement dry run logic here
    
    update_to_january_first_orders()