# File location: scripts/migrate_to_order_tracking.py
"""
Migration script to transition from page-based tracking to order-based tracking.
This script will:
1. Create the new order_sync_tracker table
2. Migrate data from the old page_index_tracker to establish initial checkpoints
3. Optionally drop the old table
"""

import logging
from datetime import datetime
from sqlalchemy import create_engine, text, func
from sqlalchemy.orm import sessionmaker
from src.config.settings import get_config
from src.database.models import Order, Restaurant
from src.services.order_tracker_v2 import OrderSyncTracker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def migrate_tracking_system(drop_old_table: bool = False):
    """
    Migrate from page-based tracking to order-based tracking.
    
    Args:
        drop_old_table: Whether to drop the old page_index_tracker table after migration
    """
    try:
        # Load configuration
        config = get_config()
        
        # Create database connection
        engine = create_engine(config.database.connection_string)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        logger.info("Starting migration from page-based to order-based tracking...")
        
        # Step 1: Create new table
        logger.info("Creating new order_sync_tracker table...")
        OrderSyncTracker.__table__.create(engine, checkfirst=True)
        
        # Step 2: Check if old page_index_tracker exists
        result = session.execute(text("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = 'page_index_tracker'
        """)).scalar()
        
        if result == 0:
            logger.warning("Old page_index_tracker table not found. Creating empty tracking records...")
            # Create empty tracking records for all restaurants
            restaurants = session.query(Restaurant).all()
            for restaurant in restaurants:
                create_initial_checkpoint(session, restaurant.id, restaurant.name)
            session.commit()
            logger.info("Migration complete - created empty tracking records")
            return
        
        # Step 3: Migrate existing tracking data
        logger.info("Migrating data from page_index_tracker...")
        
        # Get all records from old tracker
        old_trackers = session.execute(text("""
            SELECT restaurant_id, restaurant_name, last_page_index, last_updated
            FROM page_index_tracker
        """)).fetchall()
        
        for tracker in old_trackers:
            restaurant_id = tracker[0]
            restaurant_name = tracker[1]
            
            logger.info(f"Migrating restaurant {restaurant_name} (ID: {restaurant_id})...")
            
            # Find the most recent order for this restaurant
            # This represents the last order that was likely processed
            most_recent_order = session.query(
                Order.id,
                Order.creation_date
            ).filter(
                Order.restaurant_id == restaurant_id
            ).order_by(
                Order.creation_date.desc()
            ).first()
            
            if most_recent_order:
                # Create checkpoint based on most recent order
                new_tracker = OrderSyncTracker(
                    restaurant_id=restaurant_id,
                    restaurant_name=restaurant_name,
                    last_order_id=most_recent_order[0],
                    last_order_date=most_recent_order[1],
                    last_sync_date=datetime.now(),
                    total_orders_synced=session.query(func.count(Order.id))
                        .filter(Order.restaurant_id == restaurant_id).scalar()
                )
            else:
                # No orders found - create empty checkpoint
                new_tracker = OrderSyncTracker(
                    restaurant_id=restaurant_id,
                    restaurant_name=restaurant_name,
                    last_order_id=0,
                    last_order_date=datetime(1900, 1, 1),  # SQL Server safe minimum date
                    last_sync_date=datetime.now(),
                    total_orders_synced=0
                )
            
            session.merge(new_tracker)
            logger.info(f"Created checkpoint for {restaurant_name}: "
                       f"Last Order ID: {new_tracker.last_order_id}")
        
        session.commit()
        
        # Step 4: Optionally drop old table
        if drop_old_table:
            logger.info("Dropping old page_index_tracker table...")
            session.execute(text("DROP TABLE page_index_tracker"))
            session.commit()
            logger.info("Old table dropped successfully")
        else:
            logger.info("Old page_index_tracker table retained for backup")
        
        # Step 5: Verify migration
        new_tracker_count = session.query(func.count(OrderSyncTracker.restaurant_id)).scalar()
        logger.info(f"Migration complete! Created {new_tracker_count} tracking records")
        
        # Show summary
        logger.info("\nMigration Summary:")
        trackers = session.query(OrderSyncTracker).all()
        for tracker in trackers:
            logger.info(f"- {tracker.restaurant_name}: Last Order ID {tracker.last_order_id}, "
                       f"Total Synced: {tracker.total_orders_synced}")
        
    except Exception as e:
        logger.error(f"Migration failed: {str(e)}")
        raise
    finally:
        session.close()

def create_initial_checkpoint(session, restaurant_id: int, restaurant_name: str):
    """Create an initial checkpoint for a restaurant with no tracking history."""
    new_tracker = OrderSyncTracker(
        restaurant_id=restaurant_id,
        restaurant_name=restaurant_name,
        last_order_id=0,
        last_order_date=datetime(1900, 1, 1),  # SQL Server safe minimum date
        last_sync_date=datetime.now(),
        total_orders_synced=0
    )
    session.merge(new_tracker)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Migrate from page-based to order-based tracking")
    parser.add_argument("--drop-old-table", action="store_true", 
                       help="Drop the old page_index_tracker table after migration")
    parser.add_argument("--force-resync", type=int, 
                       help="Force a specific restaurant ID to resync from beginning")
    
    args = parser.parse_args()
    
    if args.force_resync:
        # Just reset checkpoint for specific restaurant
        config = get_config()
        engine = create_engine(config.database.connection_string)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        from src.services.order_tracker_v2 import OrderTrackerServiceV2
        tracker_service = OrderTrackerServiceV2(session)
        tracker_service.reset_checkpoint(args.force_resync)
        
        logger.info(f"Reset checkpoint for restaurant ID {args.force_resync}")
    else:
        # Run full migration
        migrate_tracking_system(drop_old_table=args.drop_old_table)