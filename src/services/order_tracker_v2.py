# File location: src/services/order_tracker_v2.py
from datetime import datetime
from typing import Optional, Tuple
import logging
from sqlalchemy.orm import Session
from sqlalchemy.exc import NoResultFound
from src.database.models import OrderSyncTracker

class OrderTrackerServiceV2:
    """
    New tracking service that tracks by order ID/date instead of page index.
    This is more robust against API pagination changes.
    """
    def __init__(self, session: Session):
        self.session = session
        self.logger = logging.getLogger(__name__)

    def get_sync_checkpoint(self, restaurant_id: int, restaurant_name: str) -> Optional[Tuple[int, datetime]]:
        """
        Get the last synchronized order ID and date for a restaurant.
        Returns None if no checkpoint exists (first sync).
        """
        try:
            tracker = self.session.query(OrderSyncTracker).filter_by(
                restaurant_id=restaurant_id
            ).one()
            
            self.logger.info(f"Found sync checkpoint for {restaurant_name}: "
                           f"Order ID {tracker.last_order_id}, Date {tracker.last_order_date}")
            return (tracker.last_order_id, tracker.last_order_date)
            
        except NoResultFound:
            self.logger.info(f"No sync checkpoint found for {restaurant_name}. This is the first sync.")
            # Create new tracker entry
            new_tracker = OrderSyncTracker(
                restaurant_id=restaurant_id,
                restaurant_name=restaurant_name,
                last_order_id=0,
                last_order_date=datetime.min,
                last_sync_date=datetime.now(),
                total_orders_synced=0
            )
            self.session.add(new_tracker)
            self.session.commit()
            return None

    def update_sync_checkpoint(self, restaurant_id: int, 
                             last_order_id: int, 
                             last_order_date: datetime,
                             orders_synced_count: int) -> None:
        """Update the sync checkpoint with the most recent order processed"""
        try:
            tracker = self.session.query(OrderSyncTracker).filter_by(
                restaurant_id=restaurant_id
            ).one()
            
            # Only update if this order is newer than our current checkpoint
            if last_order_date > tracker.last_order_date or \
               (last_order_date == tracker.last_order_date and last_order_id > tracker.last_order_id):
                
                self.logger.info(f"Updating sync checkpoint: Order ID {last_order_id}, Date {last_order_date}")
                tracker.last_order_id = last_order_id
                tracker.last_order_date = last_order_date
                tracker.last_sync_date = datetime.now()
                tracker.total_orders_synced += orders_synced_count
                self.session.commit()
            
        except NoResultFound:
            self.logger.error(f"No tracker found for restaurant_id: {restaurant_id}")
            raise

    def should_process_order(self, order_id: int, order_date: datetime, 
                           checkpoint: Optional[Tuple[int, datetime]]) -> bool:
        """
        Determine if an order should be processed based on the checkpoint.
        
        Args:
            order_id: The order ID to check
            order_date: The order creation date
            checkpoint: Tuple of (last_order_id, last_order_date) or None
            
        Returns:
            True if the order should be processed, False if it should be skipped
        """
        if checkpoint is None:
            # First sync - process all orders
            return True
            
        last_order_id, last_order_date = checkpoint
        
        # Process if order is newer than checkpoint
        if order_date > last_order_date:
            return True
        
        # If same date, process if order ID is higher (assuming IDs increment)
        if order_date == last_order_date and order_id > last_order_id:
            return True
            
        return False

    def reset_checkpoint(self, restaurant_id: int) -> None:
        """Reset the sync checkpoint for a restaurant (useful for full re-sync)"""
        try:
            tracker = self.session.query(OrderSyncTracker).filter_by(
                restaurant_id=restaurant_id
            ).one()
            
            tracker.last_order_id = 0
            tracker.last_order_date = datetime.min
            tracker.last_sync_date = datetime.now()
            self.session.commit()
            
            self.logger.info(f"Reset sync checkpoint for restaurant_id: {restaurant_id}")
            
        except NoResultFound:
            self.logger.warning(f"No tracker found to reset for restaurant_id: {restaurant_id}")