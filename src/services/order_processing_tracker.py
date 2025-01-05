# In src/services/order_processing_tracker.py
from datetime import datetime
from typing import List, Optional
from sqlalchemy.orm import Session
from src.database.models import ProcessedOrders  # Updated to use ProcessedOrders
import logging

class OrderProcessingTracker:
    """Central service for tracking order processing across all fact tables"""
    
    FACT_TYPES = {
        'RESTAURANT_METRICS': 'restaurant_metrics',
        'CUSTOMER_METRICS': 'customer_metrics',
        'ORDERS': 'orders',
        'PAYMENTS': 'payments'
    }

    def __init__(self, session: Session):
        self.session = session
        self.logger = logging.getLogger(__name__)

    def is_order_processed(self, order_id: int, fact_type: str) -> bool:
        """Check if an order has been processed for a specific fact type"""
        try:
            exists = self.session.query(ProcessedOrders)\
                .filter(
                    ProcessedOrders.order_id == order_id,
                    ProcessedOrders.fact_type == fact_type
                ).first()
            return bool(exists)
        except Exception as e:
            self.logger.error(f"Error checking processed status: {str(e)}")
            raise

    def get_unprocessed_orders(self, order_ids: List[int], fact_type: str) -> List[int]:
        """Get list of orders that haven't been processed for a specific fact type"""
        try:
            processed_ids = self.session.query(ProcessedOrders.order_id)\
                .filter(
                    ProcessedOrders.order_id.in_(order_ids),
                    ProcessedOrders.fact_type == fact_type
                ).all()
            
            processed_ids_set = {id[0] for id in processed_ids}
            return [id for id in order_ids if id not in processed_ids_set]
        except Exception as e:
            self.logger.error(f"Error getting unprocessed orders: {str(e)}")
            raise

    def mark_orders_processed(self, order_ids: List[int], fact_type: str) -> None:
        """Mark multiple orders as processed for a specific fact type"""
        try:
            for order_id in order_ids:
                if not self.is_order_processed(order_id, fact_type):
                    tracking_record = ProcessedOrders(
                        order_id=order_id,
                        fact_type=fact_type,
                        processed_date=datetime.now()
                    )
                    self.session.add(tracking_record)
            
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error marking orders as processed: {str(e)}")
            raise

    def reset_processing_status(self, order_ids: List[int], fact_type: Optional[str] = None) -> None:
        """Reset processing status for specified orders and fact type"""
        try:
            query = self.session.query(ProcessedOrders)\
                .filter(ProcessedOrders.order_id.in_(order_ids))
            
            if fact_type:
                query = query.filter(ProcessedOrders.fact_type == fact_type)
                
            query.delete(synchronize_session=False)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error resetting processing status: {str(e)}")
            raise