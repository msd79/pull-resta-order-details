from datetime import datetime, timedelta
from sqlalchemy import func
from sqlalchemy.orm import Session
from src.database.dimentional_models import DimRestaurant
from src.database.models import Restaurant, Order
import logging

class RestaurantDimensionService:
    def __init__(self, session: Session):
        self.session = session
        self.logger = logging.getLogger(__name__)

    def update_restaurant_dimension(self, restaurant: Restaurant) -> int:
        try:
            self.logger.info(f"Starting restaurant dimension update for restaurant_id: {restaurant.id}")
            
            # Get current record if exists
            current_record = self.session.query(DimRestaurant)\
                .filter(
                    DimRestaurant.restaurant_id == restaurant.id,
                    DimRestaurant.is_current == True
                ).first()

            # Calculate new metrics
            metrics = self._calculate_restaurant_metrics(restaurant.id)
            
            self.logger.info(f"Calculated metrics: {metrics}")
            if current_record:
                self.logger.info(f"Found current record with metrics: avg_daily_orders={current_record.avg_daily_orders}, " 
                            f"avg_order_value={current_record.avg_order_value}, "
                            f"peak_hour_capacity={current_record.peak_hour_capacity}")

            # Add strict change detection
            if current_record:
                has_changes = (
                    current_record.restaurant_name != restaurant.name or
                    abs(current_record.avg_daily_orders - metrics['avg_daily_orders']) > 0.01 or
                    abs(current_record.avg_order_value - metrics['avg_order_value']) > 0.01 or
                    current_record.peak_hour_capacity != metrics['peak_hour_capacity']
                )
                
                if not has_changes:
                    self.logger.info(f"No significant changes detected, returning existing key: {current_record.restaurant_key}")
                    return current_record.restaurant_key
                else:
                    self.logger.info("Changes detected, creating new record")

            # Create new record only if we have no current record or actual changes
            new_record = DimRestaurant(
                restaurant_id=restaurant.id,
                restaurant_name=restaurant.name,
                company_id=restaurant.company_id if hasattr(restaurant, 'company_id') else None,
                company_name=restaurant.company_name if hasattr(restaurant, 'company_name') else None,
                effective_date=datetime.now(),
                expiration_date=None,
                is_current=True,
                avg_daily_orders=metrics['avg_daily_orders'],
                avg_order_value=metrics['avg_order_value'],
                peak_hour_capacity=metrics['peak_hour_capacity']
            )

            if current_record:
                current_record.expiration_date = datetime.now()
                current_record.is_current = False
                
            self.session.add(new_record)
            self.session.flush()
            
            self.logger.info(f"Created/updated restaurant dimension record with key: {new_record.restaurant_key}")
            return new_record.restaurant_key

        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error updating restaurant dimension: {str(e)}")
            raise

    def _calculate_restaurant_metrics(self, restaurant_id: int) -> dict:
        """Calculate performance metrics for a restaurant based on order history."""
        try:
            # Look back period for calculations (e.g., last 30 days)
            look_back_date = datetime.now() - timedelta(days=30)

            # Query orders for the time period
            orders = self.session.query(Order)\
                .filter(
                    Order.restaurant_id == restaurant_id,
                    Order.creation_date >= look_back_date
                ).all()

            if not orders:
                return {
                    'avg_daily_orders': 0.0,
                    'avg_order_value': 0.0,
                    'peak_hour_capacity': 0
                }

            # Calculate average daily orders
            days_with_orders = set(order.creation_date.date() for order in orders)
            total_days = len(days_with_orders) or 1  # Avoid division by zero
            avg_daily_orders = len(orders) / total_days

            # Calculate average order value
            total_order_value = sum(order.total for order in orders)
            avg_order_value = total_order_value / len(orders) if orders else 0.0

            # Calculate peak hour capacity
            peak_hour_capacity = self._calculate_peak_hour_capacity(orders)

            return {
                'avg_daily_orders': round(avg_daily_orders, 2),
                'avg_order_value': round(avg_order_value, 2),
                'peak_hour_capacity': peak_hour_capacity
            }

        except Exception as e:
            self.logger.error(f"Error calculating restaurant metrics: {str(e)}")
            raise

    def _calculate_peak_hour_capacity(self, orders: list) -> int:
        """Calculate peak hour capacity based on maximum orders in any hour."""
        try:
            # Group orders by hour and count
            hour_counts = {}
            for order in orders:
                hour = order.creation_date.replace(minute=0, second=0, microsecond=0)
                hour_counts[hour] = hour_counts.get(hour, 0) + 1

            # Get the maximum orders in any hour
            peak_capacity = max(hour_counts.values()) if hour_counts else 0

            return peak_capacity

        except Exception as e:
            self.logger.error(f"Error calculating peak hour capacity: {str(e)}")
            raise