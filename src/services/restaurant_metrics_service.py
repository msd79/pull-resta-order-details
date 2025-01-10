from datetime import datetime, timedelta
from typing import Dict, List, Optional
from sqlalchemy import Date, cast, func
from sqlalchemy.orm import Session
from src.database.dimentional_models import (
    FactRestaurantMetrics, DimDateTime, DimRestaurant
)
from src.database.models import Order, Payment, ProcessedOrders
from src.services.order_processing_tracker import OrderProcessingTracker
import logging

class RestaurantMetricsService:
    def __init__(self, session: Session):
        self.session = session
        self.logger = logging.getLogger(__name__)
        self.order_tracker = OrderProcessingTracker(session)
        
        self.day_parts = {
            'before_peak': (6, 17),
            'peak': (18, 20),
            'after_peak': (21, 23),
        }

    async def update_daily_metrics(self, restaurant_id: int, date: datetime) -> None:
        try:
            start_date = date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = start_date + timedelta(days=1)
            
            # Get all orders for this date range
            orders = self.session.query(Order)\
                .filter(
                    Order.restaurant_id == restaurant_id,
                    Order.creation_date >= start_date,
                    Order.creation_date < end_date
                ).all()
            
            if not orders:
                return
                
            # Get unprocessed orders
            order_ids = [order.id for order in orders]
            unprocessed_ids = self.order_tracker.get_unprocessed_orders(
                order_ids, 
                OrderProcessingTracker.FACT_TYPES['RESTAURANT_METRICS']
            )
            
            if not unprocessed_ids:
                self.logger.info(f"No unprocessed orders for restaurant {restaurant_id} on {date.date()}")
                return
            
            # Calculate metrics using all orders for completeness
            metrics = await self._calculate_daily_metrics(restaurant_id, start_date, orders)
            
            # Update fact table
            await self._update_fact_table(restaurant_id, date, metrics)
            
            # Mark orders as processed
            self.order_tracker.mark_orders_processed(
                unprocessed_ids,
                OrderProcessingTracker.FACT_TYPES['RESTAURANT_METRICS']
            )
            
        except Exception as e:
            self.logger.error(f"Error updating daily metrics: {str(e)}")
            raise

    def _count_orders_in_timeframe(self, orders: List[Order], start_hour: int, end_hour: int) -> int:
        """Count orders within a specific timeframe."""
        return sum(1 for order in orders 
                  if start_hour <= order.creation_date.hour < end_hour)

    def _calculate_peak_hour(self, orders: List[Order]) -> Dict:
        """Calculate peak hour and number of orders during peak hour."""
        hour_counts = {}
        for order in orders:
            hour = order.creation_date.hour
            hour_counts[hour] = hour_counts.get(hour, 0) + 1
            
        if not hour_counts:
            return {'peak_hour_orders': 0, 'peak_hour': None}
            
        peak_hour = max(hour_counts.items(), key=lambda x: x[1])
        return {
            'peak_hour_orders': peak_hour[1],
            'peak_hour': peak_hour[0]
        }

    async def _calculate_payment_metrics(self, orders: List[Order]) -> Dict:
        """Calculate payment-related metrics for the given orders."""
        payment_counts = {
            'cash_payments': 0,
            'card_payments': 0,
            'reward_points': 0
        }
        
        for order in orders:
            payments = self.session.query(Payment)\
                .filter(Payment.order_id == order.id)\
                .all()
                
            for payment in payments:
                if payment.payment_method_type == 2:  # Cash
                    payment_counts['cash_payments'] += 1
                elif payment.payment_method_type == 4:  # Card
                    payment_counts['card_payments'] += 1
                elif payment.payment_method_type == 1 :  # Digital
                    payment_counts['reward_points'] += 1
                    
        return payment_counts

    def _get_empty_metrics(self) -> Dict:
        """Return a dictionary of metrics initialized to zero."""
        return {
            'total_orders': 0,
            'total_revenue': 0.0,
            'avg_order_value': 0.0,
            'before_peak_orders': 0,
            'peak_orders': 0,
            'after_peak_orders': 0,
            'delivery_orders': 0,
            'pickup_orders': 0,
            'cash_payments': 0,
            'card_payments': 0,
            'reward_points': 0,
            'orders_with_promotion': 0,
            'total_discount_amount': 0.0,
            'peak_hour_orders': 0,
            'peak_hour': None
        }

    def _get_datetime_key(self, date: datetime) -> Optional[int]:
        """Get datetime key for a given date."""
        try:
            # Convert input date to start of day
            target_date = date.replace(hour=0, minute=0, second=0, microsecond=0)
            
            # Query using CAST in SQL Server
            result = self.session.query(DimDateTime.datetime_key)\
                .filter(
                    cast(DimDateTime.datetime, Date) == target_date.date()
                ).first()
            
            return result[0] if result else None
            
        except Exception as e:
            self.logger.error(f"Error getting datetime key: {str(e)}")
            raise

    async def _calculate_daily_metrics(self, restaurant_id: int, date: datetime, orders: List[Order]) -> Dict:
        """Calculate all metrics for a given restaurant and date using provided orders."""
        try:
            if not orders:
                return self._get_empty_metrics()
                
            metrics = {
                'total_orders': len(orders),
                'total_revenue': sum(order.total for order in orders),
                'avg_order_value': sum(order.total for order in orders) / len(orders),
                
                'before_peak_orders': self._count_orders_in_timeframe(orders, *self.day_parts['before_peak']),
                'peak_orders': self._count_orders_in_timeframe(orders, *self.day_parts['peak']),
                'after_peak_orders': self._count_orders_in_timeframe(orders, *self.day_parts['after_peak']),
                
                'delivery_orders': sum(1 for order in orders if order.delivery_type == 1),
                'pickup_orders': sum(1 for order in orders if order.delivery_type == 2),
                
                'orders_with_promotion': sum(1 for order in orders if order.promotion_id is not None),
                'total_discount_amount': sum(order.discount for order in orders),
            }
            
            # Calculate peak hour metrics
            peak_hour_data = self._calculate_peak_hour(orders)
            metrics.update(peak_hour_data)
            
            # Calculate payment metrics
            payment_metrics = await self._calculate_payment_metrics(orders)
            metrics.update(payment_metrics)
            
            return metrics
            
        except Exception as e:
            self.logger.error(f"Error calculating daily metrics: {str(e)}")
            raise

    async def _update_fact_table(self, restaurant_id: int, date: datetime, metrics: dict) -> None:
        restaurant_dim = self.session.query(DimRestaurant)\
            .filter(
                DimRestaurant.restaurant_id == restaurant_id,
                DimRestaurant.is_current == True
            ).first()
        
        if not restaurant_dim:
            raise ValueError(f"No current restaurant dimension record found for ID {restaurant_id}")
            
        datetime_key = self._get_datetime_key(date)
        if not datetime_key:
            raise ValueError(f"No datetime key found for date {date}")
            
        fact_record = self.session.query(FactRestaurantMetrics)\
            .filter(
                FactRestaurantMetrics.restaurant_key == restaurant_dim.restaurant_key,
                FactRestaurantMetrics.datetime_key == datetime_key
            ).first()
            
        if fact_record:
            for key, value in metrics.items():
                setattr(fact_record, key, value)
        else:
            fact_record = FactRestaurantMetrics(
                restaurant_key=restaurant_dim.restaurant_key,
                datetime_key=datetime_key,
                **metrics
            )
            self.session.add(fact_record)
            
        self.session.commit()