from typing import Optional
from sqlalchemy.orm import Session
from src.database.dimentional_models import FactCustomerMetrics, FactOrders, FactPayments
from src.database.models import Order, Payment
import logging

class FactPopulationService:
    def __init__(self, session: Session):
        self.session = session
        self.logger = logging.getLogger(__name__)

    def populate_fact_orders(self, order: Order, datetime_key: int,
                        customer_key: int, restaurant_key: int,
                        promotion_key: Optional[int] = None) -> int:
        """
        Populate fact_orders table and return the generated order_key
        """
        try:
            # Check if fact already exists
            self.logger.debug(f"Checking if fact order exists for order ID {order.id}")
            existing = self.session.query(FactOrders)\
                .filter(FactOrders.order_id == order.id)\
                .first()
                
            if existing:
                self.logger.info(f"Skipping order {order.id}: Fact already exists")
                return existing.order_key

            self.logger.debug(f"Creating new fact order with datetime_key={datetime_key}, "
                             f"customer_key={customer_key}, restaurant_key={restaurant_key}")
            
            fact_order = FactOrders(
                order_id=order.id,
                datetime_key=datetime_key,
                customer_key=customer_key,
                restaurant_key=restaurant_key,
                promotion_key=promotion_key,
                order_status=order.status,
                delivery_type=order.delivery_type,
                order_method=order.order_method,
                sub_total=order.sub_total,
                delivery_fee=order.delivery_fee,
                service_charge=order.service_charge,
                total_discount=order.discount,
                total=order.total,
                used_points=order.used_points,
                is_promotion_applied=True if promotion_key is not None else False,
                review_rating=order.review_rating,  # Add this line
                review_message=order.review_message 

            )
            self.session.add(fact_order)
            self.logger.debug("Flushing session to generate order_key")
            self.session.flush()  # This will populate the order_key
            
            self.logger.info(f"Created fact order for order ID {order.id} with order_key={fact_order.order_key}")
            return fact_order.order_key

        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error populating fact_orders for order {order.id}: {str(e)}")
            raise

    def populate_fact_payments(self, payment: Payment, order_key: int,
                            datetime_key: int, payment_method_key: int, restaurant_key: int) -> None:
        """
        Populate fact_payments table using the order_key from fact_orders
        """
        try:
            # Check if fact already exists
            self.logger.debug(f"Checking if fact payment exists for payment ID {payment.id}")
            existing = self.session.query(FactPayments)\
                .filter(FactPayments.payment_id == payment.id)\
                .first()
                
            if existing:
                self.logger.info(f"Skipping payment {payment.id}: Fact already exists")
                return

            self.logger.debug(f"Creating new fact payment with order_key={order_key}, "
                             f"datetime_key={datetime_key}, payment_method_key={payment_method_key}")
            
            fact_payment = FactPayments(
                payment_id=payment.id,
                order_key=order_key,  # Using the order_key from fact_orders
                datetime_key=datetime_key,
                payment_method_key=payment_method_key,
                sub_total=payment.sub_total,
                extra_charge=payment.extra_charge,
                discount=payment.discount,
                tax=payment.tax,
                tip=payment.tip,
                total_amount=payment.amount,
                payment_status=payment.status,
                restaurant_key=restaurant_key
            )
            self.session.add(fact_payment)
            self.logger.debug("Committing session for fact payment")
            self.session.commit()
            self.logger.info(f"Created fact payment for payment ID {payment.id}")

        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error populating fact_payments for payment {payment.id}: {str(e)}")
            raise

    def populate_fact_customer_metrics(self, customer_key: int,
                                        datetime_key: int,
                                        daily_metrics: dict,
                                        order_id: int,
                                        restaurant_key: int) -> None:
        """
        Populate fact_customer_metrics table with order-specific metrics
        
        Args:
            customer_key: The surrogate key from dim_customer
            datetime_key: The surrogate key from dim_datetime
            daily_metrics: Dictionary containing the metrics to be recorded
            order_id: The business key of the order being processed
        """
        try:
            # Check if a record already exists for this order
            self.logger.debug(f"Checking if fact customer metrics exist for order ID {order_id}")
            existing_record = self.session.query(FactCustomerMetrics)\
                .filter(FactCustomerMetrics.order_id == order_id)\
                .first()
                
            if existing_record:
                self.logger.info(f"Updating customer metrics for order ID {order_id}")
                self.logger.debug(f"Current metrics before update: {self._get_metrics_dict(existing_record)}")
                
                # Update existing record
                for key, value in daily_metrics.items():
                    setattr(existing_record, key, value)
                    
                self.logger.debug(f"Updated {len(daily_metrics)} metrics for order ID {order_id}")
            else:
                self.logger.info(f"Creating new customer metrics for order ID {order_id}")
                self.logger.debug(f"Metrics to be created: {daily_metrics}")
                
                # Create new record
                fact_metrics = FactCustomerMetrics(
                    order_id=order_id,
                    customer_key=customer_key,
                    datetime_key=datetime_key,
                    daily_orders=daily_metrics.get('daily_orders', 0),
                    daily_spend=daily_metrics.get('daily_spend', 0.0),
                    points_used=daily_metrics.get('points_used', 0),
                    running_order_count=daily_metrics.get('running_order_count', 0),
                    running_total_spend=daily_metrics.get('running_total_spend', 0.0),
                    running_avg_order_value=daily_metrics.get('running_avg_order_value', 0.0),
                    days_since_last_order=daily_metrics.get('days_since_last_order', 0),
                    order_frequency_days=daily_metrics.get('order_frequency_days', 0.0),
                    restaurant_key=restaurant_key
                )
                self.session.add(fact_metrics)
            
            self.logger.debug("Committing session for fact customer metrics")
            self.session.commit()
            self.logger.info(f"Successfully 'updated' : created customer metrics for order ID {order_id}")

        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error in customer metrics for order ID {order_id}: {str(e)}")
            self.logger.debug(f"Failed metrics data: {daily_metrics}")
            raise
            
    def _get_metrics_dict(self, record: FactCustomerMetrics) -> dict:
        """Helper to get metrics as dictionary for logging"""
        return {
            'daily_orders': record.daily_orders,
            'daily_spend': record.daily_spend,
            'points_used': record.points_used,
            'running_order_count': record.running_order_count,
            'running_total_spend': record.running_total_spend,
            'running_avg_order_value': record.running_avg_order_value,
            'days_since_last_order': record.days_since_last_order,
            'order_frequency_days': record.order_frequency_days
        }