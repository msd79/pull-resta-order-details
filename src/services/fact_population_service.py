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
            existing = self.session.query(FactOrders)\
                .filter(FactOrders.order_id == order.id)\
                .first()
                
            if existing:
                self.logger.info(f"Fact order already exists for order {order.id}")
                return existing.order_key

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
                used_points=order.used_points
            )
            self.session.add(fact_order)
            self.session.flush()  # This will populate the order_key
            
            self.logger.info(f"Successfully populated fact_orders for order {order.id}")
            return fact_order.order_key

        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error populating fact_orders: {str(e)}")
            raise

    def populate_fact_payments(self, payment: Payment, order_key: int,
                            datetime_key: int, payment_method_key: int) -> None:
        """
        Populate fact_payments table using the order_key from fact_orders
        """
        try:
            # Check if fact already exists
            existing = self.session.query(FactPayments)\
                .filter(FactPayments.payment_id == payment.id)\
                .first()
                
            if existing:
                self.logger.info(f"Fact payment already exists for payment {payment.id}")
                return

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
                payment_status=payment.status
            )
            self.session.add(fact_payment)
            self.session.commit()
            self.logger.info(f"Successfully populated fact_payments for payment {payment.id}")

        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error populating fact_payments: {str(e)}")
            raise

    def populate_fact_customer_metrics(self, customer_key: int,
                                     datetime_key: int,
                                     daily_metrics: dict) -> None:
        try:
            fact_metrics = FactCustomerMetrics(
                customer_key=customer_key,
                datetime_key=datetime_key,
                daily_orders=daily_metrics.get('daily_orders', 0),
                daily_spend=daily_metrics.get('daily_spend', 0.0),
                daily_items=daily_metrics.get('daily_items', 0),
                points_earned=daily_metrics.get('points_earned', 0),
                points_used=daily_metrics.get('points_used', 0),
                running_order_count=daily_metrics.get('running_order_count', 0),
                running_total_spend=daily_metrics.get('running_total_spend', 0.0),
                running_avg_order_value=daily_metrics.get('running_avg_order_value', 0.0),
                days_since_last_order=daily_metrics.get('days_since_last_order', 0),
                order_frequency_days=daily_metrics.get('order_frequency_days', 0.0)
            )
            self.session.add(fact_metrics)
            self.session.commit()

        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error populating fact_customer_metrics: {str(e)}")
            raise