# File location: src/services/promotion_summary_service.py
from datetime import datetime, timedelta, date as date_type
from typing import Dict, List, Optional, Tuple
from sqlalchemy import Date, cast, func, and_
from sqlalchemy.orm import Session
from src.database.dimentional_models import (
    FactDailyPromotionSummary, DimDateTime, DimRestaurant, DimPromotion, FactOrders, DimCustomer
)
from src.database.models import Order, ProcessedOrders
from src.services.order_processing_tracker import OrderProcessingTracker
import logging


class PromotionSummaryService:
    """
    Manages daily promotion summary aggregations.

    This service calculates and maintains pre-aggregated promotion metrics
    for fast Power BI queries. Each summary record represents a unique
    combination of (restaurant, date, promotion).

    promotion_key = NULL represents orders without any promotion applied.
    """

    FACT_TYPE = 'promotion_summary'

    def __init__(self, session: Session):
        self.session = session
        self.logger = logging.getLogger(__name__)
        self.order_tracker = OrderProcessingTracker(session)

    async def update_summary_for_order(self, order: Order, restaurant_key: int) -> None:
        """
        Update promotion summary after processing a single order.
        This is called from the ETL pipeline for each new order.

        Args:
            order: The order that was just processed
            restaurant_key: The dimensional restaurant key
        """
        try:
            order_date = order.creation_date.date()
            promotion_key = await self._get_promotion_key(order.promotion_id) if order.promotion_id else None

            await self._update_or_create_summary(
                restaurant_key=restaurant_key,
                summary_date=order_date,
                promotion_key=promotion_key
            )

            # Mark order as processed for this fact type
            self.order_tracker.mark_orders_processed(
                [order.id],
                self.FACT_TYPE
            )

        except Exception as e:
            self.logger.error(f"Error updating promotion summary for order {order.id}: {str(e)}")
            raise

    async def update_daily_summary(self, restaurant_key: int, summary_date: date_type) -> None:
        """
        Recalculate all promotion summaries for a specific restaurant and date.
        Useful for corrections or manual recalculations.

        Args:
            restaurant_key: The dimensional restaurant key
            summary_date: The date to recalculate
        """
        try:
            # Get restaurant_id from dimension
            restaurant_dim = self.session.query(DimRestaurant).filter(
                DimRestaurant.restaurant_key == restaurant_key
            ).first()

            if not restaurant_dim:
                self.logger.warning(f"Restaurant key {restaurant_key} not found")
                return

            restaurant_id = restaurant_dim.restaurant_id

            # Get all orders for this date
            start_datetime = datetime.combine(summary_date, datetime.min.time())
            end_datetime = start_datetime + timedelta(days=1)

            orders = self.session.query(Order).filter(
                Order.restaurant_id == restaurant_id,
                Order.creation_date >= start_datetime,
                Order.creation_date < end_datetime
            ).all()

            # Group orders by promotion
            promotion_groups = self._group_orders_by_promotion(orders)

            # Update summary for each promotion group
            for promotion_id, group_orders in promotion_groups.items():
                promotion_key = await self._get_promotion_key(promotion_id) if promotion_id else None
                await self._update_or_create_summary(
                    restaurant_key=restaurant_key,
                    summary_date=summary_date,
                    promotion_key=promotion_key
                )

            self.logger.info(f"Updated promotion summaries for restaurant {restaurant_id} on {summary_date}")

        except Exception as e:
            self.logger.error(f"Error updating daily summary: {str(e)}")
            raise

    async def _update_or_create_summary(
        self,
        restaurant_key: int,
        summary_date: date_type,
        promotion_key: Optional[int]
    ) -> None:
        """
        Update or create a summary record for a specific restaurant/date/promotion combination.
        Recalculates metrics from source data.
        """
        try:
            # Get restaurant_id
            restaurant_dim = self.session.query(DimRestaurant).filter(
                DimRestaurant.restaurant_key == restaurant_key
            ).first()

            if not restaurant_dim:
                return

            restaurant_id = restaurant_dim.restaurant_id

            # Get all orders for this combination
            start_datetime = datetime.combine(summary_date, datetime.min.time())
            end_datetime = start_datetime + timedelta(days=1)

            orders_query = self.session.query(Order).filter(
                Order.restaurant_id == restaurant_id,
                Order.creation_date >= start_datetime,
                Order.creation_date < end_datetime
            )

            # Filter by promotion
            if promotion_key is not None:
                # Get promotion_id from dimension
                promotion_dim = self.session.query(DimPromotion).filter(
                    DimPromotion.promotion_key == promotion_key
                ).first()
                if promotion_dim:
                    orders_query = orders_query.filter(Order.promotion_id == promotion_dim.promotion_id)
                else:
                    return
            else:
                # Orders without promotion
                orders_query = orders_query.filter(Order.promotion_id.is_(None))

            orders = orders_query.all()

            if not orders:
                # Remove summary if no orders exist for this combination
                self._delete_summary(restaurant_key, summary_date, promotion_key)
                return

            # Calculate metrics
            metrics = await self._calculate_metrics(orders, restaurant_id)

            # Find or create summary record
            # Handle NULL promotion_key in query
            if promotion_key is None:
                existing = self.session.query(FactDailyPromotionSummary).filter(
                    FactDailyPromotionSummary.restaurant_key == restaurant_key,
                    FactDailyPromotionSummary.date == summary_date,
                    FactDailyPromotionSummary.promotion_key.is_(None)
                ).first()
            else:
                existing = self.session.query(FactDailyPromotionSummary).filter(
                    FactDailyPromotionSummary.restaurant_key == restaurant_key,
                    FactDailyPromotionSummary.date == summary_date,
                    FactDailyPromotionSummary.promotion_key == promotion_key
                ).first()

            if existing:
                # Update existing record
                for key, value in metrics.items():
                    setattr(existing, key, value)
            else:
                # Create new record
                summary = FactDailyPromotionSummary(
                    restaurant_key=restaurant_key,
                    date=summary_date,
                    promotion_key=promotion_key,
                    **metrics
                )
                self.session.add(summary)

            self.session.commit()

        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error updating/creating summary: {str(e)}")
            raise

    async def _calculate_metrics(self, orders: List[Order], restaurant_id: int) -> Dict:
        """
        Calculate all metrics for a group of orders.

        Args:
            orders: List of orders to aggregate
            restaurant_id: The restaurant ID for customer lookups

        Returns:
            Dictionary of calculated metrics
        """
        if not orders:
            return self._get_empty_metrics()

        total_revenue = sum(order.total or 0 for order in orders)
        total_sub_total = sum(order.sub_total or 0 for order in orders)
        total_discount = sum((order.discount or 0) + (order.line_item_discount or 0) for order in orders)
        order_values = [order.total for order in orders if order.total]

        # Customer analysis
        customer_ids = list(set(order.customer_id for order in orders if order.customer_id))
        new_customers, repeat_customers = await self._analyze_customers(
            customer_ids,
            restaurant_id,
            min(order.creation_date for order in orders)
        )

        metrics = {
            'order_count': len(orders),
            'total_revenue': total_revenue,
            'avg_order_value': total_revenue / len(orders) if orders else 0,
            'total_discount_given': total_discount,
            'total_sub_total': total_sub_total,
            'unique_customers': len(customer_ids),
            'new_customers': new_customers,
            'repeat_customers': repeat_customers,
            'delivery_orders': sum(1 for order in orders if order.delivery_type == 1),
            'pickup_orders': sum(1 for order in orders if order.delivery_type == 2),
            'min_order_value': min(order_values) if order_values else None,
            'max_order_value': max(order_values) if order_values else None,
        }

        return metrics

    async def _analyze_customers(
        self,
        customer_ids: List[int],
        restaurant_id: int,
        before_date: datetime
    ) -> Tuple[int, int]:
        """
        Analyze customers to determine new vs repeat customers.

        A new customer is one who has no orders before the given date.
        A repeat customer has at least one prior order.

        Returns:
            Tuple of (new_customers_count, repeat_customers_count)
        """
        if not customer_ids:
            return 0, 0

        new_count = 0
        repeat_count = 0

        for customer_id in customer_ids:
            # Check if customer had prior orders
            prior_order = self.session.query(Order).filter(
                Order.customer_id == customer_id,
                Order.restaurant_id == restaurant_id,
                Order.creation_date < before_date
            ).first()

            if prior_order:
                repeat_count += 1
            else:
                new_count += 1

        return new_count, repeat_count

    def _group_orders_by_promotion(self, orders: List[Order]) -> Dict[Optional[int], List[Order]]:
        """
        Group orders by their promotion_id.
        None key represents orders without promotion.
        """
        groups: Dict[Optional[int], List[Order]] = {}

        for order in orders:
            promo_id = order.promotion_id
            if promo_id not in groups:
                groups[promo_id] = []
            groups[promo_id].append(order)

        return groups

    async def _get_promotion_key(self, promotion_id: int) -> Optional[int]:
        """Get the dimensional promotion_key for a promotion_id."""
        if not promotion_id:
            return None

        promotion = self.session.query(DimPromotion).filter(
            DimPromotion.promotion_id == promotion_id
        ).first()

        return promotion.promotion_key if promotion else None

    def _delete_summary(
        self,
        restaurant_key: int,
        summary_date: date_type,
        promotion_key: Optional[int]
    ) -> None:
        """Delete a summary record if it exists."""
        try:
            if promotion_key is None:
                self.session.query(FactDailyPromotionSummary).filter(
                    FactDailyPromotionSummary.restaurant_key == restaurant_key,
                    FactDailyPromotionSummary.date == summary_date,
                    FactDailyPromotionSummary.promotion_key.is_(None)
                ).delete()
            else:
                self.session.query(FactDailyPromotionSummary).filter(
                    FactDailyPromotionSummary.restaurant_key == restaurant_key,
                    FactDailyPromotionSummary.date == summary_date,
                    FactDailyPromotionSummary.promotion_key == promotion_key
                ).delete()
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error deleting summary: {str(e)}")

    def _get_empty_metrics(self) -> Dict:
        """Return a dictionary of metrics initialized to zero/None."""
        return {
            'order_count': 0,
            'total_revenue': 0.0,
            'avg_order_value': 0.0,
            'total_discount_given': 0.0,
            'total_sub_total': 0.0,
            'unique_customers': 0,
            'new_customers': 0,
            'repeat_customers': 0,
            'delivery_orders': 0,
            'pickup_orders': 0,
            'min_order_value': None,
            'max_order_value': None,
        }
