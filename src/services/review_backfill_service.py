# File location: src/services/review_backfill_service.py
"""
Review Backfill Service.

Orders are synced minutes after they are placed - hours before the platform
even sends the customer its review request. Reviews that arrive later were
never captured, because the sync fetches each order exactly once.

Once a day this service re-fetches recent orders that have no review yet.
When a review has appeared it is written to the orders and fact_orders
tables, and the review metrics in fact_restaurant_metrics are recalculated
for the affected days.
"""
import asyncio
import logging
from datetime import date as date_type
from datetime import datetime, timedelta
from typing import Dict, List, Set

from sqlalchemy import func
from sqlalchemy.orm import Session

from src.database.models import Order
from src.database.dimentional_models import (
    DimDateTime,
    DimRestaurant,
    FactOrders,
    FactRestaurantMetrics,
)


class ReviewBackfillService:
    """Re-checks recent unreviewed orders for late-arriving reviews."""

    def __init__(self, session: Session, api_client, config):
        self.session = session
        self.api_client = api_client
        self.config = config
        self.logger = logging.getLogger(__name__)

    def get_candidate_order_ids(self, restaurant_id: int) -> List[int]:
        """Orders from the lookback window that still have no review."""
        cutoff = datetime.now() - timedelta(days=self.config.review_backfill.lookback_days)
        rows = (
            self.session.query(Order.id)
            .filter(
                Order.restaurant_id == restaurant_id,
                Order.review_rating.is_(None),
                Order.creation_date >= cutoff,
            )
            .order_by(Order.creation_date)
            .all()
        )
        return [row[0] for row in rows]

    async def run_for_restaurant(self, restaurant_id: int, restaurant_name: str) -> Dict:
        """Re-check unreviewed recent orders for one restaurant.

        Must be called with the api_client already logged in for this
        restaurant (the same session used by the regular sync).
        """
        stats = {'orders_checked': 0, 'reviews_found': 0, 'errors': 0}
        candidates = self.get_candidate_order_ids(restaurant_id)
        if not candidates:
            self.logger.info(f"Review backfill for {restaurant_name}: no orders to check")
            return stats

        self.logger.info(
            f"Review backfill for {restaurant_name}: checking {len(candidates)} "
            f"orders from the last {self.config.review_backfill.lookback_days} days"
        )

        affected_dates: Set[date_type] = set()
        for order_id in candidates:
            try:
                details = await self.api_client.fetch_order_details(order_id)
                data = (details or {}).get('Data')
                stats['orders_checked'] += 1
                if data and data.get('ReviewRating') is not None:
                    self._apply_review(
                        order_id,
                        data.get('ReviewRating'),
                        data.get('ReviewMessage'),
                        affected_dates,
                    )
                    stats['reviews_found'] += 1
            except Exception as e:
                stats['errors'] += 1
                self.logger.error(f"Review backfill failed for order {order_id}: {str(e)}")
                self.session.rollback()
            await asyncio.sleep(self.config.sync.delay_between_orders)

        for affected_date in sorted(affected_dates):
            try:
                self._refresh_review_metrics(restaurant_id, affected_date)
            except Exception as e:
                stats['errors'] += 1
                self.logger.error(
                    f"Failed to refresh review metrics for {restaurant_name} "
                    f"on {affected_date}: {str(e)}"
                )
                self.session.rollback()

        self.logger.info(
            f"Review backfill for {restaurant_name} complete: "
            f"checked {stats['orders_checked']} orders, "
            f"found {stats['reviews_found']} new reviews, "
            f"errors: {stats['errors']}"
        )
        return stats

    def _apply_review(self, order_id: int, rating, message,
                      affected_dates: Set[date_type]) -> None:
        """Write a newly found review to orders and fact_orders."""
        order = self.session.query(Order).filter_by(id=order_id).one_or_none()
        if order is None:
            return

        order.review_rating = rating
        order.review_message = message
        self.session.query(FactOrders).filter_by(order_id=order_id).update(
            {
                FactOrders.review_rating: rating,
                # fact_orders.review_message is capped at 1000 characters
                FactOrders.review_message: message[:1000] if message else message,
            },
            synchronize_session=False,
        )
        self.session.commit()

        if order.creation_date:
            affected_dates.add(order.creation_date.date())
        self.logger.debug(f"Captured review for order {order_id}: rating={rating}")

    def _refresh_review_metrics(self, restaurant_id: int, affected_date: date_type) -> None:
        """Recalculate the review columns of fact_restaurant_metrics for one day."""
        day_start = datetime.combine(affected_date, datetime.min.time())
        day_end = day_start + timedelta(days=1)

        reviews_count, daily_avg = self.session.query(
            func.count(Order.review_rating),
            func.avg(Order.review_rating),
        ).filter(
            Order.restaurant_id == restaurant_id,
            Order.review_rating.isnot(None),
            Order.creation_date >= day_start,
            Order.creation_date < day_end,
        ).one()

        cumulative_avg = self.session.query(func.avg(Order.review_rating)).filter(
            Order.restaurant_id == restaurant_id,
            Order.review_rating.isnot(None),
            Order.creation_date < day_end,
        ).scalar()

        restaurant_keys = self.session.query(DimRestaurant.restaurant_key).filter(
            DimRestaurant.restaurant_id == restaurant_id
        )
        datetime_keys = self.session.query(DimDateTime.datetime_key).filter(
            DimDateTime.date == affected_date
        )

        updated = self.session.query(FactRestaurantMetrics).filter(
            FactRestaurantMetrics.restaurant_key.in_(restaurant_keys),
            FactRestaurantMetrics.datetime_key.in_(datetime_keys),
        ).update(
            {
                FactRestaurantMetrics.daily_reviews_count: reviews_count or 0,
                FactRestaurantMetrics.daily_avg_rating:
                    float(daily_avg) if daily_avg is not None else 0.0,
                FactRestaurantMetrics.cumulative_avg_rating:
                    float(cumulative_avg) if cumulative_avg is not None else 0.0,
            },
            synchronize_session=False,
        )
        self.session.commit()
        self.logger.info(
            f"Refreshed review metrics for restaurant {restaurant_id} on "
            f"{affected_date}: {reviews_count} reviews "
            f"({updated} metrics rows updated)"
        )
