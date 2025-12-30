# File location: scripts/backfill_promotion_summaries.py
"""
One-time script to populate FactDailyPromotionSummary from historical orders.

This script aggregates existing order data into the promotion summary table,
enabling fast Power BI queries for promotion analysis.

Usage:
    python scripts/backfill_promotion_summaries.py
    python scripts/backfill_promotion_summaries.py --start-date 2024-01-01
    python scripts/backfill_promotion_summaries.py --start-date 2024-01-01 --end-date 2024-12-31
    python scripts/backfill_promotion_summaries.py --restaurant-id 123
    python scripts/backfill_promotion_summaries.py --dry-run

Options:
    --start-date    Start date for backfill (default: 2020-01-01)
    --end-date      End date for backfill (default: today)
    --restaurant-id Specific restaurant to backfill (default: all)
    --batch-size    Days per batch (default: 30)
    --dry-run       Show what would be done without making changes
"""
import argparse
import asyncio
from datetime import datetime, timedelta, date as date_type
from typing import Dict, List, Optional, Tuple
from sqlalchemy import create_engine, func, text, Date
from sqlalchemy.orm import sessionmaker, Session
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config.settings import get_config
from src.database.dimentional_models import (
    FactDailyPromotionSummary, DimRestaurant, DimPromotion, Base as DimBase
)
from src.database.models import Order, Base as OLTPBase

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PromotionSummaryBackfill:
    """Handles backfill of promotion summary data from historical orders."""

    def __init__(self, session: Session, dry_run: bool = False):
        self.session = session
        self.dry_run = dry_run
        self.stats = {
            'dates_processed': 0,
            'summaries_created': 0,
            'summaries_updated': 0,
            'orders_processed': 0,
            'errors': 0
        }

    async def run_backfill(
        self,
        start_date: date_type,
        end_date: date_type,
        restaurant_id: Optional[int] = None,
        batch_days: int = 30
    ) -> Dict:
        """
        Run the backfill process.

        Args:
            start_date: Start date for backfill
            end_date: End date for backfill
            restaurant_id: Optional specific restaurant to backfill
            batch_days: Number of days to process per batch

        Returns:
            Dictionary of statistics about the backfill
        """
        logger.info(f"Starting backfill from {start_date} to {end_date}")
        if restaurant_id:
            logger.info(f"Filtering to restaurant_id: {restaurant_id}")
        if self.dry_run:
            logger.info("DRY RUN - No changes will be made")

        # Get restaurants to process
        restaurants = self._get_restaurants(restaurant_id)
        logger.info(f"Found {len(restaurants)} restaurant(s) to process")

        # Process in date batches
        current_date = start_date
        while current_date <= end_date:
            batch_end = min(current_date + timedelta(days=batch_days - 1), end_date)

            logger.info(f"Processing batch: {current_date} to {batch_end}")

            for restaurant_key, rest_id, rest_name in restaurants:
                try:
                    await self._process_restaurant_batch(
                        restaurant_key=restaurant_key,
                        restaurant_id=rest_id,
                        restaurant_name=rest_name,
                        batch_start=current_date,
                        batch_end=batch_end
                    )
                except Exception as e:
                    logger.error(f"Error processing restaurant {rest_name}: {str(e)}")
                    self.stats['errors'] += 1

            current_date = batch_end + timedelta(days=1)

        logger.info("Backfill complete!")
        logger.info(f"Statistics: {self.stats}")

        return self.stats

    def _get_restaurants(self, restaurant_id: Optional[int] = None) -> List[Tuple[int, int, str]]:
        """Get list of restaurants to process."""
        query = self.session.query(
            DimRestaurant.restaurant_key,
            DimRestaurant.restaurant_id,
            DimRestaurant.restaurant_name
        ).filter(DimRestaurant.is_active == True)

        if restaurant_id:
            query = query.filter(DimRestaurant.restaurant_id == restaurant_id)

        return query.all()

    async def _process_restaurant_batch(
        self,
        restaurant_key: int,
        restaurant_id: int,
        restaurant_name: str,
        batch_start: date_type,
        batch_end: date_type
    ) -> None:
        """Process a batch of dates for a single restaurant."""

        # Get daily aggregates using SQL for efficiency
        aggregates = self._get_daily_aggregates(restaurant_id, batch_start, batch_end)

        for (order_date, promotion_id), metrics in aggregates.items():
            self.stats['dates_processed'] += 1

            # Get promotion_key if applicable
            promotion_key = None
            if promotion_id:
                promotion = self.session.query(DimPromotion).filter(
                    DimPromotion.promotion_id == promotion_id
                ).first()
                if promotion:
                    promotion_key = promotion.promotion_key

            if not self.dry_run:
                await self._upsert_summary(
                    restaurant_key=restaurant_key,
                    summary_date=order_date,
                    promotion_key=promotion_key,
                    metrics=metrics
                )
            else:
                logger.debug(
                    f"  Would create/update: {restaurant_name} | {order_date} | "
                    f"promo_key={promotion_key} | orders={metrics['order_count']}"
                )

        self.stats['orders_processed'] += sum(m['order_count'] for m in aggregates.values())

    def _get_daily_aggregates(
        self,
        restaurant_id: int,
        start_date: date_type,
        end_date: date_type
    ) -> Dict[Tuple[date_type, Optional[int]], Dict]:
        """
        Get pre-aggregated daily metrics using efficient SQL query.

        Returns:
            Dictionary keyed by (date, promotion_id) with metric values
        """
        start_datetime = datetime.combine(start_date, datetime.min.time())
        end_datetime = datetime.combine(end_date, datetime.max.time())

        # Query orders grouped by date and promotion
        results = self.session.query(
            func.cast(Order.creation_date, Date).label('order_date'),
            Order.promotion_id,
            func.count(Order.id).label('order_count'),
            func.sum(Order.total).label('total_revenue'),
            func.avg(Order.total).label('avg_order_value'),
            func.sum(Order.discount + Order.line_item_discount).label('total_discount'),
            func.sum(Order.sub_total).label('total_sub_total'),
            func.count(func.distinct(Order.customer_id)).label('unique_customers'),
            func.sum(func.IIF(Order.delivery_type == 1, 1, 0)).label('delivery_orders'),
            func.sum(func.IIF(Order.delivery_type == 2, 1, 0)).label('pickup_orders'),
            func.min(Order.total).label('min_order_value'),
            func.max(Order.total).label('max_order_value'),
        ).filter(
            Order.restaurant_id == restaurant_id,
            Order.creation_date >= start_datetime,
            Order.creation_date <= end_datetime
        ).group_by(
            func.cast(Order.creation_date, Date),
            Order.promotion_id
        ).all()

        aggregates = {}
        for row in results:
            key = (row.order_date, row.promotion_id)
            aggregates[key] = {
                'order_count': row.order_count or 0,
                'total_revenue': float(row.total_revenue or 0),
                'avg_order_value': float(row.avg_order_value or 0),
                'total_discount_given': float(row.total_discount or 0),
                'total_sub_total': float(row.total_sub_total or 0),
                'unique_customers': row.unique_customers or 0,
                'new_customers': 0,  # Would require additional query per customer
                'repeat_customers': 0,  # Would require additional query per customer
                'delivery_orders': row.delivery_orders or 0,
                'pickup_orders': row.pickup_orders or 0,
                'min_order_value': float(row.min_order_value) if row.min_order_value else None,
                'max_order_value': float(row.max_order_value) if row.max_order_value else None,
            }

        return aggregates

    async def _upsert_summary(
        self,
        restaurant_key: int,
        summary_date: date_type,
        promotion_key: Optional[int],
        metrics: Dict
    ) -> None:
        """Insert or update a summary record."""
        try:
            # Find existing record
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
                # Update existing
                for key, value in metrics.items():
                    setattr(existing, key, value)
                self.stats['summaries_updated'] += 1
            else:
                # Create new
                summary = FactDailyPromotionSummary(
                    restaurant_key=restaurant_key,
                    date=summary_date,
                    promotion_key=promotion_key,
                    **metrics
                )
                self.session.add(summary)
                self.stats['summaries_created'] += 1

            self.session.commit()

        except Exception as e:
            self.session.rollback()
            logger.error(f"Error upserting summary: {str(e)}")
            self.stats['errors'] += 1


async def main():
    parser = argparse.ArgumentParser(
        description="Backfill promotion summary table from historical orders"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default="2020-01-01",
        help="Start date for backfill (format: YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="End date for backfill (format: YYYY-MM-DD, default: today)"
    )
    parser.add_argument(
        "--restaurant-id",
        type=int,
        default=None,
        help="Specific restaurant ID to backfill (default: all)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=30,
        help="Number of days per batch (default: 30)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes"
    )

    args = parser.parse_args()

    # Parse dates
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date() if args.end_date else datetime.now().date()

    try:
        # Load configuration
        config = get_config()

        # Create database connection
        engine = create_engine(config.database.connection_string)

        # Ensure tables exist
        DimBase.metadata.create_all(engine)

        Session = sessionmaker(bind=engine)
        session = Session()

        # Run backfill
        backfill = PromotionSummaryBackfill(session, dry_run=args.dry_run)
        stats = await backfill.run_backfill(
            start_date=start_date,
            end_date=end_date,
            restaurant_id=args.restaurant_id,
            batch_days=args.batch_size
        )

        # Print summary
        print("\n" + "=" * 50)
        print("BACKFILL SUMMARY")
        print("=" * 50)
        print(f"Dates processed:    {stats['dates_processed']}")
        print(f"Summaries created:  {stats['summaries_created']}")
        print(f"Summaries updated:  {stats['summaries_updated']}")
        print(f"Orders processed:   {stats['orders_processed']}")
        print(f"Errors:             {stats['errors']}")
        print("=" * 50)

    except Exception as e:
        logger.error(f"Backfill failed: {str(e)}")
        raise
    finally:
        session.close()


if __name__ == "__main__":
    asyncio.run(main())
