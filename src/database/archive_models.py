# File location: src/database/archive_models.py
"""
Archive table models for the historical database.

These tables mirror the structure of the main fact tables and OLTP tables
but exist in a separate database (RestaOrders_Historical) for long-term storage
of data older than 24 months.

The archive tables are:
- fact_orders_archive: Archived order facts
- fact_payments_archive: Archived payment facts
- fact_customer_metrics_archive: Archived customer metrics
- orders_archive: Archived OLTP orders
- payments_archive: Archived OLTP payments
"""
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, Date, Index
from sqlalchemy.orm import declarative_base

ArchiveBase = declarative_base()


class FactOrdersArchive(ArchiveBase):
    """
    Archive of fact_orders table.
    Stores order facts older than 24 months.
    """
    __tablename__ = 'fact_orders_archive'
    __table_args__ = (
        Index('idx_fact_orders_archive_restaurant_datetime', 'restaurant_key', 'datetime_key'),
        Index('idx_fact_orders_archive_customer', 'customer_key'),
        Index('idx_fact_orders_archive_date', 'archived_date'),
    )

    order_key = Column(Integer, primary_key=True)
    order_id = Column(Integer, nullable=False)

    # Dimension keys
    datetime_key = Column(Integer, nullable=False)
    customer_key = Column(Integer, nullable=False)
    restaurant_key = Column(Integer, nullable=False)
    promotion_key = Column(Integer, nullable=True)

    # Order details
    order_status = Column(Integer, nullable=False)
    delivery_type = Column(Integer, nullable=False)
    order_method = Column(Integer, nullable=False)

    # Monetary amounts
    sub_total = Column(Float, nullable=False)
    delivery_fee = Column(Float)
    service_charge = Column(Float)
    total_discount = Column(Float)
    total = Column(Float, nullable=False)

    # Additional metrics
    used_points = Column(Integer)
    is_promotion_applied = Column(Boolean, default=False, nullable=False)
    review_rating = Column(Integer, nullable=True)
    review_message = Column(String(1000), nullable=True)

    # Archive metadata
    archived_date = Column(DateTime, nullable=False)
    original_creation_date = Column(DateTime, nullable=True)


class FactPaymentsArchive(ArchiveBase):
    """
    Archive of fact_payments table.
    Stores payment facts older than 24 months.
    """
    __tablename__ = 'fact_payments_archive'
    __table_args__ = (
        Index('idx_fact_payments_archive_order', 'order_key'),
        Index('idx_fact_payments_archive_restaurant', 'restaurant_key'),
        Index('idx_fact_payments_archive_date', 'archived_date'),
    )

    payment_key = Column(Integer, primary_key=True)
    payment_id = Column(Integer, nullable=False)
    order_key = Column(Integer, nullable=False)
    datetime_key = Column(Integer, nullable=False)
    payment_method_key = Column(Integer, nullable=False)

    # Payment amounts
    sub_total = Column(Float, nullable=False)
    extra_charge = Column(Float)
    discount = Column(Float)
    tax = Column(Float)
    tip = Column(Float)
    total_amount = Column(Float, nullable=False)

    payment_status = Column(Integer, nullable=False)
    restaurant_key = Column(Integer)

    # Archive metadata
    archived_date = Column(DateTime, nullable=False)


class FactCustomerMetricsArchive(ArchiveBase):
    """
    Archive of fact_customer_metrics table.
    Stores customer metrics older than 24 months.
    """
    __tablename__ = 'fact_customer_metrics_archive'
    __table_args__ = (
        Index('idx_fact_customer_metrics_archive_customer', 'customer_key'),
        Index('idx_fact_customer_metrics_archive_restaurant', 'restaurant_key'),
        Index('idx_fact_customer_metrics_archive_date', 'archived_date'),
    )

    metric_key = Column(Integer, primary_key=True)
    order_id = Column(Integer, nullable=False)
    customer_key = Column(Integer, nullable=False)
    datetime_key = Column(Integer, nullable=False)

    # Daily metrics
    daily_orders = Column(Integer)
    daily_spend = Column(Float)
    points_used = Column(Integer)

    # Aggregated metrics
    running_order_count = Column(Integer)
    running_total_spend = Column(Float)
    running_avg_order_value = Column(Float)
    days_since_last_order = Column(Integer)
    order_frequency_days = Column(Float)
    restaurant_key = Column(Integer)

    # Archive metadata
    archived_date = Column(DateTime, nullable=False)


class OrdersArchive(ArchiveBase):
    """
    Archive of orders OLTP table.
    Stores raw order data older than 24 months.
    """
    __tablename__ = 'orders_archive'
    __table_args__ = (
        Index('idx_orders_archive_restaurant_date', 'restaurant_id', 'creation_date'),
        Index('idx_orders_archive_archived', 'archived_date'),
        # Supports the lifetime metric lookup in CustomerDimensionService
        Index('idx_orders_archive_customer', 'customer_id',
              mssql_include=['total', 'creation_date']),
    )

    id = Column(Integer, primary_key=True)
    restaurant_id = Column(Integer)
    customer_id = Column(Integer)
    customer_address_id = Column(Integer)
    delivery_type = Column(Integer)
    order_method = Column(Integer)
    sub_total = Column(Float)
    delivery_fee = Column(Float)
    service_charge = Column(Float)
    total = Column(Float)
    status = Column(Integer)
    creation_date = Column(DateTime)
    payment_status = Column(Integer)
    number_of_orders = Column(Integer)
    phone = Column(String(15))
    order_date = Column(DateTime, nullable=True)
    promotion_id = Column(Integer, nullable=True)
    line_item_discount = Column(Float, default=0)
    discount = Column(Float, default=0)
    card_surcharge = Column(Float, default=0)
    delivery_option_type = Column(Integer, nullable=True)
    tip = Column(Float, default=0)
    used_points = Column(Integer, default=0)
    total_paid = Column(Float, default=0)
    total_balance = Column(Float, default=0)
    review_rating = Column(Integer, nullable=True)
    review_message = Column(String(4000), nullable=True)

    # Archive metadata
    archived_date = Column(DateTime, nullable=False)


class PaymentsArchive(ArchiveBase):
    """
    Archive of payments OLTP table.
    Stores raw payment data older than 24 months.
    """
    __tablename__ = 'payments_archive'
    __table_args__ = (
        Index('idx_payments_archive_order', 'order_id'),
        Index('idx_payments_archive_restaurant', 'restaurant_id'),
        Index('idx_payments_archive_date', 'archived_date'),
    )

    id = Column(Integer, primary_key=True)
    order_id = Column(Integer)
    payment_method_id = Column(Integer)
    payment_method_type = Column(Integer)
    extra_charge = Column(Float)
    sub_total = Column(Float)
    discount = Column(Float, default=0)
    tax = Column(Float, default=0)
    amount = Column(Float)
    status = Column(Integer)
    tip = Column(Float, default=0)
    payment_method_name = Column(String(255))
    restaurant_id = Column(Integer)

    # Archive metadata
    archived_date = Column(DateTime, nullable=False)


class ArchiveLog(ArchiveBase):
    """
    Tracks archive operations for audit purposes.
    """
    __tablename__ = 'archive_log'

    id = Column(Integer, primary_key=True)
    archive_date = Column(DateTime, nullable=False)
    cutoff_date = Column(Date, nullable=False)
    table_name = Column(String(100), nullable=False)
    records_archived = Column(Integer, nullable=False)
    records_deleted = Column(Integer, nullable=False)
    status = Column(String(20), nullable=False)  # SUCCESS, FAILED, PARTIAL
    error_message = Column(String(1000), nullable=True)
    duration_seconds = Column(Float, nullable=True)
