# File location: src/database/dimentional_models.py
from sqlalchemy import CheckConstraint, Column, Date, Index, Integer, String, Float, Boolean, DateTime, ForeignKey, SmallInteger, UniqueConstraint
from sqlalchemy.orm import declarative_base, relationship
from datetime import datetime

Base = declarative_base()

# Dimension Tables
class DimDateTime(Base):
    __tablename__ = 'dim_datetime'
    __table_args__ = (
        # Common time-based lookups
        Index('idx_dim_datetime_date', 'date'),
        Index('idx_dim_datetime_year_month', 'year', 'month'),
    )
    
    datetime_key = Column(Integer, primary_key=True)  # Surrogate key
    datetime = Column(DateTime(timezone=False), nullable=False)
    date = Column(Date, nullable=False) # Explicitly set timezone=False
    year = Column(SmallInteger, nullable=False)
    quarter = Column(SmallInteger, nullable=False)
    month = Column(SmallInteger, nullable=False)
    week = Column(SmallInteger, nullable=False)
    day = Column(SmallInteger, nullable=False)
    hour = Column(SmallInteger, nullable=False)
    minute = Column(SmallInteger, nullable=False)
    day_of_week = Column(SmallInteger, nullable=False)  # 1-7
    is_weekend = Column(Boolean, nullable=False)
    is_holiday = Column(Boolean, nullable=False)
    
    # Business-specific time periods
    day_part = Column(String(20), nullable=False)  # Breakfast, Lunch, Dinner
    is_peak_hour = Column(Boolean, nullable=False)
    is_business_hour = Column(Boolean, nullable=False)
    
    # New fields
    year_month = Column(Integer, nullable=False)         # e.g., 202001
    month_name = Column(String(10), nullable=False)        # e.g., "January"
    day_name = Column(String(10), nullable=False)          # e.g., "Monday"
    year_month_label = Column(String(7), nullable=False)   # e.g., "2020-01"

class DimCustomer(Base):
    __tablename__ = 'dim_customer'
    __table_args__ = (
        # Current customer lookup
        Index('idx_dim_customer_current', 'customer_id', 'is_current'),
        # Restaurant-specific customer segment analysis
        Index('idx_dim_customer_restaurant_segment', 'restaurant_key', 'customer_segment'),
    )
    
    customer_key = Column(Integer, primary_key=True)  # Surrogate key
    restaurant_key = Column(Integer, ForeignKey('dim_restaurant.restaurant_key'), nullable=False)
    customer_id = Column(Integer, nullable=False)  # Business key
    full_name = Column(String(255))
    email = Column(String(255))
    mobile = Column(String(20))
    birth_date = Column(DateTime)
    age_group = Column(String(20))
    regitered_at = Column(DateTime)
    
    # Type 2 SCD fields
    effective_date = Column(DateTime, nullable=False)
    expiration_date = Column(DateTime)
    is_current = Column(Boolean, nullable=False)
    
    # Customer status and preferences
 
    is_email_marketing_allowed = Column(Boolean)
    is_sms_marketing_allowed = Column(Boolean)
    
    # Pre-calculated metrics
    lifetime_order_count = Column(Integer)
    lifetime_order_value = Column(Float)
    average_order_value = Column(Float)
    first_order_date = Column(DateTime)
    last_order_date = Column(DateTime)
    customer_segment = Column(String(50))  # VIP, Regular, Occasional, etc.
    customer_tenure_days = Column(Integer)

class DimRestaurant(Base):
    __tablename__ = 'dim_restaurant'
    
    restaurant_key = Column(Integer, primary_key=True)  # Surrogate key
    restaurant_id = Column(Integer, nullable=False)  # Business key
    restaurant_name = Column(String(255))
    company_id = Column(Integer)
    company_name = Column(String(128))
    is_active = Column(Boolean, default=True, nullable=False)
    
    # Type 2 SCD fields
    # effective_date = Column(DateTime, nullable=False)
    # expiration_date = Column(DateTime)
    # is_current = Column(Boolean, nullable=False)
    
    # # Performance metrics
    # avg_daily_orders = Column(Float)
    # avg_order_value = Column(Float)
    # peak_hour_capacity = Column(Integer)

class DimPromotion(Base):
    __tablename__ = 'dim_promotion'
    
    promotion_key = Column(Integer, primary_key=True)  # Surrogate key
    promotion_id = Column(Integer, nullable=False)  # Business key
    promotion_name = Column(String(255))
    promotion_description = Column(String(255))
    
    # Type 1 SCD fields (assuming promotions don't change historically)
    promotion_type = Column(Integer)
    benefit_type = Column(Integer)
    discount_type = Column(Integer)
    discount_amount = Column(Float)
    min_subtotal = Column(Float)
    coupon_code = Column(String(255))
    
    # Additional attributes
    is_first_order_only = Column(Boolean)
    is_once_per_customer = Column(Boolean)
    company_id = Column(Integer)
    restaurant_key = Column(Integer, ForeignKey('dim_restaurant.restaurant_key'), nullable=False)

class DimPaymentMethod(Base):
    __tablename__ = 'dim_payment_method'

    payment_method_key = Column(Integer, primary_key=True)  # Surrogate key
    payment_method_id = Column(Integer, nullable=False)  # Business key
    payment_method_name = Column(String(255))
    payment_method_type = Column(Integer)
    requires_extra_charge = Column(Boolean)
    is_digital = Column(Boolean)
    is_card = Column(Boolean)
    is_cash = Column(Boolean)
    restaurant_id = Column(Integer)

    

# Fact Tables
class FactOrders(Base):
    __tablename__ = 'fact_orders'
    __table_args__ = (
        # Time-based analysis by restaurant
        Index('idx_fact_orders_restaurant_datetime', 'restaurant_key', 'datetime_key'),
        # Customer analysis
        Index('idx_fact_orders_customer', 'customer_key', 'restaurant_key'),
    )
    
    order_key = Column(Integer, primary_key=True)
    order_id = Column(Integer, nullable=False)  # Business key
    
    # Dimension keys
    datetime_key = Column(Integer, ForeignKey('dim_datetime.datetime_key'), nullable=False)
    customer_key = Column(Integer, ForeignKey('dim_customer.customer_key'), nullable=False)
    restaurant_key = Column(Integer, ForeignKey('dim_restaurant.restaurant_key'), nullable=False)
    promotion_key = Column(Integer, ForeignKey('dim_promotion.promotion_key'))
    
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

class FactPayments(Base):
    __tablename__ = 'fact_payments'
    __table_args__ = (
        Index('idx_fact_payments_restaurant_key', 'restaurant_key'),
        CheckConstraint('restaurant_key IS NOT NULL', name='check_fact_payments_restaurant_key'),
    )
    
    payment_key = Column(Integer, primary_key=True)
    payment_id = Column(Integer, nullable=False)  # Business key
    order_key = Column(Integer, ForeignKey('fact_orders.order_key'), nullable=False)
    datetime_key = Column(Integer, ForeignKey('dim_datetime.datetime_key'), nullable=False)
    payment_method_key = Column(Integer, ForeignKey('dim_payment_method.payment_method_key'), nullable=False)
    
    # Payment amounts
    sub_total = Column(Float, nullable=False)
    extra_charge = Column(Float)
    discount = Column(Float)
    tax = Column(Float)
    tip = Column(Float)
    total_amount = Column(Float, nullable=False)
    
    payment_status = Column(Integer, nullable=False)
    restaurant_key = Column(Integer, ForeignKey('dim_restaurant.restaurant_key'))

    

class FactCustomerMetrics(Base):
    __tablename__ = 'fact_customer_metrics'
    __table_args__ = (
        # Time-series customer metrics
        Index('idx_fact_customer_metrics_datetime', 'customer_key', 'datetime_key'),
        # Restaurant analysis
        Index('idx_fact_customer_metrics_restaurant', 'restaurant_key', 'datetime_key'),
    )
    
    metric_key = Column(Integer, primary_key=True)
    order_id = Column(Integer, nullable=False, unique=True)  # Added order_id as unique identifier
    customer_key = Column(Integer, ForeignKey('dim_customer.customer_key'), nullable=False)
    datetime_key = Column(Integer, ForeignKey('dim_datetime.datetime_key'), nullable=False)
    
    # Daily metrics
    daily_orders = Column(Integer)
    daily_spend = Column(Float)
    points_used = Column(Integer)
    
    # Aggregated metrics
    running_order_count = Column(Integer)
    running_total_spend = Column(Float)
    running_avg_order_value = Column(Float)
    days_since_last_order = Column(Integer)
    order_frequency_days = Column(Float)  # Average days between orders
    restaurant_key = Column(Integer, ForeignKey('dim_restaurant.restaurant_key'))

class FactRestaurantMetrics(Base):
    __tablename__ = 'fact_restaurant_metrics'
    __table_args__ = (
        UniqueConstraint('restaurant_key', 'datetime_key', name='unique_restaurant_date'),
    )
    
    metric_key = Column(Integer, primary_key=True)
    restaurant_key = Column(Integer, ForeignKey('dim_restaurant.restaurant_key'), nullable=False)
    datetime_key = Column(Integer, ForeignKey('dim_datetime.datetime_key'), nullable=False)
    
    # Daily order metrics
    total_orders = Column(Integer, default=0)
    total_revenue = Column(Float, default=0.0)
    avg_order_value = Column(Float, default=0.0)
    
    # Time of day breakdown
    before_peak_orders = Column(Integer, default=0)
    peak_orders = Column(Integer, default=0)
    after_peak_orders = Column(Integer, default=0)
    
    # Delivery metrics
    delivery_orders = Column(Integer, default=0)
    pickup_orders = Column(Integer, default=0)
    
    # Payment metrics
    cash_payments = Column(Integer, default=0)
    card_payments = Column(Integer, default=0)
    reward_points = Column(Integer, default=0)
    
    # Promotion metrics
    orders_with_promotion = Column(Integer, default=0)
    total_discount_amount = Column(Float, default=0.0)
    
    # Performance metrics
    peak_hour_orders = Column(Integer, default=0)
    peak_hour = Column(Integer)  # 0-23 representing hour of day
    
    # Create a unique constraint for restaurant and date combination
    