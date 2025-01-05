# File location: src/database/dimentional_models.py
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, ForeignKey, SmallInteger
from sqlalchemy.orm import declarative_base, relationship
from datetime import datetime

Base = declarative_base()

# Dimension Tables
class DimDateTime(Base):
    __tablename__ = 'dim_datetime'
    
    datetime_key = Column(Integer, primary_key=True)  # Surrogate key
    datetime = Column(DateTime(timezone=False), nullable=False)
    date = Column(DateTime(timezone=False), nullable=False)  # Explicitly set timezone=False
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
    
    # Time intelligence support
    fiscal_year = Column(SmallInteger, nullable=False)
    fiscal_quarter = Column(SmallInteger, nullable=False)
    fiscal_month = Column(SmallInteger, nullable=False)

class DimCustomer(Base):
    __tablename__ = 'dim_customer'
    
    customer_key = Column(Integer, primary_key=True)  # Surrogate key
    customer_id = Column(Integer, nullable=False)  # Business key
    full_name = Column(String(255))
    email = Column(String(255))
    mobile = Column(String(20))
    birth_date = Column(DateTime)
    age_group = Column(String(20))
    
    # Type 2 SCD fields
    effective_date = Column(DateTime, nullable=False)
    expiration_date = Column(DateTime)
    is_current = Column(Boolean, nullable=False)
    
    # Customer status and preferences
    is_active = Column(Boolean, nullable=False)
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
    
    # Type 2 SCD fields
    effective_date = Column(DateTime, nullable=False)
    expiration_date = Column(DateTime)
    is_current = Column(Boolean, nullable=False)
    
    # Performance metrics
    avg_daily_orders = Column(Float)
    avg_order_value = Column(Float)
    peak_hour_capacity = Column(Integer)

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
    restaurant_id = Column(Integer)

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

# Fact Tables
class FactOrders(Base):
    __tablename__ = 'fact_orders'
    
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

class FactPayments(Base):
    __tablename__ = 'fact_payments'
    
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

class FactCustomerMetrics(Base):
    __tablename__ = 'fact_customer_metrics'
    
    metric_key = Column(Integer, primary_key=True)
    order_id = Column(Integer, nullable=False, unique=True)  # Added order_id as unique identifier
    customer_key = Column(Integer, ForeignKey('dim_customer.customer_key'), nullable=False)
    datetime_key = Column(Integer, ForeignKey('dim_datetime.datetime_key'), nullable=False)
    
    # Daily metrics
    daily_orders = Column(Integer)
    daily_spend = Column(Float)
    daily_items = Column(Integer)
    points_used = Column(Integer)
    
    # Aggregated metrics
    running_order_count = Column(Integer)
    running_total_spend = Column(Float)
    running_avg_order_value = Column(Float)
    days_since_last_order = Column(Integer)
    order_frequency_days = Column(Float)  # Average days between orders