# File location: src/database/models.py
from sqlalchemy import CheckConstraint, Column, Index, Integer, String, Float, Boolean, DateTime, ForeignKey, LargeBinary, UniqueConstraint
from sqlalchemy.orm import declarative_base, relationship
from datetime import datetime

Base = declarative_base()

class User(Base):
    __tablename__ = 'restaurant_users'
    
    id = Column(Integer, primary_key=True)
    username = Column(String(50), unique=True, nullable=False)
    password = Column(LargeBinary, nullable=False)
    company_id = Column(Integer, nullable=True)
    restaurant_id = Column(Integer)
    company_name = Column(String(128))
    created_at = Column(DateTime, default=datetime.now)
    last_updated = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    active = Column(Boolean)


class Restaurant(Base):
    __tablename__ = 'restaurants'
    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    menuid = Column(Integer)


class PageIndexTracker(Base):
    __tablename__ = 'page_index_tracker'
    
    restaurant_id = Column(Integer, primary_key=True)
    restaurant_name = Column(String(255))
    last_page_index = Column(Integer, nullable=False)
    last_updated = Column(String(50))

class Customer(Base):
    __tablename__ = 'customers'
    __table_args__ = (
        # Customer lookup by restaurant
        Index('idx_customers_restaurant', 'restaurant_id'),
        # Customer status queries
        Index('idx_customers_restaurant_status', 'restaurant_id', 'status'),
    )
    
    id = Column(Integer, primary_key=True)
    full_name = Column(String(255))
    email = Column(String(255))
    mobile = Column(String(20))
    birth_date = Column(DateTime, nullable=True)
    age = Column(Integer, nullable=True)
    is_email_marketing_allowed = Column(Boolean)
    is_sms_marketing_allowed = Column(Boolean)
    points = Column(Integer)
    status = Column(Integer)
    creation_date = Column(DateTime, nullable=True)
    registration_date = Column(DateTime, nullable=True)
    order_count = Column(Integer, nullable=True)
    restaurant_id = Column(Integer, ForeignKey('restaurants.id'))



class CustomerAddress(Base):
    __tablename__ = 'customer_addresses'
    __table_args__ = (
        Index('idx_customer_addresses_restaurant_id', 'restaurant_id'),
        CheckConstraint('restaurant_id IS NOT NULL', name='check_customer_addresses_restaurant_id'),
    )
    
    id = Column(Integer, primary_key=True)
    customer_id = Column(Integer, ForeignKey('customers.id'))
    address_type = Column(Integer)
    street1 = Column(String(255))
    street2 = Column(String(255))
    city_town_name = Column(String(255))
    postal_code = Column(String(20))
    phone = Column(String(20))
    latitude = Column(Float)
    longitude = Column(Float)
    BuildingNo = Column(String(255))
    restaurant_id = Column(Integer, ForeignKey('restaurants.id'))

    


class Promotion(Base):
    __tablename__ = 'promotions'

    id = Column(Integer, primary_key=True)
    companyID = Column(Integer)
    externalID = Column(Integer, nullable=True)
    promotionType = Column(Integer)
    benefitType = Column(Integer)
    name = Column(String(255))
    description = Column(String(255))
    oncePerCustomer = Column(Boolean)
    onlyFirstOrder = Column(Boolean)
    minSubTotal = Column(Float)
    discountType = Column(Integer)
    discountAmount = Column(Float)
    couponCode = Column(String(255), nullable=True)
    restaurant_id = Column(Integer, ForeignKey('restaurants.id'))

class Order(Base):
    __tablename__ = 'orders'
    __table_args__ = (
    # Most common query pattern: orders by restaurant and date
    Index('idx_orders_restaurant_date', 'restaurant_id', 'creation_date'),
    # For payment status tracking
    Index('idx_orders_payment_status', 'restaurant_id', 'payment_status'),
    )

    id = Column(Integer, primary_key=True)
    restaurant_id = Column(Integer, ForeignKey('restaurants.id'))
    customer_id = Column(Integer, ForeignKey('customers.id'))
    customer_address_id = Column(Integer, ForeignKey('customer_addresses.id'))
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
    promotion_id = Column(Integer, ForeignKey('promotions.id'), nullable=True)
    line_item_discount = Column(Float, default=0)
    discount = Column(Float, default=0)
    card_surcharge = Column(Float, default=0)
    delivery_option_type = Column(Integer, nullable=True)
    tip = Column(Float, default=0)
    used_points = Column(Integer, default=0)
    total_paid = Column(Float, default=0)
    total_balance = Column(Float, default=0)
    restaurant_id = Column(Integer, ForeignKey('restaurants.id'))

class Payment(Base):
    __tablename__ = 'payments'
    __table_args__ = (
        # Payment lookups by order
        Index('idx_payments_order', 'order_id'),
        # Payment method analysis
        Index('idx_payments_method_restaurant', 'payment_method_id', 'restaurant_id'),
    )
    
    id = Column(Integer, primary_key=True)
    order_id = Column(Integer, ForeignKey('orders.id'))
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
    restaurant_id = Column(Integer, ForeignKey('restaurants.id'))


class ProcessedOrders(Base):
    __tablename__ = 'fact_processed_orders'
    __table_args__ = (
        # Order processing status lookup
        Index('idx_processed_orders_type', 'order_id', 'fact_type'),
    )
    
    id = Column(Integer, primary_key=True)
    order_id = Column(Integer, nullable=False)
    fact_type = Column(String(50), nullable=False)  # Added this column
    processed_date = Column(DateTime, nullable=False)
    

class OrderSyncTracker(Base):
    __tablename__ = 'order_sync_tracker'
    
    restaurant_id = Column(Integer, primary_key=True)
    restaurant_name = Column(String(255))
    last_order_id = Column(Integer, nullable=False)
    last_order_date = Column(DateTime, nullable=False)
    last_sync_date = Column(DateTime, nullable=False)
    total_orders_synced = Column(Integer, default=0)