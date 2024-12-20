from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, ForeignKey
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class Restaurant(Base):
    __tablename__ = 'restaurants'
    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    menuid = Column(Integer)


class PageIndexTracker(Base):
    __tablename__ = 'page_index_tracker'
    
    company_id = Column(Integer, primary_key=True)
    company_name = Column(String(255))
    last_page_index = Column(Integer, nullable=False)
    last_updated = Column(String(50))

class Customer(Base):
    __tablename__ = 'customers'
    
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

class Order(Base):
    __tablename__ = 'orders'

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

class Payment(Base):
    __tablename__ = 'payments'
    
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