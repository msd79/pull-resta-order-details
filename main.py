import requests
import json
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, DateTime, ForeignKey
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
import time
import logging
from typing import Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('order_sync.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# API configuration
API_BASE_URL = "https://api.restajet.com/admin_v1/order/Detail"
SESSION_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySUQiOiI1NDc3IiwiQ29tcGFueUlEIjoiMjkyMCIsImV4cCI6MTc2NjA1MDI5MX0.pRCk4CfcozQlL-ApU8CcBd_z1sHuI9eEoYVM0pY7mLc"

# Database configuration
database_name = 'RestaOrders1'
DATABASE_URL = f'mssql+pyodbc://sa:qwerty@localhost\\SQLEXPRESS/{database_name}?driver=ODBC+Driver+17+for+SQL+Server'

# Initialize SQLAlchemy
Base = declarative_base()

class Restaurant(Base):
    __tablename__ = 'restaurants'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    is_open = Column(Boolean)
    timezone_id = Column(String(50))
    phone = Column(String(20))

class Customer(Base):
    __tablename__ = 'customers'
    
    id = Column(Integer, primary_key=True)
    full_name = Column(String(255))
    email = Column(String(255))
    mobile = Column(String(20))
    birth_date = Column(DateTime, nullable=True)
    is_email_marketing_allowed = Column(Boolean)
    is_sms_marketing_allowed = Column(Boolean)
    points = Column(Integer)
    status = Column(Integer)
    creation_date = Column(DateTime, nullable=True)

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
    status = Column(Integer)

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

class Payment(Base):
    __tablename__ = 'payments'
    
    id = Column(Integer, primary_key=True)
    order_id = Column(Integer, ForeignKey('orders.id'))
    payment_method_id = Column(Integer)
    payment_method_type = Column(Integer)
    extra_charge = Column(Float)
    sub_total = Column(Float)
    amount = Column(Float)
    status = Column(Integer)
    payment_method_name = Column(String(255))

def create_database_tables():
    """Create all database tables if they don't exist."""
    engine = create_engine(DATABASE_URL)
    Base.metadata.create_all(engine)
    return engine

def parse_date(date_str: Optional[str]) -> Optional[datetime]:
    """Parse date string from API response."""
    if not date_str or date_str == "null":
        return None
    if '/Date(' in date_str:
        timestamp = int(date_str.replace('/Date(', '').replace(')/', ''))
        return datetime.fromtimestamp(timestamp/1000)
    return None

def fetch_order_details(order_id: int) -> dict:
    """Fetch order details from the API."""
    params = {
        'ID': order_id,
        'SessionToken': SESSION_TOKEN
    }
    
    try:
        logger.info(f"Attempting to fetch order {order_id} from API")
        response = requests.get(API_BASE_URL, params=params)
        #logger.info(f"API Response Status Code: {response.status_code}")
        #logger.info(f"API Response Headers: {response.headers}")
        logger.info(f"API Response URL: {response.request.url}")
        #logger.info(f"API Response URL: {response.request.path_url}")
        
        # Log the raw response for debugging
        logger.debug(f"Raw API Response: {response.text[:1000]}")  # Log first 1000 chars to avoid huge logs
        
        response.raise_for_status()
        data = response.json()
        
        if data is None:
            logger.error("API returned None response")
            return None
            
        if 'Data' not in data:
            logger.error(f"Missing 'Data' key in API response. Response: {data}")
            return None
            
        return data
        
    except requests.RequestException as e:
        logger.error(f"Error fetching order {order_id}: {str(e)}")
        logger.error(f"Request URL: {API_BASE_URL}")
        logger.error(f"Request Parameters: {params}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON response: {str(e)}")
        logger.error(f"Raw response: {response.text[:1000]}")  # Log first 1000 chars
        return None

def sync_order_to_database(order_data: dict, session):
    """Synchronize order data to database."""
    try:
        data = order_data['Data']
        
        # Create or update restaurant
        restaurant = session.merge(Restaurant(
            id=data['Restaurant']['ID'],
            name=data['Restaurant']['Name'],
            is_open=data['Restaurant']['IsOpen'],
            timezone_id=data['Restaurant']['TimeZoneID'],
            phone=data['Restaurant']['Phone']
        ))
        
        # Only insert customer if doesn't exist
        customer = session.query(Customer).filter_by(id=data['Customer']['ID']).first()
        if not customer:
            customer = Customer(
            id=data['Customer']['ID'],
            full_name=data['Customer']['FullName'],
            email=data['Customer']['Email'],
            mobile=data['Customer']['Mobile'],
            birth_date=parse_date(data['Customer']['BirthDate']),
            is_email_marketing_allowed=data['Customer']['IsEmailMarketingAllowed'],
            is_sms_marketing_allowed=data['Customer']['IsSmsMarketingAllowed'],
            points=data['Customer']['Points'],
            status=data['Customer']['Status'],
            creation_date=parse_date(data['Customer']['CreationDate'])
            )
            session.add(customer)
        
        # Create or update customer address
        if data['OrderMethod'] == 1: # Delivery
            address = session.merge(CustomerAddress(
                id=data['CustomerAddress']['ID'],
                customer_id=data['CustomerAddress']['CustomerID'],
                address_type=data['CustomerAddress']['AddressType'],
                street1=data['CustomerAddress']['Street1'],
                street2=data['CustomerAddress']['Street2'],
                city_town_name=data['CustomerAddress']['CityTownName'],
                postal_code=data['CustomerAddress']['PostalCode'],
                phone=data['CustomerAddress']['Phone'],
                latitude=data['CustomerAddress']['Latitude'],
                longitude=data['CustomerAddress']['Longitude'],
                status=data['CustomerAddress']['Status']
            ))
            address = session.merge(CustomerAddress(
                id=data['CustomerAddress']['ID'],
                customer_id=data['CustomerAddress']['CustomerID'],
                address_type=data['CustomerAddress']['AddressType'],
                street1=data['CustomerAddress']['Street1'],
                street2=data['CustomerAddress']['Street2'],
                city_town_name=data['CustomerAddress']['CityTownName'],
                postal_code=data['CustomerAddress']['PostalCode'],
                phone=data['CustomerAddress']['Phone'],
                latitude=data['CustomerAddress']['Latitude'],
                longitude=data['CustomerAddress']['Longitude'],
                status=data['CustomerAddress']['Status']
            ))
        
        # Create or update order
        order = session.merge(Order(
            id=data['ID'],
            restaurant_id=data['Restaurant']['ID'],
            customer_id=data['Customer']['ID'],
            customer_address_id=data['CustomerAddress']['ID'] if data['OrderMethod'] == 1 else None,
            delivery_type=data['DeliveryType'],
            order_method=data['OrderMethod'],
            sub_total=data['SubTotal'],
            delivery_fee=data['DeliveryFee'],
            service_charge=data['ServiceCharge'],
            total=data['Total'],
            status=data['Status'],
            creation_date=parse_date(data['CreationDate']),
            payment_status=data['PaymentStatus']
        ))
        
        # Create or update payments
        for payment_data in data['Payments']:
            payment = session.merge(Payment(
                id=payment_data['ID'],
                order_id=payment_data['OrderID'],
                payment_method_id=payment_data['PaymentMethodID'],
                payment_method_type=payment_data['PaymentMethodType'],
                extra_charge=payment_data['ExtraCharge'],
                sub_total=payment_data['SubTotal'],
                amount=payment_data['Amount'],
                status=payment_data['Status'],
                payment_method_name=payment_data['PaymentMethodName']
            ))
        
        session.commit()
        logger.info(f"Successfully synchronized order {data['ID']}")
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error syncing order to database: {str(e)}")
        raise

def main(polling_interval: int = 300):
    """Main function to run the order synchronization process."""
    engine = create_database_tables()
    Session = sessionmaker(bind=engine)
    
    while True:
        session = None
        try:
            session = Session()
            
            # You might want to maintain a list of order IDs to poll
            # For now, we'll just use a sample order ID
            order_id = 9487196
            
            order_data = fetch_order_details(order_id)
            if order_data is None:
                logger.error("No valid order data received from API")
                continue
                
            if 'ErrorCode' not in order_data:
                logger.error(f"Missing ErrorCode in response: {order_data}")
                continue
                
            if order_data['ErrorCode'] == 0:
                sync_order_to_database(order_data, session)
            else:
                logger.error(f"API returned error code: {order_data['ErrorCode']}, Message: {order_data.get('Message', 'No message')}")
            
        except Exception as e:
            logger.error(f"Error in main loop: {str(e)}")
        
        finally:
            time.sleep(polling_interval)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Shutting down order sync utility...")