import requests
import json
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, DateTime, ForeignKey, select
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy.orm import sessionmaker, relationship

from sqlalchemy.exc import NoResultFound
import time
import logging
from typing import Optional
import yaml
import base64

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
BASE_URL = "https://api.restajet.com/admin_v1"
PAGE_SIZE = 5

# Database configuration
database_name = 'RestaOrders1'
DATABASE_URL = f'mssql+pyodbc://sa:qwerty@localhost\\SQLEXPRESS/{database_name}?driver=ODBC+Driver+17+for+SQL+Server'

# Initialize SQLAlchemy
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
    promotion = Column(String(25), nullable=True)
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



def create_database_tables():
    """Create all database tables if they don't exist."""
    engine = create_engine(DATABASE_URL)
    Base.metadata.create_all(engine)
    return engine

def get_last_page_index(session: Session, company_id: int, company_name: str) -> int:
    """Get or create the last processed page index for a company"""
    try:
        tracker = session.query(PageIndexTracker).filter_by(company_id=company_id).one()
        return tracker.last_page_index
    except NoResultFound:
        # Create new tracker if doesn't exist
        new_tracker = PageIndexTracker(
            company_id=company_id,
            company_name=company_name,
            last_page_index=2,
            last_updated=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        )
        session.add(new_tracker)
        session.commit()
        return 0

def update_page_index(session: Session, company_id: int, page_index: int):
    """Update the last processed page index for a company"""
    tracker = session.query(PageIndexTracker).filter_by(company_id=company_id).one()
    tracker.last_page_index = page_index
    tracker.last_updated = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    session.commit()

def parse_date(date_str: Optional[str]) -> Optional[datetime]:
    """Parse date string from API response."""
    if not date_str or date_str == "null":
        return None
    if '/Date(' in date_str:
        timestamp = int(date_str.replace('/Date(', '').replace(')/', ''))
        return datetime.fromtimestamp(timestamp/1000)
    return None

def sync_order_to_database(order_data: dict, session):
    """Synchronize order data to database."""
    try:
        data = order_data['Data']
        
        # Create or update restaurant
        restaurant = session.merge(Restaurant(
            id=data['Restaurant']['ID'],
            name=data['Restaurant']['Name'],
            menuid=data['Restaurant']['MenuID']
        ))
        
       
        customer = session.merge(Customer(
            id=data['Customer']['ID'],
            full_name=data['Customer']['FullName'],
            email=data['Customer']['Email'],
            mobile=data['Customer']['Mobile'],
            birth_date=parse_date(data['Customer']['BirthDate']),
            is_email_marketing_allowed=data['Customer']['IsEmailMarketingAllowed'],
            is_sms_marketing_allowed=data['Customer']['IsSmsMarketingAllowed'],
            points=data['Customer']['Points'],
            status=data['Customer']['Status'],
            order_count = data.get('NumberOfOrders', None),
            creation_date=parse_date(data['Customer']['CreationDate'])
            ))
            
        
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
              
            ))
        
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
            payment_status=data['PaymentStatus'],
            number_of_orders=data.get('NumberOfOrders', None),
            phone=data.get('Phone', None),
            order_date=parse_date(data.get('OrderDate', None)),
            promotion=data.get('Promotion', None),
            line_item_discount=data.get('LineItemDiscount', 0),
            discount=data.get('Discount', 0),
            delivery_option_type=data.get('DeliveryOptionType', None),
            tip=data.get('Tip', 0),
            used_points=data.get('UsedPoints', 0),
            total_paid=data.get('TotalPaid', 0),
            total_balance=data.get('TotalBalance', 0),
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
            discount=payment_data.get('Discount', 0),  # Default to 0 if not provided
            tax=payment_data.get('Tax', 0),  # Default to 0 if not provided
            tip=payment_data.get('Tip', 0),  # Default to 0 if not provided
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

def parse_unix_timestamp(timestamp_str):
    try:
        timestamp = int(timestamp_str.strip('/Date()/')) // 1000
        return datetime.fromtimestamp(timestamp, datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        print(f"Error parsing timestamp {timestamp_str}: {e}")
        return None

def load_config(yaml_file):
    """Load credentials from YAML file"""
    try:
        with open(yaml_file, 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        print(f"Error: Could not find {yaml_file}")
        raise
    except yaml.YAMLError as e:
        print(f"Error parsing YAML file: {e}")
        raise

def decode_jwt_payload(token):
    """Decode JWT token payload without verification"""
    try:
        # Split the token and get the payload part (second part)
        parts = token.split('.')
        if len(parts) != 3:
            raise ValueError("Invalid token format")
            
        # Decode the payload
        padding = '=' * (4 - len(parts[1]) % 4)
        payload = base64.b64decode(parts[1] + padding)
        return json.loads(payload)
    except Exception as e:
        print(f"Error decoding token: {e}")
        return None

class RestaAPI:
    def __init__(self):
        self.session_token = None
        self.company_id = None
        
    def login(self, email, password):
        login_url = f"{BASE_URL}/Account/Login"
        params = {
            "email": email,
            "password": password,
            "apiClient": 1,
            "apiClientVersion": 196
        }
        headers = {
            "User-Agent": "RestSharp/105.2.3.0",
            "Accept-Encoding": "gzip, deflate",
            "Accept": "application/json, application/xml, text/json, text/x-json, text/javascript, text/xml"
        }
        
        response = requests.post(login_url, params=params, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Login failed with status code: {response.status_code}")
        
        try:
            data = response.json()
            print("Login response:", json.dumps(data, indent=2))  # Debug print
            
            self.session_token = data.get('SessionToken')
            if not self.session_token:
                raise Exception("Session token not found in response")
            
            # First try to get company_id from token
            token_payload = decode_jwt_payload(self.session_token)
            if token_payload and 'CompanyID' in token_payload:
                self.company_id = int(token_payload['CompanyID'])
            else:
                # Fallback to getting company_id from response
                self.company_id = data.get('Company', {}).get('ID')
                if not self.company_id:
                    raise Exception("Could not find company ID in response or token")
            
            return self.session_token, self.company_id
            
        except json.JSONDecodeError as e:
            print("Raw response:", response.text)  # Debug print
            raise Exception(f"Failed to parse login response: {e}")
    
    def get_orders_list(self, page_index):
        if not self.session_token:
            raise Exception("Not logged in. Call login() first.")
            
        response = requests.get(
            f"{BASE_URL}/Order/List",
            params={
                "pageSize": PAGE_SIZE,
                "pageIndex": page_index,
                # "status": 2,
                "sessionToken": self.session_token
            }
        )
        
        if response.status_code != 200:
            raise Exception(f"Failed to fetch orders: {response.status_code}")
            
        return response.json()

    def fetch_order_details(self,order_id: int) -> dict:
        """Fetch order details from the API."""
        params = {
            'ID': order_id,
            'SessionToken': self.session_token
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

def main(polling_interval: int = 300):
    """Main function to run the order synchronization process."""
    engine = create_database_tables()
    Session = sessionmaker(bind=engine)
    #TODO: Add a while loop with polling intervals
    # Load credentials from YAML file
    config = load_config('config.yaml')

    for restaurant in config:
        api = RestaAPI()
        session = None
        try:
            session = Session()
            
            # Login and get session token and company_id
            print(f"\nProcessing restaurant: {restaurant['Name']}")
            session_token, company_id = api.login(restaurant['Username'], restaurant['Password'])
            print(f"Successfully logged in. Company ID: {company_id}")
            print(f"Session Token: {session_token}")

            page_index = get_last_page_index(session, company_id, restaurant['Name']) - 1
            print(f"Starting from page {page_index}")

            # Poll for orders
            page_index = 1
            while True:
                print(f"Fetching page {page_index}...")
                response_data = api.get_orders(page_index)
                
                if not response_data.get('Data'):
                    print("No more data to fetch.")
                    break

                for order in response_data['Data']:
                    print(f"Processing order: {order['ID']}")
                    order_data = api.fetch_order_details(order['ID'])
                    if order_data is None:
                        logger.error(f"Failed to fetch order details for order {order['ID']}")
                        continue

                    if 'ErrorCode' not in order_data:
                        logger.error(f"Missing ErrorCode in response: {order_data}")
                        continue

                    if order_data['ErrorCode'] == 0:
                        sync_order_to_database(order_data, session)
                    else:
                        logger.error(f"API returned error code: {order_data['ErrorCode']}, Message: {order_data.get('Message', 'No message')}")
                #insert_data(response_data['Data'], company_id)
                update_page_index(session, company_id, page_index)
                page_index += 1
                time.sleep(1)


        except Exception as e:
                print(f"Error processing {restaurant['Name']}: {e}")
                continue  # Continue with next restaurant if one fails    
    
    # while True:
    #     session = None
    #     try:
    #         session = Session()
            
    #         # You might want to maintain a list of order IDs to poll
    #         # For now, we'll just use a sample order ID
    #         order_id = 9487213
            
    #         order_data = fetch_order_details(order_id)
    #         if order_data is None:
    #             logger.error("No valid order data received from API")
    #             continue
                
    #         if 'ErrorCode' not in order_data:
    #             logger.error(f"Missing ErrorCode in response: {order_data}")
    #             continue
                
    #         if order_data['ErrorCode'] == 0:
    #             sync_order_to_database(order_data, session)
    #         else:
    #             logger.error(f"API returned error code: {order_data['ErrorCode']}, Message: {order_data.get('Message', 'No message')}")
            
    #     except Exception as e:
    #         logger.error(f"Error in main loop: {str(e)}")
        
    #     finally:
    #         time.sleep(polling_interval)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Shutting down order sync utility...")