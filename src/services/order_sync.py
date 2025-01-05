# File location: src/services/order_sync.py
from typing import Optional, List
from sqlalchemy.orm import Session
from src.database.models import Restaurant, Customer, CustomerAddress, Order, Payment, Promotion
from datetime import datetime
import logging

class OrderSyncService:
    def __init__(self, session: Session):
        self.session = session
        self.logger = logging.getLogger(__name__)

    def sync_order_data(self, order_data: dict) -> Order:
        """
        Synchronize complete order data including associated entities.
        
        Args:
            order_data: Dictionary containing complete order data under 'Data' key
                       
        Returns:
            Order: The synced order object
            
        Raises:
            ValueError: If required data is missing
            Exception: If there's an error during synchronization
        """
        try:
            if 'Data' not in order_data:
                raise ValueError("Missing 'Data' key in order_data")
                
            data = order_data['Data']
            
            # Begin transaction
            self.session.begin_nested()
            
            # Sync all related entities
            restaurant = self._sync_restaurant(data['Restaurant'])
            customer = self._sync_customer(data['Customer'],data['Restaurant'], data['NumberOfOrders'])
            
            # Handle promotion if present
            promotion_id = None
            if data['Promotion'] is not None:
                promotion = self._sync_promotion(data['Promotion'], restaurant)
                promotion_id = promotion.id
            
            # Handle address for delivery orders
            address = None
            if data['OrderMethod'] == 1:  # Delivery
                address = self._sync_address(data['CustomerAddress'], restaurant.id)
            
            # Sync the order itself
            order = self._sync_order(data, restaurant, customer, address, promotion_id)
            
            # Sync payments
            self._sync_payments(data['Payments'], order.id, restaurant.id)
            
            # Commit transaction
            self.session.commit()
            
            self.logger.info(f"Successfully synchronized order for {restaurant.name} OrderID: {data['ID']} \n")
            return order
            
        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error syncing order data: {str(e)}")
            raise

    def _sync_restaurant(self, restaurant_data: dict) -> Restaurant:
        """Sync restaurant data to database."""
        try:
            restaurant = self.session.merge(
                Restaurant(
                    id=restaurant_data['ID'],
                    name=restaurant_data['Name'],
                    menuid=restaurant_data['MenuID']
                )
            )
            return restaurant
        except Exception as e:
            self.logger.error(f"Error syncing restaurant: {str(e)}")
            raise

    def _sync_customer(self, customer_data: dict, restaurant: Restaurant, number_of_orders:int ) -> Customer:
        """Sync customer data to database."""
        try:
            customer = self.session.merge(
                Customer(
                    id=customer_data['ID'],
                    full_name=customer_data['FullName'],
                    email=customer_data['Email'],
                    mobile=customer_data['Mobile'],
                    birth_date=self._parse_date(customer_data.get('BirthDate')),
                    is_email_marketing_allowed=customer_data['IsEmailMarketingAllowed'],
                    is_sms_marketing_allowed=customer_data['IsSmsMarketingAllowed'],
                    points=customer_data['Points'],
                    status=customer_data['Status'],
                    creation_date=self._parse_date(customer_data['CreationDate']),
                    order_count=number_of_orders,
                    restaurant_id=restaurant["ID"]

                )
            )
            return customer
        except Exception as e:
            self.logger.error(f"Error syncing customer: {str(e)}")
            raise

    def _sync_promotion(self, promotion_data: dict, restaurant: Restaurant) -> Promotion:
        """Sync promotion data to database."""
        try:
            # Always set externalID to 0 if it's a string that contains text
            if isinstance(promotion_data['ExternalID'], str) and not promotion_data['ExternalID'].isdigit():
                external_id = 0
            else:
                # Only try to convert if it might be a number
                try:
                    external_id = int(promotion_data['ExternalID']) if promotion_data['ExternalID'] else 0
                except (ValueError, TypeError):
                    external_id = 0
            
            self.logger.debug(f"Converting ExternalID '{promotion_data['ExternalID']}' to {external_id}")
            
            promotion = self.session.merge(
                Promotion(
                    id=promotion_data['ID'],
                    companyID=promotion_data['CompanyID'],
                    externalID=external_id,  # Will be 0 for any non-numeric value
                    promotionType=promotion_data['PromotionType'],
                    benefitType=promotion_data['BenefitType'],
                    name=promotion_data['Name'],
                    description=promotion_data['Description'],
                    oncePerCustomer=promotion_data['OncePerCustomer'],
                    onlyFirstOrder=promotion_data['OnlyFirstOrder'],
                    minSubTotal=promotion_data['MinSubTotal'],
                    discountType=promotion_data['DiscountType'],
                    discountAmount=promotion_data['DiscountAmount'],
                    couponCode=promotion_data['CouponCode'],
                    restaurant_id=restaurant.id
                )
            )
            return promotion
        except Exception as e:
            self.logger.error(f"Error syncing promotion: {str(e)}")
            raise

    def _sync_address(self, address_data: dict, restaurant_id: int) -> CustomerAddress:  # Modified
        """Sync customer address data to database."""
        try:
            address = self.session.merge(
                CustomerAddress(
                    id=address_data['ID'],
                    customer_id=address_data['CustomerID'],
                    address_type=address_data['AddressType'],
                    street1=address_data['Street1'],
                    street2=address_data['Street2'],
                    city_town_name=address_data['CityTownName'],
                    postal_code=address_data['PostalCode'],
                    phone=address_data['Phone'],
                    latitude=address_data['Latitude'],
                    longitude=address_data['Longitude'],
                    restaurant_id=restaurant_id  # NEW
                )
            )
            return address
        except Exception as e:
            self.logger.error(f"Error syncing address: {str(e)}")
            raise

    def _sync_order(self, data: dict, restaurant: Restaurant, 
                   customer: Customer, address: Optional[CustomerAddress],
                   promotion_id: Optional[int]) -> Order:
        """Sync order data to database."""
        try:
            order = self.session.merge(
                Order(
                    id=data['ID'],
                    restaurant_id=restaurant.id,
                    customer_id=customer.id,
                    customer_address_id=address.id if address else None,
                    delivery_type=data['DeliveryType'],
                    order_method=data['OrderMethod'],
                    sub_total=data['SubTotal'],
                    delivery_fee=data['DeliveryFee'],
                    service_charge=data['ServiceCharge'],
                    total=data['Total'],
                    status=data['Status'],
                    creation_date=self._parse_date(data['CreationDate']),
                    payment_status=data['PaymentStatus'],
                    number_of_orders=data.get('NumberOfOrders'),
                    phone=data.get('Phone'),
                    order_date=self._parse_date(data.get('OrderDate')),
                    promotion_id=promotion_id,
                    line_item_discount=data.get('LineItemDiscount', 0),
                    discount=data.get('Discount', 0),
                    delivery_option_type=data.get('DeliveryOptionType'),
                    tip=data.get('Tip', 0),
                    used_points=data.get('UsedPoints', 0),
                    total_paid=data.get('TotalPaid', 0),
                    total_balance=data.get('TotalBalance', 0)
                )
            )
            return order
        except Exception as e:
            self.logger.error(f"Error syncing order: {str(e)}")
            raise

    def _sync_payments(self, payments_data: List[dict], order_id: int, restaurant_id: int) -> List[Payment]:  # Modified
        """Sync payment data to database."""
        try:
            payments = []
            for payment_data in payments_data:
                payment = self.session.merge(
                    Payment(
                        id=payment_data['ID'],
                        order_id=payment_data['OrderID'],
                        payment_method_id=payment_data['PaymentMethodID'],
                        payment_method_type=payment_data['PaymentMethodType'],
                        extra_charge=payment_data['ExtraCharge'],
                        sub_total=payment_data['SubTotal'],
                        discount=payment_data.get('Discount', 0),
                        tax=payment_data.get('Tax', 0),
                        amount=payment_data['Amount'],
                        status=payment_data['Status'],
                        tip=payment_data.get('Tip', 0),
                        payment_method_name=payment_data['PaymentMethodName'],
                        restaurant_id=restaurant_id  # NEW
                    )
                )
                payments.append(payment)
            return payments
        except Exception as e:
            self.logger.error(f"Error syncing payments: {str(e)}")
            raise

    def _parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse date string from API response."""
        if not date_str or date_str == "null":
            return None
        if '/Date(' in date_str:
            timestamp = int(date_str.replace('/Date(', '').replace(')/', ''))
            return datetime.fromtimestamp(timestamp/1000)
        return None