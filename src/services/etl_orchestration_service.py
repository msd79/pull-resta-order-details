# File: src/services/etl_orchestration_service.py
from datetime import datetime, timedelta
from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy import func
import logging
from src.services.fact_population_service import FactPopulationService
from src.services.payment_method_dimension import PaymentMethodDimensionService
from src.services.promotion_dimension import PromotionDimensionService
from src.services.restaurant_dimension import RestaurantDimensionService
from src.database.models import Order, Payment, Promotion, Restaurant
from src.services.datetime_dimension import DateTimeDimensionService
from src.services.customer_dimension import CustomerDimensionService
from src.database.dimentional_models import DimCustomer, DimDateTime, DimPaymentMethod, DimPromotion, DimRestaurant

class ETLOrchestrator:
    def __init__(self, session: Session):
        self.session = session
        self.logger = logging.getLogger(__name__)
        
        # Initialize dimension services
        # Initialize all dimension services
        self.datetime_service = DateTimeDimensionService(session)
        self.customer_service = CustomerDimensionService(session)
        self.restaurant_service = RestaurantDimensionService(session)
        self.promotion_service = PromotionDimensionService(session)
        self.payment_method_service = PaymentMethodDimensionService(session)
        self.fact_service = FactPopulationService(session)

    async def initialize_dimensions(self):
        """Initialize all dimension tables with base data."""
        try:
            self.logger.info("Starting dimension initialization...")
            
            # Initialize DateTime dimension
            await self._initialize_datetime_dimension()
            
            self.logger.info("Dimension initialization completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error during dimension initialization: {str(e)}")
            raise

    async def _initialize_datetime_dimension(self):
        """Initialize the DateTime dimension with an extended range to cover historical data."""
        try:
            self.logger.info("Checking DateTime dimension...")
            
            # Check if we have any records
            record_count = self.session.query(func.count(DimDateTime.datetime_key)).scalar()
            
            if record_count == 0:
                self.logger.info("DateTime dimension is empty. Initializing with base data...")
                # Set start date to beginning of 2020 to cover historical orders
                start_date = datetime(2020, 1, 1)
                # Set end date to one year in the future from current date
                end_date = datetime.now() + timedelta(days=365)
                
                self.logger.info(f"Generating datetime records from {start_date} to {end_date}")
                self.datetime_service.generate_datetime_dimension(start_date, end_date)
                
                self.logger.info("DateTime dimension initialized successfully")
            else:
                # Check if we have sufficient date range coverage
                date_range = self.session.query(
                    func.min(DimDateTime.datetime),
                    func.max(DimDateTime.datetime)
                ).first()
                
                current_date = datetime.now()
                
                if date_range[0] and date_range[1]:
                    self.logger.info(f"DateTime dimension already populated with range: {date_range[0]} to {date_range[1]}")
                    
                    # Generate more future dates if needed
                    if date_range[1] < current_date + timedelta(days=30):
                        self.logger.info("Extending future date coverage...")
                        self.datetime_service.generate_datetime_dimension(
                            start_date=date_range[1],
                            end_date=current_date + timedelta(days=365)
                        )
                    
                    # Generate more historical dates if needed
                    historical_start = datetime(2020, 1, 1)
                    if date_range[0] > historical_start:
                        self.logger.info("Extending historical date coverage...")
                        self.datetime_service.generate_datetime_dimension(
                            start_date=historical_start,
                            end_date=date_range[0]
                        )
                
        except Exception as e:
            self.logger.error(f"Error initializing DateTime dimension: {str(e)}")
            raise

    def get_datetime_key(self, dt: datetime) -> Optional[int]:
        """Get datetime surrogate key for fact table population."""
        return self.datetime_service.get_datetime_key(dt)
    
    
    async def process_order_dimensions_and_facts(self, order: Order, datetime_key: int) -> None:
        """Process all dimensions and facts for an order."""
        self.logger.info(f"Starting ETL process for order {order.id}")
        try:
            # 1. Process Restaurant Dimension - Query for restaurant first
            self.logger.info(f"Processing restaurant dimension for order {order.id}")
            restaurant = self.session.query(Restaurant).filter_by(id=order.restaurant_id).first()
            if not restaurant:
                raise ValueError(f"Failed to find restaurant with id {order.restaurant_id}")
                
            restaurant_key = self.restaurant_service.update_restaurant_dimension(restaurant)
            self.session.flush()
            if not restaurant_key:
                raise ValueError(f"Failed to get restaurant key for {restaurant.id}")


            # 2. Get Customer Key
            self.logger.info(f"Getting customer key for order {order.id}")
            customer_key = self._get_customer_key(order.customer_id)
            if not customer_key:
                raise ValueError(f"Failed to get customer key for {order.customer_id}")

            # 3. Process Promotion if exists
            promotion_key = None
            if order.promotion_id:
                self.logger.info(f"Processing promotion for order {order.id}")
                promotion = self.session.query(Promotion).get(order.promotion_id)
                if promotion:
                    promotion_key = self.promotion_service.update_promotion_dimension(promotion)
                self.session.flush()

            # 4. Populate fact_orders
            self.logger.info(f"Populating fact_orders for order {order.id}")
            self.fact_service.populate_fact_orders(
                order=order,
                datetime_key=datetime_key,
                customer_key=customer_key,
                restaurant_key=restaurant_key,
                promotion_key=promotion_key
            )

            # 5. Process Payments
            payments = self.session.query(Payment).filter_by(order_id=order.id).all()
            self.logger.info(f"Processing {len(payments)} payments for order {order.id}")
            
            for payment in payments:
                try:
                    # Process payment method dimension
                    payment_method_key = self.payment_method_service.update_payment_method_dimension(payment)
                    self.session.flush()

                    # Populate fact_payments
                    self.fact_service.populate_fact_payments(
                        payment=payment,
                        order_key=order.id,
                        datetime_key=datetime_key,
                        payment_method_key=payment_method_key
                    )
                except Exception as payment_error:
                    self.logger.error(f"Failed to process payment {payment.id}: {str(payment_error)}")

            # Commit all changes at once
            self.session.commit()
            self.logger.info(f"Successfully completed ETL process for order {order.id}")

        except Exception as e:
            self.logger.error(f"Failed ETL process for order {order.id}: {str(e)}", exc_info=True)
            self.session.rollback()
            raise


    def _get_restaurant_key(self, restaurant_id: int) -> int:
        result = self.session.query(DimRestaurant.restaurant_key)\
            .filter_by(restaurant_id=restaurant_id, is_current=True)\
            .first()
        return result[0] if result else None

    def _get_customer_key(self, customer_id: int) -> int:
        result = self.session.query(DimCustomer.customer_key)\
            .filter_by(customer_id=customer_id, is_current=True)\
            .first()
        if not result:
            self.logger.error(f"No customer dimension record found for customer_id {customer_id}")
            # Create the customer dimension record here
            customer = self.session.query(Customer).get(customer_id)
            if customer:
                self.customer_service.update_customer_dimension(customer)
                # Try again to get the key
                result = self.session.query(DimCustomer.customer_key)\
                    .filter_by(customer_id=customer_id, is_current=True)\
                    .first()
        return result[0] if result else None

    def _get_promotion_key(self, promotion_id: int) -> int:
        result = self.session.query(DimPromotion.promotion_key)\
            .filter_by(promotion_id=promotion_id)\
            .first()
        return result[0] if result else None

    def _get_payment_method_key(self, payment_method_id: int) -> int:
        result = self.session.query(DimPaymentMethod.payment_method_key)\
            .filter_by(payment_method_id=payment_method_id)\
            .first()
        return result[0] if result else None

    def _calculate_daily_customer_metrics(self, customer_id: int, order_date: datetime) -> dict:
        # Calculate daily metrics for the customer
        start_of_day = order_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = order_date.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        daily_orders = self.session.query(Order)\
            .filter(
                Order.customer_id == customer_id,
                Order.creation_date.between(start_of_day, end_of_day)
            ).all()

        daily_metrics = {
            'daily_orders': len(daily_orders),
            'daily_spend': sum(order.total for order in daily_orders),
            'daily_items': sum(order.number_of_orders for order in daily_orders),
            'points_used': sum(order.used_points for order in daily_orders),
            # Add other metrics calculations as needed
        }

        return daily_metrics