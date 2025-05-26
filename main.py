import asyncio
import logging
from typing import List, Dict, Any, Optional
import signal
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone

from src.database.dimentional_models import FactOrders
from src.services.etl_orchestration_service import ETLOrchestrator
from src.config.settings import get_config
from src.database.database import DatabaseManager
from src.api.client import RestaAPI
from src.database.models import Order, User
from src.services.credential_manager import CredentialManagerService
from src.services.order_sync import OrderSyncService
from src.services.page_tracker import PageTrackerService
from src.services.schedule_manager import ScheduleManager
from src.utils.logging_config import setup_logging
from src.utils.retry import retry_with_backoff
from src.utils.validation import ValidationUtils
from src.database.models import Customer
import asyncio


@dataclass
class ApplicationServices:
    """Container for application services"""
    db_manager: DatabaseManager
    api_client: RestaAPI
    sync_service: OrderSyncService
    page_tracker: PageTrackerService
    credential_manager: CredentialManagerService
    schedule_manager: ScheduleManager
    etl_orchestrator: ETLOrchestrator

class ApplicationState:
    """Manages application state and lifecycle"""
    def __init__(self):
        self.is_running: bool = True
        self.is_shutting_down: bool = False
        self.logger = logging.getLogger(__name__)

    def initiate_shutdown(self):
        """Initiates graceful shutdown"""
        self.logger.info("Initiating graceful shutdown...")
        self.is_running = False
        self.is_shutting_down = True

class OrderSyncApplication:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.state = ApplicationState()
        self.config = None
        self.services: Optional[ApplicationServices] = None
        # Add counters for summary logging
        self.orders_processed = 0
        self.orders_etl_processed = 0
        self.orders_skipped = 0

    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        def signal_handler(signum, frame):
            self.logger.info(f"Received signal {signum}")
            self.state.initiate_shutdown()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    async def _initialize_dimensional_model(self, services: ApplicationServices) -> None:
        """Initialize the dimensional model tables and base data."""
        try:
            self.logger.info("Initializing dimensional model...")
            
            # Initialize dimensions using the ETL orchestrator
            await services.etl_orchestrator.initialize_dimensions()
            
            self.logger.info("Dimensional model initialization complete")
            
        except Exception as e:
            self.logger.error(f"Error initializing dimensional model: {str(e)}")
            raise

    
    async def _initialize_services(self) -> ApplicationServices:
        """Initialize all application services"""
        try:
            # Database initialization
            self.logger.debug("Initializing database manager...")
            db_manager = DatabaseManager(self.config.database.connection_string)
            db_manager.create_tables()

            # Get database session
            db_session = db_manager.get_session()
            self.logger.debug("Database session created")

            # Initialize all services
            self.logger.debug("Initializing application services...")
            sync_service = OrderSyncService(db_session)
            page_tracker = PageTrackerService(db_session)
            credential_manager = CredentialManagerService(db_session, self.config.database.passphrase)
            
            # Initialize schedule manager
            schedule_manager = ScheduleManager(
                start_hour=self.config.schedule.start_hour,
                start_minute=self.config.schedule.start_minute,
                end_hour=self.config.schedule.end_hour,
                end_minute=self.config.schedule.end_minute,
                active_days=self.config.schedule.active_days
            )

            # Initialize ETL orchestrator
            etl_orchestrator = ETLOrchestrator(db_session)

            # Initialize API client
            api_client = RestaAPI(
                base_url=self.config.api.base_url,
                page_size=self.config.api.page_size
            )
            
            self.logger.debug("All services initialized successfully")

            return ApplicationServices(
                db_manager=db_manager,
                api_client=api_client,
                sync_service=sync_service,
                page_tracker=page_tracker,
                credential_manager=credential_manager,
                schedule_manager=schedule_manager,
                etl_orchestrator=etl_orchestrator
            )

        except Exception as e:
            self.logger.error(f"Failed to initialize services: {str(e)}")
            raise

    async def _cleanup_services(self, services: ApplicationServices):
        """Cleanup application services"""
        if services:
            try:
                self.logger.debug("Starting service cleanup...")
                # Add specific cleanup for each service
                await services.api_client.close()
                self.logger.debug("Service cleanup completed")
                # Note: Remove services.sync_service.close() if it doesn't exist
                # Note: Remove services.page_tracker.close() if it doesn't exist
                # Note: Remove services.credential_manager.close() if it doesn't exist
            except Exception as e:
                self.logger.error(f"Error during service cleanup: {str(e)}")

    @asynccontextmanager
    async def _service_context(self):
        """Context manager for application services"""
        services = None
        try:
            services = await self._initialize_services()
            yield services
        finally:
            if services:
                await self._cleanup_services(services)

 
    async def _process_etl(self, order: Order, services: ApplicationServices) -> bool:
        """Handle ETL processing for an order"""
        try:
            self.logger.debug(f"Starting ETL process for order {order.id}")
            
            # Get datetime key
            datetime_key = services.etl_orchestrator.get_datetime_key(order.creation_date)
            if not datetime_key:
                self.logger.error(f"Could not get datetime key for order {order.id}")
                return False
                
            self.logger.debug(f"Got datetime key {datetime_key}")
            
            # Process dimensions and facts
            self.logger.debug(f"Processing dimensions and facts for order {order.id}")
            await services.etl_orchestrator.process_order_dimensions_and_facts(
                order=order,
                datetime_key=datetime_key
            )
            self.logger.debug(f"ETL process completed for order {order.id}")
            self.orders_etl_processed += 1
            return True
            
        except Exception as e:
            self.logger.error(f"Failed ETL processing for order {order.id}: {str(e)}", exc_info=True)
            return False
    @retry_with_backoff(retries=3, backoff_factor=2)
    async def _process_order(self, order_data: Dict[str, Any], services: ApplicationServices, restaurant_name: str = None) -> bool:
        """Process a single order with validation and retries"""
        restaurant_context = f" for {restaurant_name}" if restaurant_name else ""
        
        try:
            # First, sync to OLTP model
            order_id = order_data.get('ID')
            self.logger.debug(f"Starting OLTP sync for order {order_id}{restaurant_context}")
            order = services.sync_service.sync_order_data(order_data)
            
            
            if order.creation_date < datetime(2020, 1, 1):
                self.logger.debug(f"Order {order.id}{restaurant_context} was created before 01/01/2020. Skipping ETL processing...")
                self.orders_skipped += 1
                return False
            self.orders_processed += 1
            # Then do ETL processing
            etl_success = await self._process_etl(order, services)
            if not etl_success:
                self.logger.warning(f"ETL processing failed for order {order.id}{restaurant_context}")
                
            # Finally update customer dimension
            try:
                customer = services.sync_service.session.query(Customer).filter(
                    Customer.id == order.customer_id
                ).first()
                
                if customer:
                    self.logger.debug(f"Updating customer dimension for customer {customer.id}{restaurant_context}")
                    restaurant_key = services.api_client.restaurant_id
                    services.etl_orchestrator.customer_service.update_customer_dimension(customer, restaurant_key)
                    self.logger.debug(f"Customer dimension updated for customer {customer.id}{restaurant_context}")
                else:
                    self.logger.error(f"Could not find customer record for ID {order.customer_id}{restaurant_context}")
            except Exception as e:
                self.logger.error(f"Error updating customer dimension for order {order.id}{restaurant_context}: {str(e)}", exc_info=True)

            return True

        except Exception as e:
            self.logger.error(f"Error processing order {order_data.get('ID', 'unknown')}{restaurant_context}: {str(e)}", exc_info=True)
            raise
    async def _process_restaurant_page(
        self,
        current_page: int,
        services: ApplicationServices,
        restaurant_name: str = None
    ) -> bool:
        """Process a single page of restaurant orders"""
        restaurant_context = f" for {restaurant_name}" if restaurant_name else ""
        self.logger.debug(f"Processing restaurant page {current_page}{restaurant_context}...")
        try:
            orders_response = await services.api_client.get_orders_list(current_page)
            
            if not orders_response.get('Data'):
                self.logger.debug(f"No data found on page {current_page}{restaurant_context}")
                return False

            order_count = len(orders_response['Data'])
            self.logger.debug(f"Found {order_count} orders on page {current_page}{restaurant_context}")

            # Track failed orders for this page
            failed_orders = 0
            
            for order in orders_response['Data']:
                if not self.state.is_running:
                    return False
                    
                try:
                    # Check if order exists in either Order table or fact_orders table
                    order_exists_in_order_table = services.sync_service.session.query(Order).filter(Order.id == order['ID']).first()
                    order_exists_in_fact_table = services.sync_service.session.query(FactOrders).filter(FactOrders.order_id == order['ID']).first()

                    if order_exists_in_order_table or order_exists_in_fact_table:
                        self.logger.debug(f"Order {order['ID']} already exists in the database{restaurant_context}. Skipping...")
                        self.orders_skipped += 1
                        continue

                    order_details = await services.api_client.fetch_order_details(order['ID'])
                    if order_details and order_details.get('ErrorCode') == 0:
                        # Wrap the order processing in a try-catch to handle individual order failures
                        try:
                            await self._process_order(order_details, services, restaurant_name)
                            self.logger.debug(f"Successfully processed order {order['ID']}{restaurant_context}")
                        except Exception as order_error:
                            failed_orders += 1
                            # Rollback session to clean state after error
                            try:
                                services.sync_service.session.rollback()
                            except:
                                pass  # Ignore rollback errors
                            self.logger.error(f"Failed to process order {order['ID']}{restaurant_context}: {str(order_error)}")
                            
                            # Check if it's a data truncation error specifically
                            if "String or binary data would be truncated" in str(order_error):
                                self.logger.warning(f"Data truncation error for order {order['ID']}{restaurant_context} - likely data mapping issue")
                            elif "ProgrammingError" in str(order_error):
                                self.logger.warning(f"Database programming error for order {order['ID']}{restaurant_context} - continuing with next order")
                            
                            # Continue to next order instead of failing the entire page
                            continue
                    else:
                        self.logger.warning(f"Could not fetch details for order {order['ID']}{restaurant_context} or API error occurred")
                    
                    await asyncio.sleep(self.config.sync.delay_between_orders)
                    
                except Exception as individual_order_error:
                    failed_orders += 1
                    self.logger.error(f"Unexpected error processing order {order.get('ID', 'unknown')}{restaurant_context}: {str(individual_order_error)}")
                    # Continue to next order
                    continue

            # Log summary for this page
            successful_orders = order_count - failed_orders
            if failed_orders > 0:
                self.logger.warning(f"Page {current_page}{restaurant_context} completed with {failed_orders} failed orders out of {order_count} total orders")
            else:
                self.logger.debug(f"Page {current_page}{restaurant_context} completed successfully with {successful_orders} orders processed")

            return True

        except Exception as e:
            self.logger.error(f"Error processing page {current_page}{restaurant_context}: {str(e)}")
            return False
    async def _process_restaurant(self, restaurant_user: User, services: ApplicationServices):
        """Process orders for a single restaurant"""
        self.logger.info(f"Processing restaurant: {restaurant_user.restaurant_id} - {restaurant_user.company_name}")
        
        # Reset counters for this restaurant
        restaurant_orders_processed = 0
        restaurant_orders_etl_processed = 0
        restaurant_orders_skipped = 0
        
        try:
            # Store current counters to calculate restaurant-specific counts later
            start_orders_processed = self.orders_processed
            start_orders_etl_processed = self.orders_etl_processed
            start_orders_skipped = self.orders_skipped
            
            credentials = services.credential_manager.get_credential_by_restaurant(restaurant_user.restaurant_id)
            self.logger.debug(f"Retrieved credentials for restaurant {restaurant_user.restaurant_id}")
            
            if not credentials:
                self.logger.error(f"No credentials found for restaurant {restaurant_user.name}")
                return
            
            # Login with restaurant credentials
            try:
                self.logger.debug(f"Logging in to restaurant API for {restaurant_user.restaurant_id}")
                session_token, company_id = await services.api_client.login(
                    email=credentials.get('username', ''),
                    password=credentials.get('password', '')
                )

                if not session_token:
                    raise ValueError("Login failed: No session token returned.")
                
                self.logger.debug(f"Login successful for restaurant {restaurant_user.restaurant_id}")

            except KeyError as e:
                self.logger.error(f"Missing credential key: {e}")
                return None

            except Exception as e:
                self.logger.error(f"Login error: {e}")
                return None
            
            current_page = services.page_tracker.get_last_page_index(
                restaurant_id=services.api_client.restaurant_id,
                restaurant_name=services.api_client.restaurant_name
            )
            
            self.logger.debug(f"Starting sync from page {current_page} for restaurant {restaurant_user.restaurant_id}")

            while self.state.is_running:
                self.logger.debug(f"Processing page {current_page} for {restaurant_user.company_name}")
                
                has_more_pages = await self._process_restaurant_page(
                    current_page, services, restaurant_user.company_name
                )
                
                if not has_more_pages:
                    self.logger.debug(f"No more pages for restaurant {restaurant_user.restaurant_id}")
                    break
                
                services.page_tracker.update_page_index(services.api_client.restaurant_id, current_page)
                current_page += 1
                await asyncio.sleep(self.config.sync.delay_between_pages)
            
            # Calculate restaurant-specific metrics
            restaurant_orders_processed = self.orders_processed - start_orders_processed
            restaurant_orders_etl_processed = self.orders_etl_processed - start_orders_etl_processed
            restaurant_orders_skipped = self.orders_skipped - start_orders_skipped
            
            self.logger.info(f"Restaurant {restaurant_user.company_name} sync complete: "
                            f"processed {restaurant_orders_processed} orders, "
                            f"ETL processed {restaurant_orders_etl_processed} orders, "
                            f"skipped {restaurant_orders_skipped} orders")

        except Exception as e:
            self.logger.error(f"Error processing restaurant {restaurant_user.company_name}: {str(e)}")

    async def initialize(self) -> bool:
        """Initialize application configuration and logging"""
        try:
            self.logger.debug("Starting application initialization...")
            
            # Load configuration
            self.logger.debug("Loading configuration...")
            self.config = get_config()
        
            # Setup logging
            self.logger.debug("Setting up logging...")
            setup_logging(
                log_dir="logs",
                log_level=getattr(logging, self.config.logging.level)
            )
            
            # Setup signal handlers
            self.logger.debug("Setting up signal handlers...")
            self._setup_signal_handlers()

            self.logger.info("Application initialized successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to initialize application: {str(e)}")
            import traceback
            self.logger.error(f"Initialization error traceback: {traceback.format_exc()}")
            return False

    async def run(self):
        """Main application loop"""
        if not await self.initialize():
            self.logger.error("Failed to initialize application. Exiting.")
            return

        self.logger.info("Starting order synchronization process...")

        async with self._service_context() as services:
            try:
                # Initialize dimensional model before starting sync process
                await self._initialize_dimensional_model(services)

                # Check if we should start immediately
                if not services.schedule_manager.should_start_immediately():
                    wait_time = services.schedule_manager.time_until_next_window()
                    self.logger.info(f"Outside of scheduled running hours. Waiting for {wait_time/3600:.2f} hours until next window")
                    await asyncio.sleep(min(wait_time, 3600))
                else:
                    self.logger.info("Within scheduled window - starting immediately")

                cycle_count = 0
                
                while self.state.is_running:
                    try:
                        # Reset counters for this cycle
                        cycle_start_orders = self.orders_processed
                        cycle_start_etl_orders = self.orders_etl_processed
                        cycle_start_skipped = self.orders_skipped
                        
                        cycle_count += 1
                        self.logger.info(f"Starting sync cycle #{cycle_count}")
                        
                        if not services.schedule_manager.is_within_schedule():
                            wait_time = services.schedule_manager.time_until_next_window()
                            self.logger.info(f"Outside of scheduled running hours. Waiting for {wait_time/3600:.2f} hours until next window")
                            await asyncio.sleep(min(wait_time, 3600))
                            continue

                        self.logger.debug("Importing credentials from YAML")
                        services.credential_manager.import_credentials_from_yaml()
                        
                        self.logger.debug("Retrieving restaurant credentials list")
                        restaurant_users = services.credential_manager.list_credentials()
                        self.logger.info(f"Found {len(restaurant_users)} restaurants to process")
                        
                        for restaurant_user in restaurant_users:
                            if not self.state.is_running:
                                break
                            
                            if not services.schedule_manager.is_within_schedule():
                                break
                                
                            await self._process_restaurant(restaurant_user, services)

                        # Calculate cycle statistics
                        cycle_orders = self.orders_processed - cycle_start_orders
                        cycle_etl_orders = self.orders_etl_processed - cycle_start_etl_orders
                        cycle_skipped = self.orders_skipped - cycle_start_skipped
                        
                        self.logger.info(f"Completed sync cycle #{cycle_count}: "
                                        f"processed {cycle_orders} orders, "
                                        f"ETL processed {cycle_etl_orders} orders, "
                                        f"skipped {cycle_skipped} orders")
                        
                        if self.state.is_running:
                            self.logger.info(f"Waiting {self.config.sync.polling_interval}s before next cycle...")
                            await asyncio.sleep(self.config.sync.polling_interval)

                    except Exception as e:
                        self.logger.error(f"Error in main sync loop: {str(e)}")
                        await asyncio.sleep(self.config.sync.delay_on_error)

            except Exception as e:
                self.logger.error(f"Error during application run: {str(e)}")
                raise

        self.logger.info(f"Application shutdown complete. Total statistics: "
                         f"processed {self.orders_processed} orders, "
                         f"ETL processed {self.orders_etl_processed} orders, "
                         f"skipped {self.orders_skipped} orders")


def main():
    """Application entry point"""
    try:
        app = OrderSyncApplication()
        # Run the async application
        asyncio.run(app.run())
    except KeyboardInterrupt:
        logging.info("Application terminated by user")
    except Exception as e:
        logging.error(f"Unhandled exception: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()