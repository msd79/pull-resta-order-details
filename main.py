import asyncio
import logging
from typing import List, Dict, Any, Optional
import signal
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta

from src.database.dimentional_models import FactOrders
from src.services.etl_orchestration_service import ETLOrchestrator
from src.config.settings import get_config
from src.database.database import DatabaseManager
from src.api.client import RestaAPI
from src.database.models import Order, User
from src.services.credential_manager import CredentialManagerService
from src.services.order_sync import OrderSyncService
# Replace page tracker with order tracker
from src.services.order_tracker_v2 import OrderTrackerServiceV2
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
    order_tracker: OrderTrackerServiceV2  # Changed from page_tracker
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
            order_tracker = OrderTrackerServiceV2(db_session)  # Changed from PageTrackerService
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
                order_tracker=order_tracker,  # Changed from page_tracker
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

    def _parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse date string from API response."""
        if not date_str or date_str.lower() == "null":
            return None

        try:
            if date_str.startswith('/Date(') and date_str.endswith(')/'):
                timestamp_ms = int(date_str[6:-2])
                return datetime.fromtimestamp(timestamp_ms / 1000)
        except (ValueError, TypeError):
            return None

        return None

    async def _process_restaurant_orders(self, services: ApplicationServices, 
                                       restaurant_id: int, 
                                       restaurant_name: str,
                                       max_pages: Optional[int] = None) -> Dict[str, Any]:
        """
        Process orders for a restaurant using the new order-based tracking approach.
        Always starts from page 1 (latest orders) and continues until finding processed orders.
        """
        try:
            self.logger.info(f"Starting order sync for {restaurant_name} (ID: {restaurant_id})")
            
            # Get the sync checkpoint
            checkpoint = services.order_tracker.get_sync_checkpoint(restaurant_id, restaurant_name)
            if checkpoint:
                last_order_id, last_order_date = checkpoint
                self.logger.info(f"Resuming from checkpoint - Last Order ID: {last_order_id}, "
                               f"Date: {last_order_date}")
            else:
                self.logger.info("No checkpoint found - performing full sync")
            
            # Track sync statistics
            stats = {
                'total_orders_processed': 0,
                'new_orders_synced': 0,
                'duplicate_orders_skipped': 0,
                'pages_processed': 0,
                'errors': [],
                'most_recent_order': None
            }
            
            # Track the most recent order for checkpoint update
            most_recent_order_id = None
            most_recent_order_date = None
            
            page_index = 1
            consecutive_old_orders = 0
            stop_threshold = 10  # Stop after finding 10 consecutive old orders
            checkpoint_reached = False  # Track if we've seen the checkpoint order
            
            while self.state.is_running:
                if max_pages and page_index > max_pages:
                    self.logger.info(f"Reached max pages limit ({max_pages})")
                    break
                
                try:
                    self.logger.info(f"Fetching page {page_index} for {restaurant_name}")
                    
                    # Fetch orders from API
                    response = await services.api_client.get_orders_list(page_index)
                    
                    if not response or 'Data' not in response:
                        self.logger.warning(f"No data in response for page {page_index}")
                        break
                    
                    orders_data = response['Data']
                    
                    if not orders_data:
                        self.logger.info(f"No more orders found on page {page_index}")
                        break
                    
                    stats['pages_processed'] += 1
                    page_new_orders = 0
                    page_failed_orders = 0
                    
                    # Process orders in the order they appear (latest first)
                    for order_summary in orders_data:
                        if not self.state.is_running:
                            break
                            
                        try:
                            order_id = order_summary['ID']
                            # Parse order date from the summary
                            order_date = self._parse_date(order_summary.get('CreationDate'))
                            
                            if not order_date:
                                self.logger.warning(f"Order {order_id} has no creation date")
                                continue
                            
                            # Check if this is our checkpoint order
                            if checkpoint and order_id == checkpoint[0]:
                                self.logger.info(f"Reached checkpoint order {order_id}")
                                checkpoint_reached = True
                            
                            # PRIMARY CHECK 1: Order Tracker Check
                            if not self.config.sync.skip_duplicate_checks:
                                # Check if we should process this order
                                if not services.order_tracker.should_process_order(order_id, order_date, checkpoint):
                                    self.logger.debug(f"Order {order_id} already processed - skipping")
                                    stats['duplicate_orders_skipped'] += 1
                                    self.orders_skipped += 1
                                    
                                    # Only count consecutive old orders AFTER reaching checkpoint
                                    if checkpoint_reached:
                                        consecutive_old_orders += 1
                                        self.logger.debug(f"Old order count after checkpoint: {consecutive_old_orders}")
                                        
                                        # Stop if we've seen enough old orders after checkpoint
                                        if consecutive_old_orders >= stop_threshold:
                                            self.logger.info(f"Found {stop_threshold} consecutive old orders after checkpoint - stopping sync")
                                            # Update checkpoint before returning
                                            if most_recent_order_id and stats['new_orders_synced'] > 0:
                                                services.order_tracker.update_sync_checkpoint(
                                                    restaurant_id=restaurant_id,
                                                    last_order_id=most_recent_order_id,
                                                    last_order_date=most_recent_order_date,
                                                    orders_synced_count=stats['new_orders_synced']
                                                )
                                            return stats
                                    else:
                                        self.logger.debug(f"Found old order {order_id} but haven't reached checkpoint yet - continuing")
                                    continue
                            else:
                                self.logger.warning(f"Duplicate checks disabled via config - processing order {order_id} regardless of checkpoint")
                            
                            # PRIMARY CHECK 2: Database Check
                            if not self.config.sync.skip_duplicate_checks:
                                # Also check if order already exists in database (additional safety check)
                                order_exists_in_order_table = services.sync_service.session.query(Order).filter(Order.id == order_id).first()
                                order_exists_in_fact_table = services.sync_service.session.query(FactOrders).filter(FactOrders.order_id == order_id).first()

                                if order_exists_in_order_table or order_exists_in_fact_table:
                                    self.logger.debug(f"Order {order_id} already exists in database. Skipping...")
                                    self.orders_skipped += 1
                                    stats['duplicate_orders_skipped'] += 1
                                    
                                    # Check if this is our checkpoint order
                                    if checkpoint and order_id == checkpoint[0]:
                                        self.logger.info(f"Reached checkpoint order {order_id}")
                                        checkpoint_reached = True
                                    
                                    # Only count consecutive old orders AFTER reaching checkpoint
                                    if checkpoint_reached:
                                        consecutive_old_orders += 1
                                        self.logger.debug(f"Old order count after checkpoint: {consecutive_old_orders}")
                                        
                                        if consecutive_old_orders >= stop_threshold:
                                            self.logger.info(f"Found {stop_threshold} consecutive old orders after checkpoint - stopping sync")
                                            # Update checkpoint before returning
                                            if most_recent_order_id and stats['new_orders_synced'] > 0:
                                                services.order_tracker.update_sync_checkpoint(
                                                    restaurant_id=restaurant_id,
                                                    last_order_id=most_recent_order_id,
                                                    last_order_date=most_recent_order_date,
                                                    orders_synced_count=stats['new_orders_synced']
                                                )
                                            return stats
                                    else:
                                        self.logger.debug(f"Found existing order {order_id} but haven't reached checkpoint yet - continuing")
                                    continue
                            else:
                                self.logger.warning(f"Database duplicate checks disabled via config - will attempt to process order {order_id}")
                            
                            # Reset counter since we found a new order
                            consecutive_old_orders = 0
                            
                            # Fetch full order details
                            self.logger.debug(f"Fetching details for order {order_id}")
                            order_details = await services.api_client.fetch_order_details(order_id)
                            
                            if not order_details or order_details.get('ErrorCode') != 0:
                                self.logger.error(f"Failed to fetch details for order {order_id}")
                                stats['errors'].append(f"Failed to fetch order {order_id}")
                                page_failed_orders += 1
                                continue
                            
                            # Process the order (sync + ETL)
                            try:
                                await self._process_order(order_details, services, restaurant_name)
                                stats['new_orders_synced'] += 1
                                page_new_orders += 1
                                
                                # Track most recent order
                                if most_recent_order_date is None or order_date > most_recent_order_date:
                                    most_recent_order_id = order_id
                                    most_recent_order_date = order_date
                                    stats['most_recent_order'] = {
                                        'id': order_id,
                                        'date': order_date
                                    }
                                
                            except Exception as order_error:
                                page_failed_orders += 1
                                # Rollback session to clean state after error
                                try:
                                    services.sync_service.session.rollback()
                                except:
                                    pass  # Ignore rollback errors
                                self.logger.error(f"Failed to process order {order_id}: {str(order_error)}")
                                stats['errors'].append(f"Order {order_id}: {str(order_error)}")
                                
                                # Check if it's a data truncation error specifically
                                if "String or binary data would be truncated" in str(order_error):
                                    self.logger.warning(f"Data truncation error for order {order_id} - likely data mapping issue")
                                elif "ProgrammingError" in str(order_error):
                                    self.logger.warning(f"Database programming error for order {order_id} - continuing with next order")
                                
                                # Continue to next order instead of failing
                                continue
                            
                            # Add delay between orders
                            await asyncio.sleep(self.config.sync.delay_between_orders)
                            
                        except Exception as e:
                            self.logger.error(f"Error processing order {order_id}: {str(e)}")
                            stats['errors'].append(f"Order {order_id}: {str(e)}")
                            page_failed_orders += 1
                            continue
                    
                    stats['total_orders_processed'] += len(orders_data)
                    
                    # Log page summary
                    if page_failed_orders > 0:
                        self.logger.warning(f"Page {page_index} completed with {page_failed_orders} failed orders out of {len(orders_data)} total")
                    else:
                        self.logger.info(f"Page {page_index} complete - {page_new_orders} new orders synced")
                    
                    # If no new orders on this page, we might be approaching the end
                    if page_new_orders == 0:
                        self.logger.info("No new orders on this page")
                    
                    # Add delay between pages
                    await asyncio.sleep(self.config.sync.delay_between_pages)
                    page_index += 1
                    
                except Exception as e:
                    self.logger.error(f"Error processing page {page_index}: {str(e)}")
                    stats['errors'].append(f"Page {page_index}: {str(e)}")
                    await asyncio.sleep(self.config.sync.delay_on_error)
                    page_index += 1
            
            # Update checkpoint with most recent order
            if most_recent_order_id and stats['new_orders_synced'] > 0:
                services.order_tracker.update_sync_checkpoint(
                    restaurant_id=restaurant_id,
                    last_order_id=most_recent_order_id,
                    last_order_date=most_recent_order_date,
                    orders_synced_count=stats['new_orders_synced']
                )
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Critical error in sync process: {str(e)}")
            # Ensure session is rolled back
            try:
                services.sync_service.session.rollback()
            except:
                pass
            raise

    async def _process_restaurant(self, restaurant_user: User, services: ApplicationServices):
        """Process orders for a single restaurant"""
        self.logger.info(f"Processing restaurant: {restaurant_user.restaurant_id} - {restaurant_user.company_name}")
        
        try:
            # Store current counters to calculate restaurant-specific counts later
            start_orders_processed = self.orders_processed
            start_orders_etl_processed = self.orders_etl_processed
            start_orders_skipped = self.orders_skipped
            
            credentials = services.credential_manager.get_credential_by_restaurant(restaurant_user.restaurant_id)
            self.logger.debug(f"Retrieved credentials for restaurant {restaurant_user.restaurant_id}")
            
            if not credentials:
                self.logger.error(f"No credentials found for restaurant {restaurant_user.company_name}")
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
                return

            except Exception as e:
                self.logger.error(f"Login error: {e}")
                return
            
            # Process orders using the new order-based tracking approach
            sync_stats = await self._process_restaurant_orders(
                services=services,
                restaurant_id=services.api_client.restaurant_id,
                restaurant_name=services.api_client.restaurant_name
            )
            
            # Calculate restaurant-specific metrics
            restaurant_orders_processed = self.orders_processed - start_orders_processed
            restaurant_orders_etl_processed = self.orders_etl_processed - start_orders_etl_processed
            restaurant_orders_skipped = self.orders_skipped - start_orders_skipped
            
            self.logger.info(f"Restaurant {restaurant_user.company_name} sync complete: "
                            f"processed {restaurant_orders_processed} orders, "
                            f"ETL processed {restaurant_orders_etl_processed} orders, "
                            f"skipped {restaurant_orders_skipped} orders, "
                            f"errors: {len(sync_stats['errors'])}")

        except Exception as e:
            self.logger.error(f"Error processing restaurant {restaurant_user.company_name}: {str(e)}")
            # Rollback session on error
            try:
                services.sync_service.session.rollback()
            except:
                pass  # Ignore rollback errors

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
        
        # Check if duplicate checks are disabled
        if self.config.sync.skip_duplicate_checks:
            self.logger.warning("=" * 60)
            self.logger.warning("WARNING: Duplicate checks are DISABLED in config!")
            self.logger.warning("Orders will be reprocessed even if they already exist.")
            self.logger.warning("Secondary fact table checks will still prevent duplicates.")
            self.logger.warning("=" * 60)

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
                        # Rollback the session to clear any failed transactions
                        try:
                            services.sync_service.session.rollback()
                        except:
                            pass  # Ignore rollback errors
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