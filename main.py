import asyncio
import logging
from typing import List, Dict, Any, Optional
import signal
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime

from src.config.settings import get_config
from src.database.database import DatabaseManager
from src.api.client import RestaAPI
from src.database.models import User
from src.services.credential_manager import CredentialManagerService
from src.services.order_sync import OrderSyncService
from src.services.page_tracker import PageTrackerService
from src.services.schedule_manager import ScheduleManager
from src.utils.logging_config import setup_logging
from src.utils.retry import retry_with_backoff
from src.utils.validation import ValidationUtils

@dataclass
class ApplicationServices:
    """Container for application services"""
    db_manager: DatabaseManager
    api_client: RestaAPI
    sync_service: OrderSyncService
    page_tracker: PageTrackerService
    credential_manager: CredentialManagerService
    schedule_manager: ScheduleManager

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

    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        def signal_handler(signum, frame):
            self.logger.info(f"Received signal {signum}")
            self.state.initiate_shutdown()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    async def _initialize_services(self) -> ApplicationServices:
            """Initialize all application services"""
            try:
                # Previous initialization code remains the same
                db_manager = DatabaseManager(self.config.database.connection_string)
                db_manager.create_tables()

                api_client = RestaAPI(
                    base_url=self.config.api.base_url,
                    page_size=self.config.api.page_size
                )

                db_session = db_manager.get_session()
                sync_service = OrderSyncService(db_session)
                page_tracker = PageTrackerService(db_session)
                credential_manager = CredentialManagerService(db_session, self.config.database.passphrase)

                credential_manager.import_credentials_from_yaml()
                
                # Initialize schedule manager with config values
                schedule_manager = ScheduleManager(
                    start_hour=self.config.schedule.start_hour,
                    start_minute=self.config.schedule.start_minute,
                    end_hour=self.config.schedule.end_hour,
                    end_minute=self.config.schedule.end_minute,
                    active_days=self.config.schedule.active_days
                )

                return ApplicationServices(
                    db_manager=db_manager,
                    api_client=api_client,
                    sync_service=sync_service,
                    page_tracker=page_tracker,
                    credential_manager=credential_manager,
                    schedule_manager=schedule_manager
                )

            except Exception as e:
                self.logger.error(f"Failed to initialize services: {str(e)}")
                raise
    @asynccontextmanager
    async def _service_context(self):
        """Context manager for application services"""
        try:
            services = await self._initialize_services()
            yield services
        finally:
            await self._cleanup_services(services)

    async def _cleanup_services(self, services: ApplicationServices):
        """Cleanup application services"""
        if services:
            try:
                # Add specific cleanup for each service
                await services.api_client.close()
                services.sync_service.close()
                services.page_tracker.close()
                services.db_manager.close()
                services.creadential_manager.close()
            except Exception as e:
                self.logger.error(f"Error during service cleanup: {str(e)}")

    @retry_with_backoff(retries=3, backoff_factor=2)
    async def _process_order(self, order_data: Dict[str, Any], services: ApplicationServices) -> bool:
        """Process a single order with validation and retries"""
        try:
            # if not ValidationUtils.validate_required_fields(order_data, ['ID', 'Restaurant', 'Customer']):
            #     self.logger.error(f"Invalid order data for order {order_data.get('ID')}")
            #     return False

            order =  services.sync_service.sync_order_data(order_data)
            #self.logger.info(f"Successfully processed order {order.id}")
           
            return True

        except Exception as e:
            self.logger.error(f"Error processing order {order_data.get('ID')}: {str(e)}")
            raise

    async def _process_restaurant_page(
        self,
        current_page: int,
        services: ApplicationServices
    ) -> bool:
        """Process a single page of restaurant orders"""
        self.logger.debug("process_restaurant_page...")
        try:
            orders_response = await services.api_client.get_orders_list(current_page)
            
            if not orders_response.get('Data'):
                return False

            for order in orders_response['Data']:
                if not self.state.is_running:
                    return False

                order_details = await services.api_client.fetch_order_details(order['ID'])
                if order_details and order_details.get('ErrorCode') == 0:
                    await self._process_order(order_details, services)
                
                await asyncio.sleep(self.config.sync.delay_between_orders)

            return True

        except Exception as e:
            self.logger.error(f"Error processing page {current_page}: {str(e)}")
            return False

    async def _process_restaurant(self, restaurant_user: User, services: ApplicationServices):
        """Process orders for a single restaurant"""
        self.logger.info(f"Processing orders for restaurant: {restaurant_user.restaurant_id}")
        try:
            
            #creadentials = self.services.creadential_manager.get_credentials_by_restaurant(restaurant.id)
            credentials = services.credential_manager.get_credential_by_restaurant(restaurant_user.restaurant_id)
            self.logger.debug(f"Credentials: {credentials}")
            if not credentials:
                self.logger.error(f"No credentials found for restaurant {restaurant_user.name}")
                return
            
            # Login with restaurant credentials
            session_token, company_id = await services.api_client.login(
                email=credentials.get('username'),
                password=credentials.get('password')
            )
            
            current_page = services.page_tracker.get_last_page_index(
                company_id=company_id,
                company_name=restaurant_user.company_name
            )

            while self.state.is_running:
                self.logger.info(f"Current page index for {restaurant_user.company_name} - {current_page}")
                
                has_more_pages = await self._process_restaurant_page(
                    current_page, services
                )
                
                if not has_more_pages:
                    break
                
                services.page_tracker.update_page_index(company_id, current_page)
                current_page += 1
                await asyncio.sleep(self.config.sync.delay_between_pages)

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
                log_dir=self.config.logging.filename,  # Updated to use correct path
                log_level=getattr(logging, self.config.logging.level)  # Convert string to log level
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

            #import credentials from yaml
            

            async with self._service_context() as services:
                # Check if we should start immediately
                if not services.schedule_manager.should_start_immediately():
                    wait_time = services.schedule_manager.time_until_next_window()
                    self.logger.info(f"Outside of scheduled running hours. Waiting for {wait_time/3600:.2f} hours until next window")
                    await asyncio.sleep(min(wait_time, 3600))  # Sleep for wait_time or 1 hour, whichever is shorter
                else:
                    self.logger.info("Within scheduled window - starting immediately")

                while self.state.is_running:
                    try:
                        # Check if we're within the scheduled running window
                        if not services.schedule_manager.is_within_schedule():
                            wait_time = services.schedule_manager.time_until_next_window()
                            self.logger.info(f"Outside of scheduled running hours. Waiting for {wait_time/3600:.2f} hours until next window")
                            await asyncio.sleep(min(wait_time, 3600))  # Sleep for wait_time or 1 hour, whichever is shorter
                            continue

                        # Rest of the original run loop
                        services.credential_manager.import_credentials_from_yaml()
                        restaurant_users = services.credential_manager.list_credentials()
                        
                        for restaurant_user in restaurant_users:
                            if not self.state.is_running:
                                break
                            
                            # Check schedule again before processing each restaurant
                            if not services.schedule_manager.is_within_schedule():
                                break
                                
                            await self._process_restaurant(restaurant_user, services)

                        if self.state.is_running:
                            self.logger.info("Completed sync cycle. Waiting before next cycle...")
                            await asyncio.sleep(self.config.sync.polling_interval)

                    except Exception as e:
                        self.logger.error(f"Error in main sync loop: {str(e)}")
                        await asyncio.sleep(self.config.sync.delay_on_error)

            self.logger.info("Application shutdown complete")
def main():
    """Application entry point"""
    try:
        app = OrderSyncApplication()
        asyncio.run(app.run())
    except KeyboardInterrupt:
        logging.info("Application terminated by user")
    except Exception as e:
        logging.error(f"Unhandled exception: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()