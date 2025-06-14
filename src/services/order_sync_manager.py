# File location: src/services/order_sync_manager.py
import asyncio
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import logging
from src.api.client import RestaAPI
from src.services.order_sync import OrderSyncService
from src.services.order_tracker_v2 import OrderTrackerServiceV2
from src.services.etl_orchestration_service import ETLOrchestrator
from src.database.models import Order

class OrderSyncManager:
    """
    Manages order synchronization with the new API behavior where latest orders
    appear on page 1.
    """
    def __init__(self, api_client: RestaAPI, session, config):
        self.api_client = api_client
        self.session = session
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Initialize services
        self.order_sync_service = OrderSyncService(session)
        self.order_tracker = OrderTrackerServiceV2(session)
        self.etl_orchestrator = ETLOrchestrator(session)
        
    async def sync_restaurant_orders(self, restaurant_id: int, restaurant_name: str, 
                                   max_pages: Optional[int] = None) -> Dict[str, Any]:
        """
        Sync orders for a restaurant starting from page 1 (latest orders).
        Continues until it finds orders that have already been processed.
        
        Args:
            restaurant_id: The restaurant ID
            restaurant_name: The restaurant name
            max_pages: Maximum number of pages to process (None for unlimited)
            
        Returns:
            Dictionary with sync statistics
        """
        try:
            self.logger.info(f"Starting order sync for {restaurant_name} (ID: {restaurant_id})")
            
            # Get the sync checkpoint
            checkpoint = self.order_tracker.get_sync_checkpoint(restaurant_id, restaurant_name)
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
            
            while True:
                if max_pages and page_index > max_pages:
                    self.logger.info(f"Reached max pages limit ({max_pages})")
                    break
                
                try:
                    self.logger.info(f"Fetching page {page_index}")
                    
                    # Fetch orders from API
                    response = await self.api_client.get_orders_list(page_index)
                    
                    if not response or 'Data' not in response:
                        self.logger.warning(f"No data in response for page {page_index}")
                        break
                    
                    orders_data = response['Data']
                    
                    if not orders_data:
                        self.logger.info(f"No more orders found on page {page_index}")
                        break
                    
                    stats['pages_processed'] += 1
                    page_new_orders = 0
                    
                    # Process orders in the order they appear (latest first)
                    for order_summary in orders_data:
                        try:
                            order_id = order_summary['ID']
                            # Parse order date from the summary
                            order_date = self._parse_date(order_summary.get('CreationDate'))
                            
                            if not order_date:
                                self.logger.warning(f"Order {order_id} has no creation date")
                                continue
                            
                            # Check if we should process this order
                            if not self.order_tracker.should_process_order(order_id, order_date, checkpoint):
                                self.logger.debug(f"Order {order_id} already processed - skipping")
                                stats['duplicate_orders_skipped'] += 1
                                consecutive_old_orders += 1
                                
                                # Stop if we've seen enough old orders
                                if consecutive_old_orders >= stop_threshold:
                                    self.logger.info(f"Found {stop_threshold} consecutive old orders - stopping sync")
                                    return stats
                                continue
                            
                            # Reset counter since we found a new order
                            consecutive_old_orders = 0
                            
                            # Fetch full order details
                            self.logger.debug(f"Fetching details for order {order_id}")
                            order_details = await self.api_client.fetch_order_details(order_id)
                            
                            if not order_details:
                                self.logger.error(f"Failed to fetch details for order {order_id}")
                                stats['errors'].append(f"Failed to fetch order {order_id}")
                                continue
                            
                            # Sync order to database
                            order = self.order_sync_service.sync_order_data(order_details)
                            
                            # Process through ETL pipeline
                            datetime_key = self.etl_orchestrator.get_datetime_key(order.creation_date)
                            if datetime_key:
                                await self.etl_orchestrator.process_order_dimensions_and_facts(order, datetime_key)
                            else:
                                self.logger.warning(f"No datetime key found for order {order_id}")
                            
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
                            
                            # Add delay between orders
                            await asyncio.sleep(self.config.sync.delay_between_orders)
                            
                        except Exception as e:
                            self.logger.error(f"Error processing order {order_id}: {str(e)}")
                            stats['errors'].append(f"Order {order_id}: {str(e)}")
                            continue
                    
                    stats['total_orders_processed'] += len(orders_data)
                    
                    self.logger.info(f"Page {page_index} complete - {page_new_orders} new orders synced")
                    
                    # If no new orders on this page, check a few more pages before stopping
                    if page_new_orders == 0:
                        self.logger.info("No new orders on this page")
                        # Could implement logic to check a few more pages or stop here
                    
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
                self.order_tracker.update_sync_checkpoint(
                    restaurant_id=restaurant_id,
                    last_order_id=most_recent_order_id,
                    last_order_date=most_recent_order_date,
                    orders_synced_count=stats['new_orders_synced']
                )
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Critical error in sync process: {str(e)}")
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

    async def perform_full_resync(self, restaurant_id: int, restaurant_name: str) -> Dict[str, Any]:
        """
        Perform a full resync by resetting the checkpoint and syncing all orders.
        
        Args:
            restaurant_id: The restaurant ID
            restaurant_name: The restaurant name
            
        Returns:
            Dictionary with sync statistics
        """
        self.logger.info(f"Performing full resync for {restaurant_name}")
        
        # Reset the checkpoint
        self.order_tracker.reset_checkpoint(restaurant_id)
        
        # Perform sync
        return await self.sync_restaurant_orders(restaurant_id, restaurant_name)