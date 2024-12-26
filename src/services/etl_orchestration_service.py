from datetime import datetime, timedelta
from typing import Optional
from sqlalchemy.orm import Session
import logging
from src.services.datetime_dimension import DateTimeDimensionService
from src.services.customer_dimension import CustomerDimensionService

class ETLOrchestrator:
    def __init__(self, session: Session):
        self.session = session
        self.logger = logging.getLogger(__name__)
        
        # Initialize dimension services
        self.datetime_service = DateTimeDimensionService(session)
        self.customer_service = CustomerDimensionService(session)
        
    async def initialize_dimensions(self):
        """Initialize all dimension tables with base data."""
        try:
            self.logger.info("Starting dimension initialization...")
            
            # Generate DateTime dimension for next 2 years
            await self._initialize_datetime_dimension()
            
            self.logger.info("Dimension initialization completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error during dimension initialization: {str(e)}")
            raise
    
    async def _initialize_datetime_dimension(self):
        """Initialize the DateTime dimension with a 2-year range."""
        try:
            self.logger.info("Initializing DateTime dimension...")
            
            start_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = start_date + timedelta(days=365*2)  # 2 years of data
            
            self.datetime_service.generate_datetime_dimension(start_date, end_date)
            
            self.logger.info("DateTime dimension initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Error initializing DateTime dimension: {str(e)}")
            raise

    def get_datetime_key(self, dt: datetime) -> Optional[int]:
        """Get datetime surrogate key for fact table population."""
        return self.datetime_service.get_datetime_key(dt)