# File: src/services/etl_orchestration_service.py
from datetime import datetime, timedelta
from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy import func
import logging
from src.services.datetime_dimension import DateTimeDimensionService
from src.services.customer_dimension import CustomerDimensionService
from src.database.dimentional_models import DimDateTime

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