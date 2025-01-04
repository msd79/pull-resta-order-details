# File: src/services/datetime_dimension.py

from datetime import datetime, timedelta
from typing import Optional, List
from sqlalchemy.orm import Session
from sqlalchemy import func
import logging
from src.database.dimentional_models import DimDateTime

class DateTimeDimensionService:
    def __init__(self, session: Session):
        self.session = session
        self.logger = logging.getLogger(__name__)
        
        # Configure business hours and day parts
        self.business_hours = {
            'start': 6,  # 6 AM
            'end': 23,   # 11 PM
        }
        
        self.day_parts = {
            'breakfast': {'start': 6, 'end': 11},    # 6 AM - 11 AM
            'lunch': {'start': 11, 'end': 15},       # 11 AM - 3 PM
            'dinner': {'start': 15, 'end': 23},      # 3 PM - 11 PM
        }
        
        self.peak_hours = [
            {'start': 7, 'end': 9},    # Morning peak
            {'start': 12, 'end': 14},  # Lunch peak
            {'start': 18, 'end': 20},  # Dinner peak
        ]

    def generate_datetime_dimension(self, start_date: datetime, end_date: datetime) -> None:
        """
        Generate datetime dimension records for the specified date range at 1-hour intervals.
        Only generates records that don't already exist.
        """
        try:
            self.logger.info(f"Generating datetime dimension from {start_date} to {end_date}")
            
            # Ensure start_date is at the beginning of the hour
            start_date = start_date.replace(minute=0, second=0, microsecond=0)
            
            # Ensure end_date is at the end of the hour
            end_date = end_date.replace(minute=59, second=59, microsecond=999999)
            
            current_date = start_date
            batch_size = 1000
            batch = []
            total_records = 0

            while current_date <= end_date:
                # Generate hourly intervals for the day
                for hour in range(24):
                    current_datetime = current_date.replace(hour=hour, minute=0)
                    
                    # Skip if datetime already exists
                    exists = self.session.query(DimDateTime).filter(
                        DimDateTime.datetime == current_datetime
                    ).first()
                    
                    if not exists:
                        dim_datetime = self._create_datetime_record(current_datetime)
                        batch.append(dim_datetime)
                        total_records += 1
                    
                    if len(batch) >= batch_size:
                        self._save_batch(batch)
                        batch = []
            
                current_date += timedelta(days=1)
            
            # Save any remaining records
            if batch:
                self._save_batch(batch)
                
            self.logger.info(f"Generated {total_records} new datetime records")
                
        except Exception as e:
            self.logger.error(f"Error generating datetime dimension: {str(e)}")
            raise

    def _create_datetime_record(self, dt: datetime) -> DimDateTime:
        """Create a single datetime dimension record."""
        # Convert the date field to datetime to avoid timezone issues
        date_as_datetime = datetime.combine(dt.date(), datetime.min.time())
        
        return DimDateTime(
            datetime=dt,
            date=date_as_datetime,
            year=dt.year,
            quarter=((dt.month - 1) // 3) + 1,
            month=dt.month,
            week=dt.isocalendar()[1],
            day=dt.day,
            hour=dt.hour,
            minute=dt.minute,
            day_of_week=dt.isoweekday(),
            is_weekend=dt.isoweekday() in [5, 6, 7],
            is_holiday=self._is_holiday(dt),
            day_part=self._get_day_part(dt.hour),
            is_peak_hour=self._is_peak_hour(dt.hour),
            is_business_hour=self._is_business_hour(dt.hour),
            fiscal_year=self._get_fiscal_year(dt),
            fiscal_quarter=self._get_fiscal_quarter(dt),
            fiscal_month=self._get_fiscal_month(dt)
        )

    def _save_batch(self, batch: List[DimDateTime]) -> None:
        """Save a batch of datetime records to the database."""
        try:
            self.session.bulk_save_objects(batch)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise Exception(f"Error saving datetime batch: {str(e)}")

    def _get_day_part(self, hour: int) -> str:
        """Determine the part of day (breakfast, lunch, dinner) for a given hour."""
        for day_part, time_range in self.day_parts.items():
            if time_range['start'] <= hour < time_range['end']:
                return day_part
        return 'off_hours'

    def _is_peak_hour(self, hour: int) -> bool:
        """Determine if the given hour is a peak hour."""
        return any(
            peak['start'] <= hour < peak['end']
            for peak in self.peak_hours
        )

    def _is_business_hour(self, hour: int) -> bool:
        """Determine if the given hour is during business hours."""
        return self.business_hours['start'] <= hour < self.business_hours['end']

    def _is_holiday(self, dt: datetime) -> bool:
        """Determine if the given date is a holiday."""
        # TODO: Implement holiday logic based on your needs
        return False

    def _get_fiscal_year(self, dt: datetime) -> int:
        """Get fiscal year for the date (assuming fiscal year starts July 1st)."""
        if dt.month >= 7:
            return dt.year
        return dt.year - 1

    def _get_fiscal_quarter(self, dt: datetime) -> int:
        """Get fiscal quarter for the date."""
        month = dt.month
        if month >= 7:
            return ((month - 7) // 3) + 1
        return ((month + 5) // 3) + 1

    def _get_fiscal_month(self, dt: datetime) -> int:
        """Get fiscal month for the date."""
        if dt.month >= 7:
            return dt.month - 6
        return dt.month + 6

    def get_datetime_key(self, dt: datetime) -> Optional[int]:
        """Get the surrogate key for a given datetime."""
        try:
            if not dt:
                self.logger.error("Received null datetime")
                return None
                
            # Round to nearest hour
            dt = dt.replace(minute=0, second=0, microsecond=0)
            
            self.logger.debug(f"Looking for datetime key for: {dt}")
            
            # Check if we need to generate more historical dates
            earliest_date = self.session.query(func.min(DimDateTime.datetime)).scalar()
            latest_date = self.session.query(func.max(DimDateTime.datetime)).scalar()
            
            if earliest_date and dt < earliest_date:
                self.logger.info(f"Date {dt} is before our current dimension range. Generating more historical dates...")
                new_start = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                self.generate_datetime_dimension(
                    start_date=new_start,
                    end_date=earliest_date
                )
            elif latest_date and dt > latest_date:
                self.logger.info(f"Date {dt} is beyond our current dimension range. Generating more future dates...")
                self.generate_datetime_dimension(
                    start_date=latest_date,
                    end_date=dt + timedelta(days=30)
                )
            
            result = self.session.query(DimDateTime.datetime_key)\
                .filter(DimDateTime.datetime == dt)\
                .first()
                
            if not result:
                self.logger.error(f"No datetime key found for {dt}")
                date_range = self.session.query(
                    func.min(DimDateTime.datetime),
                    func.max(DimDateTime.datetime)
                ).first()
                self.logger.error(f"Current datetime dimension range: {date_range[0]} to {date_range[1]}")
                return None
                
            return result[0]
                
        except Exception as e:
            self.logger.error(f"Error getting datetime key: {str(e)}")
            raise