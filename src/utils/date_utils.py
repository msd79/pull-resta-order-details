# File location: src/utils/date_utils.py
from datetime import datetime, timezone
from typing import Optional
import logging

logger = logging.getLogger(__name__)

def parse_unix_timestamp(timestamp_str):
    try:
        timestamp = int(timestamp_str.strip('/Date()/')) // 1000
        return datetime.fromtimestamp(timestamp, datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        logger.error(f"Error parsing timestamp {timestamp_str}: {e}")
        return None

class DateUtils:
    @staticmethod
    def parse_date(date_str: Optional[str]) -> Optional[datetime]:
        """
        Parse various date formats from API responses.
        
        Args:
            date_str: String containing date information
            
        Returns:
            datetime object or None if parsing fails
        """
        if not date_str or date_str == "null":
            return None
            
        try:
            # Handle .NET JSON Date format
            if '/Date(' in date_str:
                timestamp = int(date_str.replace('/Date(', '').replace(')/', ''))
                return datetime.fromtimestamp(timestamp/1000, timezone.utc)
                
            # Handle ISO format
            if 'T' in date_str:
                return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                
            # Handle standard date format
            return datetime.strptime(date_str, '%Y-%m-%d')
            
        except Exception as e:
            logger.error(f"Error parsing date {date_str}: {str(e)}")
            return None

    @staticmethod
    def format_date(date: Optional[datetime], format_str: str = '%Y-%m-%d %H:%M:%S') -> Optional[str]:
        """
        Format datetime object to string.
        
        Args:
            date: datetime object to format
            format_str: desired output format
            
        Returns:
            Formatted date string or None
        """
        if not date:
            return None
            
        try:
            return date.strftime(format_str)
        except Exception as e:
            logger.error(f"Error formatting date {date}: {str(e)}")
            return None
