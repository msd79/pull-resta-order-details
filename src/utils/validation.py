# File location: src/utils/validation.py
# src/utils/validation.py
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class ValidationUtils:
    @staticmethod
    def validate_required_fields(data: Dict[str, Any], required_fields: list) -> bool:
        """
        Validate that all required fields are present and not None.
        
        Args:
            data: Dictionary containing data to validate
            required_fields: List of required field names
            
        Returns:
            bool: True if all required fields are present and not None
        """
        try:
            for field in required_fields:
                if field not in data or data[field] is None:
                    logger.error(f"Missing required field: {field}")
                    return False
            return True
        except Exception as e:
            logger.error(f"Error during field validation: {str(e)}")
            return False

    @staticmethod
    def validate_numeric_field(value: Any, field_name: str, 
                             min_value: Optional[float] = None,
                             max_value: Optional[float] = None) -> bool:
        """
        Validate numeric field values.
        
        Args:
            value: Value to validate
            field_name: Name of the field (for logging)
            min_value: Optional minimum allowed value
            max_value: Optional maximum allowed value
            
        Returns:
            bool: True if validation passes
        """
        try:
            if not isinstance(value, (int, float)):
                logger.error(f"Field {field_name} must be numeric")
                return False
                
            if min_value is not None and value < min_value:
                logger.error(f"Field {field_name} below minimum value {min_value}")
                return False
                
            if max_value is not None and value > max_value:
                logger.error(f"Field {field_name} above maximum value {max_value}")
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Error validating numeric field {field_name}: {str(e)}")
            return False

# src/utils/retry.py
import time
from functools import wraps
from typing import Callable, Optional, Type
import logging

logger = logging.getLogger(__name__)

def retry_with_backoff(
    retries: int = 3,
    backoff_factor: float = 1.5,
    exceptions: tuple = (Exception,),
    on_retry: Optional[Callable] = None
):
    """
    Decorator for retrying functions with exponential backoff.
    
    Args:
        retries: Maximum number of retries
        backoff_factor: Multiplier for backoff time
        exceptions: Tuple of exceptions to catch and retry
        on_retry: Optional callback function to execute before retry
        
    Returns:
        Decorator function
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retry_count = 0
            wait_time = 1  # Initial wait time in seconds
            
            while retry_count < retries:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    retry_count += 1
                    
                    if retry_count == retries:
                        logger.error(
                            f"Max retries ({retries}) reached for {func.__name__}"
                        )
                        raise
                        
                    logger.warning(
                        f"Retry {retry_count}/{retries} for {func.__name__} "
                        f"after error: {str(e)}"
                    )
                    
                    if on_retry:
                        on_retry(retry_count, e)
                    
                    time.sleep(wait_time)
                    wait_time *= backoff_factor
                    
            return None  # Should never reach here
        return wrapper
    return decorator