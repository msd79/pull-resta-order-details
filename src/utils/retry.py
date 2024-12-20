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