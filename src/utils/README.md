# File location: src/utils/README.md
# Example usage of utilities:

# 1. Date handling:
"""
from utils.date_utils import DateUtils

# Parse date from API
creation_date = DateUtils.parse_date(data.get('CreationDate'))
if creation_date:
    formatted_date = DateUtils.format_date(creation_date)
"""

# 2. Logging setup:
"""
from utils.logging_config import setup_logging

# In main.py or app startup
setup_logging(log_dir='logs', log_level=logging.INFO)
"""

# 3. Data validation:
"""
from utils.validation import ValidationUtils

# Validate required fields
required_fields = ['id', 'name', 'email']
if ValidationUtils.validate_required_fields(data, required_fields):
    # Process data
    pass

# Validate numeric field
if ValidationUtils.validate_numeric_field(order.total, 'total', min_value=0):
    # Process order
    pass
"""

# 4. Retry mechanism:
"""
from utils.retry import retry_with_backoff

@retry_with_backoff(retries=3, backoff_factor=2)
def fetch_data_from_api():
    # API call that might fail
    pass
"""