Run docker compopse

Build container:
DB_PASSWORD=qwerty DB_PASSPHRASE=X7h3NkL9qzVm docker-compose up --build

python3 -c "import pyodbc; print(pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};Server=192.168.0.11\\SQLEXPRESS;Database=RestaOrders1;UID=sa;PWD=qwerty'))"


# Restaurant Analytics System Refactoring Project

I am refactoring a restaurant order management system from a transactional database to a dimensional model optimized for PowerBI analytics. I have the following source data structure:

## Current Data Models:
- Orders (main transaction table)
- Customers
- Restaurants
- Promotions
- Payments
- CustomerAddresses

## Refactoring Goals:
1. Transform the current OLTP structure into a dimensional model
2. Optimize for PowerBI analytics and reporting
3. Enable efficient time-based analysis
4. Support customer behavior analytics
5. Create aggregated metrics for performance analysis

## Requested Changes:

### 1. Create Dimension Tables:
- DimDateTime (highest priority)
  - Support time intelligence at 15-minute intervals
  - Include business-specific time periods (breakfast, lunch, dinner)
  - Track business hours and weekends
  - Enable efficient date-based filtering and aggregation

- DimCustomer
  - Track customer lifecycle
  - Include aggregated metrics (lifetime value, total orders)
  - Support customer segmentation

- DimRestaurant
  - Restaurant performance metrics
  - Location and service information

- DimPromotion
  - Promotion effectiveness tracking
  - Discount and benefit type analysis

- DimPaymentMethod
  - Payment type analysis
  - Payment method performance

### 2. Create Fact Tables:
- FactOrders
  - Main fact table for order transactions
  - Link to all relevant dimensions
  - Include all monetary metrics

- FactPayments
  - Payment transaction details
  - Payment status tracking

- FactCustomerMetrics
  - Daily/Monthly customer metrics
  - Customer behavior patterns

### 3. Technical Requirements:
- Use surrogate keys for all dimensions
- Implement slowly changing dimensions where appropriate
- Create efficient indexing strategy
- Support both detailed and aggregated analysis

### 4. Analytics Requirements:
- Time-based analysis (daily, weekly, monthly trends)
- Customer behavior analysis
- Restaurant performance metrics
- Promotion effectiveness
- Payment pattern analysis
- Delivery vs. pickup analysis

Please help me implement these changes systematically, focusing on:
1. Creating dimension and fact table structures
2. Developing ETL processes for data transformation
3. Implementing specific business metrics
4. Optimizing for PowerBI performance

The system pulls data from an API and we can restart with a fresh database, so we don't need migration scripts.
