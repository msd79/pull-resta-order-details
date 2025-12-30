# Promotion Analysis & Data Archival Implementation Plan

## Overview

This document outlines the implementation plan for:
1. **Promotion Analysis Summary Tables** - Pre-aggregated daily metrics for fast Power BI queries
2. **Data Archival Strategy** - Moving data older than 24 months to a historical database

---

## Current State

| Aspect | Current |
|--------|---------|
| Database | SQL Server Express (10GB limit) |
| Current size | ~1.5 GB |
| Data history | ~5 years (since 2020) |
| Power BI connection | DirectQuery to fact tables |
| Performance | Slow due to large table scans |

---

## Target Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         MAIN DATABASE                           │
│                    (RestaOrders - Active)                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Summary Tables (All Time - Small)                              │
│  ├── FactDailyPromotionSummary    ~100 MB                       │
│  ├── FactRestaurantMetrics        ~50 MB                        │
│  └── Dimension Tables             ~20 MB                        │
│                                                                 │
│  Detail Tables (Rolling 24 Months)                              │
│  ├── FactOrders                   ~600 MB                       │
│  ├── FactPayments                 ~200 MB                       │
│  └── FactCustomerMetrics          ~100 MB                       │
│                                                                 │
│  OLTP Tables (Rolling 24 Months)                                │
│  ├── orders                       ~200 MB                       │
│  ├── payments                     ~100 MB                       │
│  └── customers, addresses, etc.   ~50 MB                        │
│                                                                 │
│  Estimated Total: ~1.0-1.2 GB (stable)                          │
│                                                                 │
│  Power BI Connects Here                                         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ Monthly Archive Job
                              │ (Move data older than 24 months)
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      HISTORICAL DATABASE                        │
│                  (RestaOrders_Historical)                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Archived Detail Tables                                         │
│  ├── FactOrders_Archive                                         │
│  ├── FactPayments_Archive                                       │
│  └── FactCustomerMetrics_Archive                                │
│                                                                 │
│  Archived OLTP Tables                                           │
│  ├── orders_archive                                             │
│  ├── payments_archive                                           │
│  └── customers (reference copy)                                 │
│                                                                 │
│  Growth: ~300 MB/year                                           │
│                                                                 │
│  Accessed only for deep historical queries (rare)               │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Implementation Phases

### Phase 1: Promotion Summary Table

**Goal:** Enable fast promotion impact analysis in Power BI

#### 1.1 New Database Model

**File:** `src/database/dimentional_models.py`

```python
class FactDailyPromotionSummary(Base):
    __tablename__ = 'fact_daily_promotion_summary'
    __table_args__ = (
        UniqueConstraint('restaurant_key', 'date', 'promotion_key',
                         name='unique_restaurant_date_promotion'),
        Index('idx_fact_promo_summary_date', 'date'),
        Index('idx_fact_promo_summary_restaurant_date', 'restaurant_key', 'date'),
    )

    summary_key = Column(Integer, primary_key=True)
    restaurant_key = Column(Integer, ForeignKey('dim_restaurant.restaurant_key'), nullable=False)
    date = Column(Date, nullable=False)

    # NULL promotion_key = orders without any promotion
    promotion_key = Column(Integer, ForeignKey('dim_promotion.promotion_key'), nullable=True)

    # Metrics
    order_count = Column(Integer, default=0)
    total_revenue = Column(Float, default=0.0)
    avg_order_value = Column(Float, default=0.0)
    total_discount_given = Column(Float, default=0.0)

    # Customer metrics
    unique_customers = Column(Integer, default=0)
    new_customers = Column(Integer, default=0)      # First order ever
    repeat_customers = Column(Integer, default=0)   # Had previous orders

    # Order type breakdown
    delivery_orders = Column(Integer, default=0)
    pickup_orders = Column(Integer, default=0)
```

#### 1.2 New Service

**File:** `src/services/promotion_summary_service.py`

```python
class PromotionSummaryService:
    """
    Manages daily promotion summary aggregations.
    """

    def __init__(self, session):
        self.session = session

    async def update_daily_summary(self, restaurant_key: int, date: date, promotion_key: int = None):
        """
        Update or create summary record for a specific restaurant/date/promotion combination.
        Called after each order is processed.
        """
        pass

    async def backfill_summaries(self, start_date: date = None, end_date: date = None):
        """
        One-time backfill of historical data into summary table.
        """
        pass

    async def recalculate_date(self, restaurant_key: int, date: date):
        """
        Recalculate all summaries for a specific date (useful for corrections).
        """
        pass
```

#### 1.3 Integration Points

- **ETL Orchestration:** Call `update_daily_summary()` after processing each order
- **Backfill Script:** One-time script to populate historical summaries

---

### Phase 2: Historical Database Setup

**Goal:** Create separate database for archived data

#### 2.1 Create Historical Database

```sql
-- Run manually on SQL Server
CREATE DATABASE RestaOrders_Historical;
```

#### 2.2 Archive Table Structures

**File:** `src/database/archive_models.py`

Mirror of main tables but in historical database:

```python
# These tables exist in RestaOrders_Historical database
# Same structure as main tables

class FactOrdersArchive(Base):
    __tablename__ = 'fact_orders_archive'
    # Same columns as FactOrders

class FactPaymentsArchive(Base):
    __tablename__ = 'fact_payments_archive'
    # Same columns as FactPayments

class OrdersArchive(Base):
    __tablename__ = 'orders_archive'
    # Same columns as Orders (OLTP)

class PaymentsArchive(Base):
    __tablename__ = 'payments_archive'
    # Same columns as Payments (OLTP)
```

#### 2.3 Database Connection Configuration

**File:** `config/config.yaml`

```yaml
database:
  # Main database (existing)
  server: "192.168.0.184\\SQLEXPRESS"
  database: "RestaOrders"
  username: "sa"
  driver: "ODBC Driver 17 for SQL Server"

  # Historical database (new)
  historical_database: "RestaOrders_Historical"
```

---

### Phase 3: Archive Job Service

**Goal:** Monthly job to move old data to historical database

#### 3.1 Archive Service

**File:** `src/services/archive_service.py`

```python
class ArchiveService:
    """
    Handles moving data older than retention period to historical database.
    """

    def __init__(self, main_session, historical_session):
        self.main_session = main_session
        self.historical_session = historical_session
        self.retention_months = 24

    async def run_archive_job(self):
        """
        Main archive job - run monthly.

        Steps:
        1. Identify records older than retention period
        2. Copy to historical database
        3. Delete from main database (in FK order)
        4. Log results
        """
        pass

    async def archive_fact_tables(self, cutoff_date: date):
        """
        Archive dimensional fact tables.
        Order: FactPayments -> FactCustomerMetrics -> FactOrders
        """
        pass

    async def archive_oltp_tables(self, cutoff_date: date):
        """
        Archive OLTP tables.
        Order: payments -> orders
        """
        pass

    async def verify_archive(self, cutoff_date: date):
        """
        Verify data was copied correctly before deletion.
        """
        pass
```

#### 3.2 Archive Job Deletion Order

To avoid FK constraint errors, delete in this order:

```
DIMENSIONAL TABLES (delete order):
1. FactPayments      (references FactOrders)
2. FactCustomerMetrics (references dim_customer, dim_datetime)
3. FactOrders        (parent - delete last)

OLTP TABLES (delete order):
1. payments          (references orders)
2. orders            (parent - delete last)
3. customer_addresses (optional - if needed)
```

#### 3.3 Archive Job Scheduling

**Option A:** Integrate into main application
```python
# In main.py - run on 1st of each month
if datetime.now().day == 1:
    await archive_service.run_archive_job()
```

**Option B:** Separate script (recommended)
```bash
# scripts/run_archive_job.py
# Run via cron/Task Scheduler monthly
```

---

### Phase 4: Backfill Historical Data

**Goal:** One-time population of summary tables from existing data

#### 4.1 Backfill Script

**File:** `scripts/backfill_promotion_summaries.py`

```python
"""
One-time script to populate FactDailyPromotionSummary from historical orders.

Usage:
    python scripts/backfill_promotion_summaries.py

Options:
    --start-date    Start date for backfill (default: 2020-01-01)
    --end-date      End date for backfill (default: today)
    --restaurant-id Specific restaurant (default: all)
    --batch-size    Records per batch (default: 1000)
"""

async def backfill_promotion_summaries():
    """
    Aggregate existing FactOrders data into FactDailyPromotionSummary.

    Process:
    1. Query FactOrders grouped by restaurant, date, promotion
    2. Calculate metrics for each group
    3. Insert into FactDailyPromotionSummary
    """
    pass
```

#### 4.2 Backfill SQL Logic

```sql
-- Core aggregation query for backfill
INSERT INTO fact_daily_promotion_summary
    (restaurant_key, date, promotion_key, order_count, total_revenue,
     avg_order_value, total_discount_given, unique_customers,
     delivery_orders, pickup_orders)
SELECT
    fo.restaurant_key,
    CAST(dd.date AS DATE) as date,
    fo.promotion_key,  -- NULL for non-promoted orders
    COUNT(*) as order_count,
    SUM(fo.total) as total_revenue,
    AVG(fo.total) as avg_order_value,
    SUM(COALESCE(fo.total_discount, 0)) as total_discount_given,
    COUNT(DISTINCT fo.customer_key) as unique_customers,
    SUM(CASE WHEN fo.delivery_type = 1 THEN 1 ELSE 0 END) as delivery_orders,
    SUM(CASE WHEN fo.delivery_type = 2 THEN 1 ELSE 0 END) as pickup_orders
FROM fact_orders fo
JOIN dim_datetime dd ON fo.datetime_key = dd.datetime_key
GROUP BY fo.restaurant_key, CAST(dd.date AS DATE), fo.promotion_key
```

---

### Phase 5: ETL Integration

**Goal:** Automatically update summaries when new orders arrive

#### 5.1 Modify ETL Orchestration

**File:** `src/services/etl_orchestration_service.py`

Add call to promotion summary service:

```python
async def process_order_to_dimensional(self, order_data: dict):
    """
    Existing method - add promotion summary update.
    """
    # Existing logic...

    # NEW: Update promotion summary
    await self.promotion_summary_service.update_daily_summary(
        restaurant_key=restaurant_key,
        date=order_date,
        promotion_key=promotion_key  # None if no promotion
    )
```

#### 5.2 Integration Flow

```
Order Received from API
        │
        ▼
Sync to OLTP Tables (existing)
        │
        ▼
Populate Dimension Tables (existing)
        │
        ▼
Populate Fact Tables (existing)
        │
        ▼
Update FactDailyPromotionSummary (NEW)
        │
        ▼
Update FactRestaurantMetrics (existing)
```

---

### Phase 6: Power BI Views

**Goal:** Provide clean interfaces for Power BI

#### 6.1 SQL Views

**File:** `scripts/create_powerbi_views.sql`

```sql
-- View for promotion analysis (uses summary table)
CREATE VIEW vw_PromotionAnalysis AS
SELECT
    fps.date,
    fps.restaurant_key,
    dr.restaurant_name,
    fps.promotion_key,
    COALESCE(dp.promotion_name, 'No Promotion') as promotion_name,
    fps.order_count,
    fps.total_revenue,
    fps.avg_order_value,
    fps.total_discount_given,
    fps.unique_customers,
    fps.new_customers,
    fps.repeat_customers,
    fps.delivery_orders,
    fps.pickup_orders
FROM fact_daily_promotion_summary fps
JOIN dim_restaurant dr ON fps.restaurant_key = dr.restaurant_key
LEFT JOIN dim_promotion dp ON fps.promotion_key = dp.promotion_key;

-- View for recent order details (last 24 months)
CREATE VIEW vw_RecentOrders AS
SELECT fo.*
FROM fact_orders fo
JOIN dim_datetime dd ON fo.datetime_key = dd.datetime_key
WHERE dd.date >= DATEADD(month, -24, GETDATE());

-- View for all-time metrics (from summaries)
CREATE VIEW vw_PromotionComparison AS
SELECT
    dr.restaurant_name,
    CASE WHEN fps.promotion_key IS NULL THEN 'Without Promotion'
         ELSE 'With Promotion' END as promotion_status,
    dp.promotion_name,
    SUM(fps.order_count) as total_orders,
    SUM(fps.total_revenue) as total_revenue,
    AVG(fps.avg_order_value) as avg_order_value,
    SUM(fps.total_discount_given) as total_discounts
FROM fact_daily_promotion_summary fps
JOIN dim_restaurant dr ON fps.restaurant_key = dr.restaurant_key
LEFT JOIN dim_promotion dp ON fps.promotion_key = dp.promotion_key
GROUP BY dr.restaurant_name,
         CASE WHEN fps.promotion_key IS NULL THEN 'Without Promotion'
              ELSE 'With Promotion' END,
         dp.promotion_name;
```

---

## Implementation Order

| Step | Task | Dependencies | Estimated Effort |
|------|------|--------------|------------------|
| 1 | Add `FactDailyPromotionSummary` model | None | Small |
| 2 | Create `PromotionSummaryService` | Step 1 | Medium |
| 3 | Create backfill script | Steps 1-2 | Medium |
| 4 | Run backfill on historical data | Step 3 | Run time varies |
| 5 | Integrate summary update into ETL | Step 2 | Small |
| 6 | Create Historical database | None | Small |
| 7 | Create archive table models | Step 6 | Small |
| 8 | Create `ArchiveService` | Steps 6-7 | Medium |
| 9 | Create archive job script | Step 8 | Small |
| 10 | Run initial archive (move old data) | Step 9 | Run time varies |
| 11 | Create Power BI views | Steps 1-5 | Small |
| 12 | Update Power BI reports | Step 11 | Manual |

---

## File Changes Summary

### New Files

| File | Purpose |
|------|---------|
| `src/services/promotion_summary_service.py` | Daily summary calculations |
| `src/services/archive_service.py` | Data archival logic |
| `src/database/archive_models.py` | Archive table definitions |
| `scripts/backfill_promotion_summaries.py` | One-time historical backfill |
| `scripts/run_archive_job.py` | Monthly archive job |
| `scripts/create_powerbi_views.sql` | Power BI view definitions |

### Modified Files

| File | Changes |
|------|---------|
| `src/database/dimentional_models.py` | Add `FactDailyPromotionSummary` |
| `src/services/etl_orchestration_service.py` | Call promotion summary service |
| `src/config/settings.py` | Add historical database config |
| `config/config.yaml` | Add historical database connection |

---

## Power BI Usage After Implementation

### For Promotion Analysis

```
┌──────────────────────────────────────────────────────────────┐
│  Data Source: vw_PromotionAnalysis                           │
│                                                              │
│  Slicers:                                                    │
│  ├── Date Range: [Start Date] to [End Date]                  │
│  ├── Restaurant: [Select Restaurant]                         │
│  └── Promotion: [Select Promotion or "No Promotion"]         │
│                                                              │
│  Visuals:                                                    │
│  ├── Card: Total Orders, Revenue, AOV                        │
│  ├── Line Chart: Daily trends                                │
│  └── Table: Promotion comparison                             │
│                                                              │
│  Performance: Fast (queries pre-aggregated summary table)    │
└──────────────────────────────────────────────────────────────┘
```

### For Order-Level Drill Down

```
┌──────────────────────────────────────────────────────────────┐
│  Data Source: vw_RecentOrders                                │
│                                                              │
│  Available: Last 24 months of detailed order data            │
│  Use for: Ad-hoc analysis, specific order lookups            │
│                                                              │
│  Performance: Good (only 24 months of data)                  │
└──────────────────────────────────────────────────────────────┘
```

---

## Rollback Plan

If issues occur:

1. **Summary table issues:** Drop and recreate, re-run backfill
2. **Archive job issues:** Data remains in historical DB, can be restored
3. **ETL integration issues:** Disable summary update, fix, re-enable

---

## Success Criteria

- [ ] Power BI promotion analysis queries complete in < 5 seconds
- [ ] Main database size stabilizes at ~1-1.2 GB
- [ ] Historical data accessible in separate database
- [ ] Daily ETL completes without errors
- [ ] Monthly archive job runs successfully

---

## Next Steps

1. Review and approve this plan
2. Begin Phase 1: Promotion Summary Table implementation
3. Test with subset of data before full backfill
