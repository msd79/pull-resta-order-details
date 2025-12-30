-- =============================================================================
-- Script: Create Power BI Views
-- Purpose: Creates optimized views for Power BI promotion analysis
--
-- Usage: Run this script on your main RestaOrders database
-- =============================================================================

USE RestaOrders;
GO

-- =============================================================================
-- View: vw_PromotionAnalysis
-- Purpose: Main view for promotion analysis in Power BI
--          Uses pre-aggregated summary table for fast queries
-- =============================================================================
IF OBJECT_ID('vw_PromotionAnalysis', 'V') IS NOT NULL
    DROP VIEW vw_PromotionAnalysis;
GO

CREATE VIEW vw_PromotionAnalysis AS
SELECT
    fps.summary_key,
    fps.date,
    fps.restaurant_key,
    dr.restaurant_name,
    dr.company_name,
    fps.promotion_key,
    COALESCE(dp.promotion_name, 'No Promotion') AS promotion_name,
    COALESCE(dp.promotion_description, 'Orders without promotion applied') AS promotion_description,
    COALESCE(dp.discount_type, 0) AS discount_type,
    COALESCE(dp.discount_amount, 0) AS discount_amount,
    CASE
        WHEN fps.promotion_key IS NULL THEN 'Without Promotion'
        ELSE 'With Promotion'
    END AS promotion_status,
    fps.order_count,
    fps.total_revenue,
    fps.avg_order_value,
    fps.total_discount_given,
    fps.total_sub_total,
    fps.unique_customers,
    fps.new_customers,
    fps.repeat_customers,
    fps.delivery_orders,
    fps.pickup_orders,
    fps.min_order_value,
    fps.max_order_value,
    -- Calculated fields for Power BI
    YEAR(fps.date) AS year,
    MONTH(fps.date) AS month,
    DATENAME(MONTH, fps.date) AS month_name,
    FORMAT(fps.date, 'yyyy-MM') AS year_month,
    DATEPART(QUARTER, fps.date) AS quarter,
    DATEPART(WEEK, fps.date) AS week_number
FROM fact_daily_promotion_summary fps
JOIN dim_restaurant dr ON fps.restaurant_key = dr.restaurant_key
LEFT JOIN dim_promotion dp ON fps.promotion_key = dp.promotion_key;
GO

PRINT 'View vw_PromotionAnalysis created successfully.';
GO

-- =============================================================================
-- View: vw_PromotionComparison
-- Purpose: Aggregated view for comparing promoted vs non-promoted orders
-- =============================================================================
IF OBJECT_ID('vw_PromotionComparison', 'V') IS NOT NULL
    DROP VIEW vw_PromotionComparison;
GO

CREATE VIEW vw_PromotionComparison AS
SELECT
    dr.restaurant_key,
    dr.restaurant_name,
    CASE
        WHEN fps.promotion_key IS NULL THEN 'Without Promotion'
        ELSE 'With Promotion'
    END AS promotion_status,
    dp.promotion_name,
    SUM(fps.order_count) AS total_orders,
    SUM(fps.total_revenue) AS total_revenue,
    CASE
        WHEN SUM(fps.order_count) > 0
        THEN SUM(fps.total_revenue) / SUM(fps.order_count)
        ELSE 0
    END AS avg_order_value,
    SUM(fps.total_discount_given) AS total_discounts,
    SUM(fps.unique_customers) AS total_unique_customers,
    SUM(fps.delivery_orders) AS total_delivery_orders,
    SUM(fps.pickup_orders) AS total_pickup_orders
FROM fact_daily_promotion_summary fps
JOIN dim_restaurant dr ON fps.restaurant_key = dr.restaurant_key
LEFT JOIN dim_promotion dp ON fps.promotion_key = dp.promotion_key
GROUP BY
    dr.restaurant_key,
    dr.restaurant_name,
    CASE
        WHEN fps.promotion_key IS NULL THEN 'Without Promotion'
        ELSE 'With Promotion'
    END,
    dp.promotion_name;
GO

PRINT 'View vw_PromotionComparison created successfully.';
GO

-- =============================================================================
-- View: vw_RecentOrders
-- Purpose: Order-level detail for last 24 months only
--          Use this for drill-down analysis
-- =============================================================================
IF OBJECT_ID('vw_RecentOrders', 'V') IS NOT NULL
    DROP VIEW vw_RecentOrders;
GO

CREATE VIEW vw_RecentOrders AS
SELECT
    fo.order_key,
    fo.order_id,
    dd.date AS order_date,
    dd.datetime AS order_datetime,
    dd.year,
    dd.month,
    dd.day,
    dd.hour,
    dd.day_name,
    dd.month_name,
    dd.day_part,
    dd.is_peak_hour,
    dd.is_weekend,
    fo.restaurant_key,
    dr.restaurant_name,
    fo.customer_key,
    dc.full_name AS customer_name,
    dc.customer_segment,
    fo.promotion_key,
    COALESCE(dp.promotion_name, 'No Promotion') AS promotion_name,
    fo.is_promotion_applied,
    fo.order_status,
    fo.delivery_type,
    CASE fo.delivery_type
        WHEN 1 THEN 'Delivery'
        WHEN 2 THEN 'Pickup'
        ELSE 'Other'
    END AS delivery_type_name,
    fo.order_method,
    fo.sub_total,
    fo.delivery_fee,
    fo.service_charge,
    fo.total_discount,
    fo.total,
    fo.used_points,
    fo.review_rating,
    fo.review_message
FROM fact_orders fo
JOIN dim_datetime dd ON fo.datetime_key = dd.datetime_key
JOIN dim_restaurant dr ON fo.restaurant_key = dr.restaurant_key
JOIN dim_customer dc ON fo.customer_key = dc.customer_key
LEFT JOIN dim_promotion dp ON fo.promotion_key = dp.promotion_key
WHERE dd.date >= DATEADD(MONTH, -24, GETDATE());
GO

PRINT 'View vw_RecentOrders created successfully.';
GO

-- =============================================================================
-- View: vw_PromotionEffectiveness
-- Purpose: Calculate promotion effectiveness metrics
-- =============================================================================
IF OBJECT_ID('vw_PromotionEffectiveness', 'V') IS NOT NULL
    DROP VIEW vw_PromotionEffectiveness;
GO

CREATE VIEW vw_PromotionEffectiveness AS
WITH PromoStats AS (
    SELECT
        restaurant_key,
        promotion_key,
        SUM(order_count) AS promo_orders,
        SUM(total_revenue) AS promo_revenue,
        CASE WHEN SUM(order_count) > 0
             THEN SUM(total_revenue) / SUM(order_count)
             ELSE 0 END AS promo_aov,
        SUM(total_discount_given) AS total_discount
    FROM fact_daily_promotion_summary
    WHERE promotion_key IS NOT NULL
    GROUP BY restaurant_key, promotion_key
),
BaselineStats AS (
    SELECT
        restaurant_key,
        SUM(order_count) AS baseline_orders,
        SUM(total_revenue) AS baseline_revenue,
        CASE WHEN SUM(order_count) > 0
             THEN SUM(total_revenue) / SUM(order_count)
             ELSE 0 END AS baseline_aov
    FROM fact_daily_promotion_summary
    WHERE promotion_key IS NULL
    GROUP BY restaurant_key
)
SELECT
    dr.restaurant_name,
    dp.promotion_name,
    dp.discount_type,
    dp.discount_amount,
    ps.promo_orders,
    ps.promo_revenue,
    ps.promo_aov,
    ps.total_discount,
    bs.baseline_aov,
    -- Effectiveness metrics
    CASE WHEN bs.baseline_aov > 0
         THEN ((ps.promo_aov - bs.baseline_aov) / bs.baseline_aov) * 100
         ELSE 0 END AS aov_lift_percentage,
    ps.promo_revenue - ps.total_discount AS net_revenue,
    CASE WHEN ps.total_discount > 0
         THEN ps.promo_revenue / ps.total_discount
         ELSE 0 END AS revenue_per_discount_dollar
FROM PromoStats ps
JOIN BaselineStats bs ON ps.restaurant_key = bs.restaurant_key
JOIN dim_restaurant dr ON ps.restaurant_key = dr.restaurant_key
JOIN dim_promotion dp ON ps.promotion_key = dp.promotion_key;
GO

PRINT 'View vw_PromotionEffectiveness created successfully.';
GO

-- =============================================================================
-- View: vw_DailyMetricsTrend
-- Purpose: Daily trends for time-series analysis
-- =============================================================================
IF OBJECT_ID('vw_DailyMetricsTrend', 'V') IS NOT NULL
    DROP VIEW vw_DailyMetricsTrend;
GO

CREATE VIEW vw_DailyMetricsTrend AS
SELECT
    fps.date,
    fps.restaurant_key,
    dr.restaurant_name,
    SUM(CASE WHEN fps.promotion_key IS NOT NULL THEN fps.order_count ELSE 0 END) AS orders_with_promo,
    SUM(CASE WHEN fps.promotion_key IS NULL THEN fps.order_count ELSE 0 END) AS orders_without_promo,
    SUM(fps.order_count) AS total_orders,
    SUM(CASE WHEN fps.promotion_key IS NOT NULL THEN fps.total_revenue ELSE 0 END) AS revenue_with_promo,
    SUM(CASE WHEN fps.promotion_key IS NULL THEN fps.total_revenue ELSE 0 END) AS revenue_without_promo,
    SUM(fps.total_revenue) AS total_revenue,
    CASE WHEN SUM(CASE WHEN fps.promotion_key IS NOT NULL THEN fps.order_count ELSE 0 END) > 0
         THEN SUM(CASE WHEN fps.promotion_key IS NOT NULL THEN fps.total_revenue ELSE 0 END) /
              SUM(CASE WHEN fps.promotion_key IS NOT NULL THEN fps.order_count ELSE 0 END)
         ELSE 0 END AS aov_with_promo,
    CASE WHEN SUM(CASE WHEN fps.promotion_key IS NULL THEN fps.order_count ELSE 0 END) > 0
         THEN SUM(CASE WHEN fps.promotion_key IS NULL THEN fps.total_revenue ELSE 0 END) /
              SUM(CASE WHEN fps.promotion_key IS NULL THEN fps.order_count ELSE 0 END)
         ELSE 0 END AS aov_without_promo,
    SUM(fps.total_discount_given) AS total_discounts,
    SUM(fps.unique_customers) AS unique_customers,
    -- Promo penetration
    CASE WHEN SUM(fps.order_count) > 0
         THEN CAST(SUM(CASE WHEN fps.promotion_key IS NOT NULL THEN fps.order_count ELSE 0 END) AS FLOAT) /
              SUM(fps.order_count) * 100
         ELSE 0 END AS promo_penetration_pct
FROM fact_daily_promotion_summary fps
JOIN dim_restaurant dr ON fps.restaurant_key = dr.restaurant_key
GROUP BY fps.date, fps.restaurant_key, dr.restaurant_name;
GO

PRINT 'View vw_DailyMetricsTrend created successfully.';
GO

-- =============================================================================
-- View: vw_PromotionList
-- Purpose: Simple list of all promotions for slicer/filter
-- =============================================================================
IF OBJECT_ID('vw_PromotionList', 'V') IS NOT NULL
    DROP VIEW vw_PromotionList;
GO

CREATE VIEW vw_PromotionList AS
SELECT
    dp.promotion_key,
    dp.promotion_id,
    dp.promotion_name,
    dp.promotion_description,
    dp.discount_type,
    dp.discount_amount,
    dp.coupon_code,
    dp.is_first_order_only,
    dp.is_once_per_customer,
    dr.restaurant_name,
    -- Usage stats
    COALESCE(usage.total_orders, 0) AS total_orders_using_promo,
    COALESCE(usage.total_revenue, 0) AS total_revenue_from_promo,
    COALESCE(usage.first_used, NULL) AS first_used_date,
    COALESCE(usage.last_used, NULL) AS last_used_date
FROM dim_promotion dp
JOIN dim_restaurant dr ON dp.restaurant_key = dr.restaurant_key
LEFT JOIN (
    SELECT
        promotion_key,
        SUM(order_count) AS total_orders,
        SUM(total_revenue) AS total_revenue,
        MIN(date) AS first_used,
        MAX(date) AS last_used
    FROM fact_daily_promotion_summary
    WHERE promotion_key IS NOT NULL
    GROUP BY promotion_key
) usage ON dp.promotion_key = usage.promotion_key;
GO

PRINT 'View vw_PromotionList created successfully.';
GO

-- =============================================================================
-- Verification
-- =============================================================================
PRINT '';
PRINT '=============================================================================';
PRINT 'Power BI Views Created Successfully';
PRINT '=============================================================================';
PRINT '';
PRINT 'Views available for Power BI:';
PRINT '  - vw_PromotionAnalysis        : Main analysis view (uses summary table)';
PRINT '  - vw_PromotionComparison      : Aggregated comparison view';
PRINT '  - vw_RecentOrders             : Order detail (last 24 months)';
PRINT '  - vw_PromotionEffectiveness   : Promotion ROI metrics';
PRINT '  - vw_DailyMetricsTrend        : Time-series trends';
PRINT '  - vw_PromotionList            : Promotion slicer source';
PRINT '';
PRINT 'Recommended Power BI setup:';
PRINT '  1. Connect to vw_PromotionAnalysis for main analysis';
PRINT '  2. Use vw_PromotionList for promotion slicer';
PRINT '  3. Use vw_RecentOrders only for drill-through to order details';
PRINT '';
GO
