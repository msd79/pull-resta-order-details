-- =============================================================================
-- Script: Create Historical Database for Data Archival
-- Purpose: Creates the RestaOrders_Historical database and archive tables
--
-- Usage: Run this script on your SQL Server instance
--        Execute in SQL Server Management Studio or via sqlcmd
-- =============================================================================

-- Create the historical database
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'RestaOrders_Historical')
BEGIN
    CREATE DATABASE RestaOrders_Historical;
    PRINT 'Database RestaOrders_Historical created successfully.';
END
ELSE
BEGIN
    PRINT 'Database RestaOrders_Historical already exists.';
END
GO

USE RestaOrders_Historical;
GO

-- =============================================================================
-- Archive Tables
-- =============================================================================

-- Fact Orders Archive
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'fact_orders_archive')
BEGIN
    CREATE TABLE fact_orders_archive (
        order_key INT PRIMARY KEY,
        order_id INT NOT NULL,
        datetime_key INT NOT NULL,
        customer_key INT NOT NULL,
        restaurant_key INT NOT NULL,
        promotion_key INT NULL,
        order_status INT NOT NULL,
        delivery_type INT NOT NULL,
        order_method INT NOT NULL,
        sub_total FLOAT NOT NULL,
        delivery_fee FLOAT NULL,
        service_charge FLOAT NULL,
        total_discount FLOAT NULL,
        total FLOAT NOT NULL,
        used_points INT NULL,
        is_promotion_applied BIT NOT NULL DEFAULT 0,
        review_rating INT NULL,
        review_message NVARCHAR(1000) NULL,
        archived_date DATETIME NOT NULL,
        original_creation_date DATETIME NULL
    );

    CREATE INDEX idx_fact_orders_archive_restaurant_datetime
        ON fact_orders_archive (restaurant_key, datetime_key);
    CREATE INDEX idx_fact_orders_archive_customer
        ON fact_orders_archive (customer_key);
    CREATE INDEX idx_fact_orders_archive_date
        ON fact_orders_archive (archived_date);

    PRINT 'Table fact_orders_archive created successfully.';
END
GO

-- Fact Payments Archive
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'fact_payments_archive')
BEGIN
    CREATE TABLE fact_payments_archive (
        payment_key INT PRIMARY KEY,
        payment_id INT NOT NULL,
        order_key INT NOT NULL,
        datetime_key INT NOT NULL,
        payment_method_key INT NOT NULL,
        sub_total FLOAT NOT NULL,
        extra_charge FLOAT NULL,
        discount FLOAT NULL,
        tax FLOAT NULL,
        tip FLOAT NULL,
        total_amount FLOAT NOT NULL,
        payment_status INT NOT NULL,
        restaurant_key INT NULL,
        archived_date DATETIME NOT NULL
    );

    CREATE INDEX idx_fact_payments_archive_order
        ON fact_payments_archive (order_key);
    CREATE INDEX idx_fact_payments_archive_restaurant
        ON fact_payments_archive (restaurant_key);
    CREATE INDEX idx_fact_payments_archive_date
        ON fact_payments_archive (archived_date);

    PRINT 'Table fact_payments_archive created successfully.';
END
GO

-- Fact Customer Metrics Archive
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'fact_customer_metrics_archive')
BEGIN
    CREATE TABLE fact_customer_metrics_archive (
        metric_key INT PRIMARY KEY,
        order_id INT NOT NULL,
        customer_key INT NOT NULL,
        datetime_key INT NOT NULL,
        daily_orders INT NULL,
        daily_spend FLOAT NULL,
        points_used INT NULL,
        running_order_count INT NULL,
        running_total_spend FLOAT NULL,
        running_avg_order_value FLOAT NULL,
        days_since_last_order INT NULL,
        order_frequency_days FLOAT NULL,
        restaurant_key INT NULL,
        archived_date DATETIME NOT NULL
    );

    CREATE INDEX idx_fact_customer_metrics_archive_customer
        ON fact_customer_metrics_archive (customer_key);
    CREATE INDEX idx_fact_customer_metrics_archive_restaurant
        ON fact_customer_metrics_archive (restaurant_key);
    CREATE INDEX idx_fact_customer_metrics_archive_date
        ON fact_customer_metrics_archive (archived_date);

    PRINT 'Table fact_customer_metrics_archive created successfully.';
END
GO

-- Orders Archive (OLTP)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'orders_archive')
BEGIN
    CREATE TABLE orders_archive (
        id INT PRIMARY KEY,
        restaurant_id INT NULL,
        customer_id INT NULL,
        customer_address_id INT NULL,
        delivery_type INT NULL,
        order_method INT NULL,
        sub_total FLOAT NULL,
        delivery_fee FLOAT NULL,
        service_charge FLOAT NULL,
        total FLOAT NULL,
        status INT NULL,
        creation_date DATETIME NULL,
        payment_status INT NULL,
        number_of_orders INT NULL,
        phone NVARCHAR(15) NULL,
        order_date DATETIME NULL,
        promotion_id INT NULL,
        line_item_discount FLOAT DEFAULT 0,
        discount FLOAT DEFAULT 0,
        card_surcharge FLOAT DEFAULT 0,
        delivery_option_type INT NULL,
        tip FLOAT DEFAULT 0,
        used_points INT DEFAULT 0,
        total_paid FLOAT DEFAULT 0,
        total_balance FLOAT DEFAULT 0,
        review_rating INT NULL,
        review_message NVARCHAR(4000) NULL,
        archived_date DATETIME NOT NULL
    );

    CREATE INDEX idx_orders_archive_restaurant_date
        ON orders_archive (restaurant_id, creation_date);
    CREATE INDEX idx_orders_archive_archived
        ON orders_archive (archived_date);

    PRINT 'Table orders_archive created successfully.';
END
GO

-- Payments Archive (OLTP)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'payments_archive')
BEGIN
    CREATE TABLE payments_archive (
        id INT PRIMARY KEY,
        order_id INT NULL,
        payment_method_id INT NULL,
        payment_method_type INT NULL,
        extra_charge FLOAT NULL,
        sub_total FLOAT NULL,
        discount FLOAT DEFAULT 0,
        tax FLOAT DEFAULT 0,
        amount FLOAT NULL,
        status INT NULL,
        tip FLOAT DEFAULT 0,
        payment_method_name NVARCHAR(255) NULL,
        restaurant_id INT NULL,
        archived_date DATETIME NOT NULL
    );

    CREATE INDEX idx_payments_archive_order
        ON payments_archive (order_id);
    CREATE INDEX idx_payments_archive_restaurant
        ON payments_archive (restaurant_id);
    CREATE INDEX idx_payments_archive_date
        ON payments_archive (archived_date);

    PRINT 'Table payments_archive created successfully.';
END
GO

-- Archive Log (for tracking archive operations)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'archive_log')
BEGIN
    CREATE TABLE archive_log (
        id INT IDENTITY(1,1) PRIMARY KEY,
        archive_date DATETIME NOT NULL,
        cutoff_date DATE NOT NULL,
        table_name NVARCHAR(100) NOT NULL,
        records_archived INT NOT NULL,
        records_deleted INT NOT NULL,
        status NVARCHAR(20) NOT NULL,
        error_message NVARCHAR(1000) NULL,
        duration_seconds FLOAT NULL
    );

    PRINT 'Table archive_log created successfully.';
END
GO

-- =============================================================================
-- Verification
-- =============================================================================

PRINT '';
PRINT '=============================================================================';
PRINT 'Historical Database Setup Complete';
PRINT '=============================================================================';
PRINT '';

SELECT
    t.name AS TableName,
    SUM(p.rows) AS RowCount
FROM sys.tables t
INNER JOIN sys.partitions p ON t.object_id = p.object_id
WHERE p.index_id IN (0, 1)
GROUP BY t.name
ORDER BY t.name;

PRINT '';
PRINT 'Next steps:';
PRINT '1. Update config/config.yaml with historical_database setting';
PRINT '2. Run the archive job: python scripts/run_archive_job.py';
PRINT '';
