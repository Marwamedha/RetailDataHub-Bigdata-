-- =====================================================
-- DWH Layer - Retail DatatHub Data Warehouse
-- Author  : Marwa Medhat
-- Purpose : Create Dimension & Fact Tables for Batch + Streaming
-- Storage : ORC
-- =====================================================

-- Enable Dynamic Partitioning
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

-- =====================================================
-- Create Database
-- =====================================================
CREATE DATABASE IF NOT EXISTS companyabc;
USE companyabc;

-- =====================================================
-- ================= DIMENSION TABLES ==================
-- =====================================================

-- -----------------------
-- Date Dimension
-- -----------------------
CREATE EXTERNAL TABLE IF NOT EXISTS date_dim (
    date_key INT,
    full_date STRING,
    year INT,
    quarter_number INT,
    month_name STRING,
    month_number INT,
    day_name STRING,
    day_of_month INT
)
STORED AS ORC
LOCATION '/project/dwh/date_dim';

-- -----------------------
-- Customer Dimension
-- -----------------------
CREATE EXTERNAL TABLE IF NOT EXISTS customer_dim (
    customer_key BIGINT,
    customer_id INT,
    customer_full_name STRING,
    email STRING
)
STORED AS ORC
LOCATION '/project/dwh/customer_dim';

-- -----------------------
-- Product Dimension
-- -----------------------
CREATE EXTERNAL TABLE IF NOT EXISTS product_dim (
    product_key BIGINT,
    product_id INT,
    name STRING,
    category STRING
)
STORED AS ORC
LOCATION '/project/dwh/product_dim';

-- -----------------------
-- Branch Dimension
-- -----------------------
CREATE EXTERNAL TABLE IF NOT EXISTS branch_dim (
    branch_key BIGINT,
    branch_id INT,
    location STRING,
    established_date_id INT,
    class STRING
)
STORED AS ORC
LOCATION '/project/dwh/branch_dim';

-- -----------------------
-- Sales Agent Dimension
-- -----------------------
CREATE EXTERNAL TABLE IF NOT EXISTS sales_agent_dim (
    agent_key BIGINT,
    agent_id INT,
    name STRING,
    hire_date_id INT
)
STORED AS ORC
LOCATION '/project/dwh/sales_agent_dim';

-- =====================================================
-- ================= FACT TABLES =======================
-- =====================================================

-- -----------------------
-- Online Sales Fact
-- -----------------------
CREATE EXTERNAL TABLE IF NOT EXISTS online_sales_fact (
    transaction_id STRING,
    customer_key BIGINT,
    product_key BIGINT,
    units INT,
    unit_price DECIMAL(18,2),
    discount DECIMAL(18,2),
    total_price DECIMAL(18,2),
    payment_method STRING,
    street STRING,
    city STRING,
    state STRING,
    postal_code STRING
)
PARTITIONED BY (transaction_date_key INT)
STORED AS ORC
LOCATION '/project/dwh/online_sales_fact';

-- -----------------------
-- Branch Sales Fact
-- -----------------------
CREATE EXTERNAL TABLE IF NOT EXISTS branch_sales_fact (
    transaction_id STRING,
    customer_key BIGINT,
    branch_key BIGINT,
    sales_agent_key BIGINT,
    product_key BIGINT,
    units INT,
    unit_price DECIMAL(18,2),
    discount DECIMAL(18,2),
    total_price DECIMAL(18,2),
    payment_method STRING
)
PARTITIONED BY (transaction_date_key INT)
STORED AS ORC
LOCATION '/project/dwh/branch_sales_fact';

-- =====================================================
-- ================= STREAMING TABLE ===================
-- =====================================================

-- -----------------------
-- Log Events (Streaming / Lambda Layer)
-- -----------------------
CREATE EXTERNAL TABLE IF NOT EXISTS log_events (
    eventType STRING,
    customerId STRING,
    productId STRING,
    `timestamp` STRING,
    quantity INT,
    totalAmount FLOAT,
    paymentMethod STRING,
    recommendedProductId STRING,
    algorithm STRING,
    category STRING,
    source STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/project/log_Events';

-- =====================================================
-- END OF SCRIPT
-- =====================================================
