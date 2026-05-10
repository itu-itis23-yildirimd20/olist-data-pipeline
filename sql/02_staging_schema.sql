-- ============================================================
-- 02_staging_schema.sql
-- Layer 2: Staging — type-cast, cleaned, normalized data
-- Populated by Airflow DAG (raw_to_staging task)
-- ============================================================

CREATE SCHEMA IF NOT EXISTS staging;

-- Orders
CREATE TABLE IF NOT EXISTS staging.orders (
    order_id                          VARCHAR(50) PRIMARY KEY,
    customer_id                       VARCHAR(50),
    order_status                      VARCHAR(30),
    order_purchase_timestamp          TIMESTAMP,
    order_approved_at                 TIMESTAMP,
    order_delivered_carrier_date      TIMESTAMP,
    order_delivered_customer_date     TIMESTAMP,
    order_estimated_delivery_date     TIMESTAMP
);

-- Order Items
CREATE TABLE IF NOT EXISTS staging.order_items (
    order_id            VARCHAR(50),
    order_item_id       INTEGER,
    product_id          VARCHAR(50),
    seller_id           VARCHAR(50),
    shipping_limit_date TIMESTAMP,
    price               NUMERIC(10,2),
    freight_value       NUMERIC(10,2),
    PRIMARY KEY (order_id, order_item_id)
);

-- Customers
CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id               VARCHAR(50) PRIMARY KEY,
    customer_unique_id        VARCHAR(50),
    customer_zip_code_prefix  VARCHAR(10),
    customer_city             VARCHAR(100),
    customer_state            CHAR(2)
);

-- Sellers
CREATE TABLE IF NOT EXISTS staging.sellers (
    seller_id               VARCHAR(50) PRIMARY KEY,
    seller_zip_code_prefix  VARCHAR(10),
    seller_city             VARCHAR(100),
    seller_state            CHAR(2)
);

-- Products
CREATE TABLE IF NOT EXISTS staging.products (
    product_id                  VARCHAR(50) PRIMARY KEY,
    product_category_name       VARCHAR(100),
    product_name_lenght         INTEGER,
    product_description_lenght  INTEGER,
    product_photos_qty          INTEGER,
    product_weight_g            NUMERIC(10,2),
    product_length_cm           NUMERIC(10,2),
    product_height_cm           NUMERIC(10,2),
    product_width_cm            NUMERIC(10,2)
);

-- Payments
CREATE TABLE IF NOT EXISTS staging.order_payments (
    order_id                VARCHAR(50),
    payment_sequential      INTEGER,
    payment_type            VARCHAR(30),
    payment_installments    INTEGER,
    payment_value           NUMERIC(10,2),
    PRIMARY KEY (order_id, payment_sequential)
);

-- Category translation
CREATE TABLE IF NOT EXISTS staging.category_translation (
    product_category_name         VARCHAR(100) PRIMARY KEY,
    product_category_name_english VARCHAR(100)
);
