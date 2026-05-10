-- ============================================================
-- 03_warehouse_schema.sql
-- Layer 3: Warehouse — star schema optimized for analytics
-- Populated by Airflow DAG (staging_to_warehouse task)
-- ============================================================

CREATE SCHEMA IF NOT EXISTS warehouse;

-- ─── Dimension Tables ────────────────────────────────────────

CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_id         SERIAL PRIMARY KEY,
    full_date       DATE UNIQUE NOT NULL,
    year            INTEGER,
    quarter         INTEGER,
    month           INTEGER,
    month_name      VARCHAR(20),
    week            INTEGER,
    day_of_month    INTEGER,
    day_of_week     INTEGER,
    day_name        VARCHAR(20),
    is_weekend      BOOLEAN
);

CREATE TABLE IF NOT EXISTS warehouse.dim_customers (
    customer_key        SERIAL PRIMARY KEY,
    customer_id         VARCHAR(50) UNIQUE NOT NULL,
    customer_unique_id  VARCHAR(50),
    zip_code_prefix     VARCHAR(10),
    city                VARCHAR(100),
    state               CHAR(2),
    region              VARCHAR(30)
);

CREATE TABLE IF NOT EXISTS warehouse.dim_sellers (
    seller_key          SERIAL PRIMARY KEY,
    seller_id           VARCHAR(50) UNIQUE NOT NULL,
    zip_code_prefix     VARCHAR(10),
    city                VARCHAR(100),
    state               CHAR(2),
    region              VARCHAR(30)
);

CREATE TABLE IF NOT EXISTS warehouse.dim_products (
    product_key             SERIAL PRIMARY KEY,
    product_id              VARCHAR(50) UNIQUE NOT NULL,
    category_name_pt        VARCHAR(100),
    category_name_en        VARCHAR(100),
    name_length             INTEGER,
    description_length      INTEGER,
    photos_qty              INTEGER,
    weight_g                NUMERIC(10,2),
    volume_cm3              NUMERIC(12,2)   -- length * height * width
);

-- ─── Fact Tables ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS warehouse.fact_orders (
    order_key                   SERIAL PRIMARY KEY,
    order_id                    VARCHAR(50) UNIQUE NOT NULL,
    customer_key                INTEGER REFERENCES warehouse.dim_customers(customer_key),
    purchase_date_key           INTEGER REFERENCES warehouse.dim_date(date_id),
    approved_date_key           INTEGER REFERENCES warehouse.dim_date(date_id),
    delivered_date_key          INTEGER REFERENCES warehouse.dim_date(date_id),
    estimated_delivery_date_key INTEGER REFERENCES warehouse.dim_date(date_id),
    order_status                VARCHAR(30),
    delivery_delay_days         INTEGER,   -- delivered - estimated (negative = early)
    approval_time_hours         NUMERIC(8,2)
);

CREATE TABLE IF NOT EXISTS warehouse.fact_order_items (
    item_key        SERIAL PRIMARY KEY,
    order_id        VARCHAR(50),
    order_item_id   INTEGER,
    order_key       INTEGER REFERENCES warehouse.fact_orders(order_key),
    product_key     INTEGER REFERENCES warehouse.dim_products(product_key),
    seller_key      INTEGER REFERENCES warehouse.dim_sellers(seller_key),
    price           NUMERIC(10,2),
    freight_value   NUMERIC(10,2),
    total_value     NUMERIC(10,2)  -- price + freight_value
);

CREATE TABLE IF NOT EXISTS warehouse.fact_payments (
    payment_key         SERIAL PRIMARY KEY,
    order_id            VARCHAR(50),
    payment_sequential  INTEGER,
    order_key           INTEGER REFERENCES warehouse.fact_orders(order_key),
    payment_type        VARCHAR(30),
    installments        INTEGER,
    payment_value       NUMERIC(10,2)
);
