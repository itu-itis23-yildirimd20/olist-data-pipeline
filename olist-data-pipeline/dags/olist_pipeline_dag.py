"""
olist_pipeline_dag.py
─────────────────────
Olist E-Commerce Data Engineering Pipeline
YZV 322E — Applied Data Engineering · Spring 2026

DAG Flow:
    raw_to_staging >> staging_to_warehouse >> refresh_views >> push_to_elasticsearch

Schedule: daily (@daily), can also be triggered manually.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta

import psycopg2
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

# ─── Default arguments ───────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner": "olist_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ─── Connection helpers ───────────────────────────────────────────────────────

def _pg_conn():
    return psycopg2.connect(
        host="olist_postgres",
        port=5432,
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def _exec_sql(sql: str) -> None:
    conn = _pg_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql)
    finally:
        conn.close()


# ─── Task functions ──────────────────────────────────────────────────────────

def raw_to_staging(**_):
    """Transform raw TEXT data into typed staging tables."""
    logging.info("Starting raw → staging transformation")

    sql = """
    -- Truncate staging tables before reload (idempotent)
    TRUNCATE staging.orders, staging.order_items, staging.customers,
             staging.sellers, staging.products, staging.order_payments,
             staging.category_translation;

    -- Orders
    INSERT INTO staging.orders
    SELECT
        order_id,
        customer_id,
        order_status,
        NULLIF(order_purchase_timestamp, '')::TIMESTAMP,
        NULLIF(order_approved_at, '')::TIMESTAMP,
        NULLIF(order_delivered_carrier_date, '')::TIMESTAMP,
        NULLIF(order_delivered_customer_date, '')::TIMESTAMP,
        NULLIF(order_estimated_delivery_date, '')::TIMESTAMP
    FROM raw.orders
    WHERE order_id IS NOT NULL AND order_id <> ''
    ON CONFLICT (order_id) DO NOTHING;

    -- Customers
    INSERT INTO staging.customers
    SELECT customer_id, customer_unique_id, customer_zip_code_prefix,
           customer_city, customer_state
    FROM raw.customers
    WHERE customer_id IS NOT NULL AND customer_id <> ''
    ON CONFLICT (customer_id) DO NOTHING;

    -- Sellers
    INSERT INTO staging.sellers
    SELECT seller_id, seller_zip_code_prefix, seller_city, seller_state
    FROM raw.sellers
    WHERE seller_id IS NOT NULL AND seller_id <> ''
    ON CONFLICT (seller_id) DO NOTHING;

    -- Category translation
    INSERT INTO staging.category_translation
    SELECT product_category_name, product_category_name_english
    FROM raw.category_translation
    WHERE product_category_name IS NOT NULL AND product_category_name <> ''
    ON CONFLICT (product_category_name) DO NOTHING;

    -- Products (joined with translation)
    INSERT INTO staging.products
    SELECT
        p.product_id,
        p.product_category_name,
        NULLIF(p.product_name_lenght, '')::INTEGER,
        NULLIF(p.product_description_lenght, '')::INTEGER,
        NULLIF(p.product_photos_qty, '')::INTEGER,
        NULLIF(p.product_weight_g, '')::NUMERIC,
        NULLIF(p.product_length_cm, '')::NUMERIC,
        NULLIF(p.product_height_cm, '')::NUMERIC,
        NULLIF(p.product_width_cm, '')::NUMERIC
    FROM raw.products p
    WHERE p.product_id IS NOT NULL AND p.product_id <> ''
    ON CONFLICT (product_id) DO NOTHING;

    -- Order items
    INSERT INTO staging.order_items
    SELECT
        order_id,
        NULLIF(order_item_id, '')::INTEGER,
        product_id,
        seller_id,
        NULLIF(shipping_limit_date, '')::TIMESTAMP,
        NULLIF(price, '')::NUMERIC,
        NULLIF(freight_value, '')::NUMERIC
    FROM raw.order_items
    WHERE order_id IS NOT NULL AND order_item_id IS NOT NULL
    ON CONFLICT (order_id, order_item_id) DO NOTHING;

    -- Payments
    INSERT INTO staging.order_payments
    SELECT
        order_id,
        NULLIF(payment_sequential, '')::INTEGER,
        payment_type,
        NULLIF(payment_installments, '')::INTEGER,
        NULLIF(payment_value, '')::NUMERIC
    FROM raw.order_payments
    WHERE order_id IS NOT NULL AND payment_sequential IS NOT NULL
    ON CONFLICT (order_id, payment_sequential) DO NOTHING;
    """
    _exec_sql(sql)
    logging.info("raw → staging: done")


def staging_to_warehouse(**_):
    """Populate star schema warehouse from staging tables."""
    logging.info("Starting staging → warehouse transformation")

    sql = """
    -- ── dim_date (generate for all relevant dates) ──────────────────
    INSERT INTO warehouse.dim_date (
        full_date, year, quarter, month, month_name,
        week, day_of_month, day_of_week, day_name, is_weekend
    )
    SELECT DISTINCT
        d::DATE,
        EXTRACT(YEAR FROM d)::INTEGER,
        EXTRACT(QUARTER FROM d)::INTEGER,
        EXTRACT(MONTH FROM d)::INTEGER,
        TO_CHAR(d, 'Month'),
        EXTRACT(WEEK FROM d)::INTEGER,
        EXTRACT(DAY FROM d)::INTEGER,
        EXTRACT(DOW FROM d)::INTEGER,
        TO_CHAR(d, 'Day'),
        EXTRACT(DOW FROM d) IN (0, 6)
    FROM (
        SELECT generate_series(
            MIN(order_purchase_timestamp)::DATE,
            MAX(COALESCE(order_delivered_customer_date, order_estimated_delivery_date))::DATE,
            '1 day'::INTERVAL
        ) AS d
        FROM staging.orders
        WHERE order_purchase_timestamp IS NOT NULL
    ) dates
    ON CONFLICT (full_date) DO NOTHING;

    -- ── dim_customers ────────────────────────────────────────────────
    INSERT INTO warehouse.dim_customers (
        customer_id, customer_unique_id, zip_code_prefix, city, state, region
    )
    SELECT
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        CASE customer_state
            WHEN 'SP' THEN 'Southeast' WHEN 'RJ' THEN 'Southeast'
            WHEN 'MG' THEN 'Southeast' WHEN 'ES' THEN 'Southeast'
            WHEN 'PR' THEN 'South'     WHEN 'SC' THEN 'South'
            WHEN 'RS' THEN 'South'     WHEN 'BA' THEN 'Northeast'
            WHEN 'CE' THEN 'Northeast' WHEN 'PE' THEN 'Northeast'
            WHEN 'GO' THEN 'Central-West' WHEN 'MT' THEN 'Central-West'
            WHEN 'MS' THEN 'Central-West' WHEN 'DF' THEN 'Central-West'
            WHEN 'AM' THEN 'North'     WHEN 'PA' THEN 'North'
            ELSE 'Other'
        END
    FROM staging.customers
    ON CONFLICT (customer_id) DO NOTHING;

    -- ── dim_sellers ──────────────────────────────────────────────────
    INSERT INTO warehouse.dim_sellers (
        seller_id, zip_code_prefix, city, state, region
    )
    SELECT
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state,
        CASE seller_state
            WHEN 'SP' THEN 'Southeast' WHEN 'RJ' THEN 'Southeast'
            WHEN 'MG' THEN 'Southeast' WHEN 'ES' THEN 'Southeast'
            WHEN 'PR' THEN 'South'     WHEN 'SC' THEN 'South'
            WHEN 'RS' THEN 'South'     WHEN 'BA' THEN 'Northeast'
            WHEN 'CE' THEN 'Northeast' WHEN 'PE' THEN 'Northeast'
            WHEN 'GO' THEN 'Central-West' WHEN 'MT' THEN 'Central-West'
            ELSE 'Other'
        END
    FROM staging.sellers
    ON CONFLICT (seller_id) DO NOTHING;

    -- ── dim_products ─────────────────────────────────────────────────
    INSERT INTO warehouse.dim_products (
        product_id, category_name_pt, category_name_en,
        name_length, description_length, photos_qty,
        weight_g, volume_cm3
    )
    SELECT
        p.product_id,
        p.product_category_name,
        ct.product_category_name_english,
        p.product_name_lenght,
        p.product_description_lenght,
        p.product_photos_qty,
        p.product_weight_g,
        ROUND(p.product_length_cm * p.product_height_cm * p.product_width_cm, 2)
    FROM staging.products p
    LEFT JOIN staging.category_translation ct
           ON p.product_category_name = ct.product_category_name
    ON CONFLICT (product_id) DO NOTHING;

    -- ── fact_orders ──────────────────────────────────────────────────
    INSERT INTO warehouse.fact_orders (
        order_id, customer_key, purchase_date_key,
        approved_date_key, delivered_date_key, estimated_delivery_date_key,
        order_status, delivery_delay_days, approval_time_hours
    )
    SELECT
        o.order_id,
        dc.customer_key,
        dp.date_id,
        da.date_id,
        dd_del.date_id,
        dd_est.date_id,
        o.order_status,
        CASE WHEN o.order_delivered_customer_date IS NOT NULL AND o.order_estimated_delivery_date IS NOT NULL
             THEN (o.order_delivered_customer_date - o.order_estimated_delivery_date)::INTEGER
             ELSE NULL END,
        CASE WHEN o.order_approved_at IS NOT NULL AND o.order_purchase_timestamp IS NOT NULL
             THEN ROUND(EXTRACT(EPOCH FROM (o.order_approved_at - o.order_purchase_timestamp)) / 3600, 2)
             ELSE NULL END
    FROM staging.orders o
    JOIN warehouse.dim_customers dc ON o.customer_id = dc.customer_id
    LEFT JOIN warehouse.dim_date dp     ON dp.full_date = o.order_purchase_timestamp::DATE
    LEFT JOIN warehouse.dim_date da     ON da.full_date = o.order_approved_at::DATE
    LEFT JOIN warehouse.dim_date dd_del ON dd_del.full_date = o.order_delivered_customer_date::DATE
    LEFT JOIN warehouse.dim_date dd_est ON dd_est.full_date = o.order_estimated_delivery_date::DATE
    ON CONFLICT (order_id) DO NOTHING;

    -- ── fact_order_items ─────────────────────────────────────────────
    INSERT INTO warehouse.fact_order_items (
        order_id, order_item_id, order_key, product_key, seller_key,
        price, freight_value, total_value
    )
    SELECT
        oi.order_id,
        oi.order_item_id,
        fo.order_key,
        dp.product_key,
        ds.seller_key,
        oi.price,
        oi.freight_value,
        ROUND(oi.price + oi.freight_value, 2)
    FROM staging.order_items oi
    JOIN warehouse.fact_orders fo  ON oi.order_id   = fo.order_id
    JOIN warehouse.dim_products dp ON oi.product_id = dp.product_id
    JOIN warehouse.dim_sellers ds  ON oi.seller_id  = ds.seller_id;

    -- ── fact_payments ────────────────────────────────────────────────
    INSERT INTO warehouse.fact_payments (
        order_id, payment_sequential, order_key,
        payment_type, installments, payment_value
    )
    SELECT
        p.order_id,
        p.payment_sequential,
        fo.order_key,
        p.payment_type,
        p.payment_installments,
        p.payment_value
    FROM staging.order_payments p
    JOIN warehouse.fact_orders fo ON p.order_id = fo.order_id;
    """
    _exec_sql(sql)
    logging.info("staging → warehouse: done")


def refresh_views(**_):
    """Refresh all materialized views."""
    logging.info("Refreshing materialized views")
    sql = """
    REFRESH MATERIALIZED VIEW warehouse.vw_revenue_by_state;
    REFRESH MATERIALIZED VIEW warehouse.vw_top_categories;
    REFRESH MATERIALIZED VIEW warehouse.vw_seller_performance;
    """
    _exec_sql(sql)
    logging.info("Views refreshed")


def push_to_elasticsearch(**_):
    """Index warehouse data into Elasticsearch."""
    ES_URL = "http://olist_elasticsearch:9200"
    INDEX = "olist_orders"

    logging.info("Pushing data to Elasticsearch index: %s", INDEX)

    conn = _pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    fo.order_id,
                    fo.order_status,
                    dc.customer_state,
                    dc.city AS customer_city,
                    dd.full_date AS purchase_date,
                    SUM(fp.payment_value) AS total_payment,
                    SUM(fi.total_value) AS total_item_value
                FROM warehouse.fact_orders fo
                JOIN warehouse.dim_customers dc ON fo.customer_key = dc.customer_key
                LEFT JOIN warehouse.dim_date dd ON fo.purchase_date_key = dd.date_id
                LEFT JOIN warehouse.fact_payments fp ON fo.order_key = fp.order_key
                LEFT JOIN warehouse.fact_order_items fi ON fo.order_key = fi.order_key
                GROUP BY fo.order_id, fo.order_status,
                         dc.customer_state, dc.city, dd.full_date
                LIMIT 50000
            """)
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
    finally:
        conn.close()

    # Bulk index
    bulk_body = ""
    for row in rows:
        doc = dict(zip(cols, [str(v) if v is not None else None for v in row]))
        bulk_body += json.dumps({"index": {"_index": INDEX, "_id": doc["order_id"]}}) + "\n"
        bulk_body += json.dumps(doc) + "\n"

    if bulk_body:
        resp = requests.post(
            f"{ES_URL}/_bulk",
            data=bulk_body,
            headers={"Content-Type": "application/x-ndjson"},
            timeout=60,
        )
        resp.raise_for_status()
        result = resp.json()
        errors = result.get("errors", False)
        logging.info("Bulk index done. Errors: %s, Items: %d", errors, len(result.get("items", [])))
    else:
        logging.warning("No data to push to Elasticsearch")


# ─── DAG definition ──────────────────────────────────────────────────────────

with DAG(
    dag_id="olist_pipeline_dag",
    description="Olist E-Commerce: raw → staging → warehouse → elasticsearch",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["olist", "yzv322e"],
) as dag:

    t1 = PythonOperator(
        task_id="raw_to_staging",
        python_callable=raw_to_staging,
    )

    t2 = PythonOperator(
        task_id="staging_to_warehouse",
        python_callable=staging_to_warehouse,
    )

    t3 = PythonOperator(
        task_id="refresh_materialized_views",
        python_callable=refresh_views,
    )

    t4 = PythonOperator(
        task_id="push_to_elasticsearch",
        python_callable=push_to_elasticsearch,
    )

    t1 >> t2 >> t3 >> t4
