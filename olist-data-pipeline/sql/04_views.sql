-- ============================================================
-- 04_views.sql
-- Materialized views for common analytical queries
-- ============================================================

-- Revenue by customer state, aggregated monthly
CREATE MATERIALIZED VIEW IF NOT EXISTS warehouse.vw_revenue_by_state AS
SELECT
    dc.state,
    dd.year,
    dd.month,
    dd.month_name,
    COUNT(DISTINCT fo.order_id)         AS total_orders,
    SUM(fp.payment_value)               AS total_revenue,
    AVG(fp.payment_value)               AS avg_order_value
FROM warehouse.fact_orders fo
JOIN warehouse.dim_customers dc  ON fo.customer_key  = dc.customer_key
JOIN warehouse.dim_date dd       ON fo.purchase_date_key = dd.date_id
JOIN warehouse.fact_payments fp  ON fo.order_key     = fp.order_key
GROUP BY dc.state, dd.year, dd.month, dd.month_name
ORDER BY dd.year, dd.month, total_revenue DESC;

-- Top product categories by total revenue
CREATE MATERIALIZED VIEW IF NOT EXISTS warehouse.vw_top_categories AS
SELECT
    dp.category_name_en                 AS category,
    COUNT(DISTINCT fi.order_id)         AS total_orders,
    SUM(fi.total_value)                 AS total_revenue,
    AVG(fi.price)                       AS avg_price,
    SUM(fi.freight_value)               AS total_freight
FROM warehouse.fact_order_items fi
JOIN warehouse.dim_products dp ON fi.product_key = dp.product_key
WHERE dp.category_name_en IS NOT NULL
GROUP BY dp.category_name_en
ORDER BY total_revenue DESC;

-- Seller performance: order volume and revenue contribution
CREATE MATERIALIZED VIEW IF NOT EXISTS warehouse.vw_seller_performance AS
SELECT
    ds.seller_id,
    ds.city                             AS seller_city,
    ds.state                            AS seller_state,
    COUNT(DISTINCT fi.order_id)         AS total_orders,
    COUNT(fi.item_key)                  AS total_items_sold,
    SUM(fi.price)                       AS total_revenue,
    SUM(fi.freight_value)               AS total_freight_charged,
    AVG(fi.price)                       AS avg_item_price,
    ROUND(
        SUM(fi.price) * 100.0 /
        NULLIF(SUM(SUM(fi.price)) OVER (), 0), 4
    )                                   AS revenue_share_pct
FROM warehouse.fact_order_items fi
JOIN warehouse.dim_sellers ds ON fi.seller_key = ds.seller_key
GROUP BY ds.seller_id, ds.city, ds.state
ORDER BY total_revenue DESC;

-- Create indexes on materialized views for fast querying
CREATE INDEX IF NOT EXISTS idx_revenue_state ON warehouse.vw_revenue_by_state (state);
CREATE INDEX IF NOT EXISTS idx_revenue_year_month ON warehouse.vw_revenue_by_state (year, month);
CREATE INDEX IF NOT EXISTS idx_top_cat ON warehouse.vw_top_categories (total_revenue DESC);
CREATE INDEX IF NOT EXISTS idx_seller_perf ON warehouse.vw_seller_performance (total_revenue DESC);
