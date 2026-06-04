-- etl-pipeline/sql_scripts/03_calculate_gold.sql
-- Stage: GOLD — Business KPI aggregations from Silver clean tables
-- All metrics derived from verified, quality-passed Silver data

USE DATABASE OLIST_DB;
CREATE SCHEMA IF NOT EXISTS GOLD;
USE SCHEMA GOLD;

-- ── Customer KPIs ─────────────────────────────────────────────────
CREATE OR REPLACE TABLE GOLD_CUSTOMER_KPIS AS
SELECT
    COUNT(*)                                    AS total_customers,
    COUNT(DISTINCT customer_unique_id)          AS unique_customers,
    COUNT(DISTINCT customer_state)              AS states_covered,
    COUNT(DISTINCT customer_city)               AS cities_covered,
    CURRENT_TIMESTAMP()                         AS computed_at
FROM SILVER.SILVER_OLIST_CUSTOMERS_CLEAN;

-- Top 5 states by customer count
CREATE OR REPLACE TABLE GOLD_CUSTOMER_STATE_BREAKDOWN AS
SELECT
    customer_state,
    COUNT(*)                                    AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM SILVER.SILVER_OLIST_CUSTOMERS_CLEAN
WHERE customer_state != 'UNKNOWN'
GROUP BY customer_state
ORDER BY customer_count DESC
LIMIT 10;

-- ── Order KPIs ────────────────────────────────────────────────────
CREATE OR REPLACE TABLE GOLD_ORDER_KPIS AS
SELECT
    COUNT(*)                                            AS total_orders,
    COUNT(DISTINCT customer_id)                         AS unique_customers_with_orders,
    SUM(CASE WHEN order_status = 'delivered'  THEN 1 ELSE 0 END) AS delivered_orders,
    SUM(CASE WHEN order_status = 'canceled'   THEN 1 ELSE 0 END) AS canceled_orders,
    SUM(CASE WHEN order_status = 'shipped'    THEN 1 ELSE 0 END) AS shipped_orders,
    ROUND(SUM(CASE WHEN order_status = 'delivered' THEN 1 ELSE 0 END)
          * 100.0 / COUNT(*), 2)                        AS delivery_rate_pct,
    ROUND(SUM(CASE WHEN order_status = 'canceled' THEN 1 ELSE 0 END)
          * 100.0 / COUNT(*), 2)                        AS cancellation_rate_pct,
    CURRENT_TIMESTAMP()                                 AS computed_at
FROM SILVER.SILVER_OLIST_ORDERS_CLEAN;

-- Monthly order volume
CREATE OR REPLACE TABLE GOLD_MONTHLY_ORDER_VOLUME AS
SELECT
    DATE_TRUNC('month', order_purchase_timestamp)       AS order_month,
    COUNT(*)                                            AS order_count,
    COUNT(DISTINCT customer_id)                         AS unique_customers
FROM SILVER.SILVER_OLIST_ORDERS_CLEAN
WHERE order_purchase_timestamp IS NOT NULL
GROUP BY 1
ORDER BY 1;

-- ── Revenue KPIs ──────────────────────────────────────────────────
CREATE OR REPLACE TABLE GOLD_REVENUE_KPIS AS
SELECT
    ROUND(SUM(p.payment_value), 2)                      AS total_revenue_brl,
    ROUND(AVG(p.payment_value), 2)                      AS avg_order_value_brl,
    ROUND(MEDIAN(p.payment_value), 2)                   AS median_order_value_brl,
    MAX(p.payment_value)                                AS max_order_value_brl,
    MIN(NULLIF(p.payment_value, 0))                     AS min_order_value_brl,
    SUM(CASE WHEN p.payment_type = 'credit_card'        THEN p.payment_value ELSE 0 END) AS credit_card_revenue,
    SUM(CASE WHEN p.payment_type = 'boleto'             THEN p.payment_value ELSE 0 END) AS boleto_revenue,
    ROUND(AVG(p.payment_installments), 2)               AS avg_installments,
    CURRENT_TIMESTAMP()                                 AS computed_at
FROM SILVER.SILVER_OLIST_PAYMENTS_CLEAN p;

-- Revenue by payment type
CREATE OR REPLACE TABLE GOLD_REVENUE_BY_PAYMENT_TYPE AS
SELECT
    payment_type,
    COUNT(*)                                            AS transaction_count,
    ROUND(SUM(payment_value), 2)                        AS total_revenue,
    ROUND(AVG(payment_value), 2)                        AS avg_transaction_value,
    ROUND(SUM(payment_value) * 100.0 / SUM(SUM(payment_value)) OVER (), 2) AS pct_of_total_revenue
FROM SILVER.SILVER_OLIST_PAYMENTS_CLEAN
GROUP BY payment_type
ORDER BY total_revenue DESC;

-- ── Product KPIs ──────────────────────────────────────────────────
CREATE OR REPLACE TABLE GOLD_PRODUCT_KPIS AS
SELECT
    COUNT(*)                                            AS total_products,
    COUNT(DISTINCT product_category_name)               AS unique_categories,
    ROUND(AVG(product_weight_g), 2)                     AS avg_weight_g,
    ROUND(AVG(product_photos_qty), 2)                   AS avg_photos_per_product,
    CURRENT_TIMESTAMP()                                 AS computed_at
FROM SILVER.SILVER_OLIST_PRODUCTS_CLEAN;

-- Top product categories
CREATE OR REPLACE TABLE GOLD_TOP_PRODUCT_CATEGORIES AS
SELECT
    product_category_name,
    COUNT(*)                                            AS product_count,
    ROUND(AVG(product_weight_g), 2)                     AS avg_weight_g,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_catalog
FROM SILVER.SILVER_OLIST_PRODUCTS_CLEAN
WHERE product_category_name != 'unknown'
GROUP BY product_category_name
ORDER BY product_count DESC
LIMIT 20;

-- ── Pipeline Audit tables ─────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS OLIST_DB.AUDIT;
USE SCHEMA OLIST_DB.AUDIT;

CREATE TABLE IF NOT EXISTS PIPELINE_AUDIT_LOG (
    audit_id         VARCHAR(100),
    run_id           VARCHAR(50),
    dataset          VARCHAR(30),
    recorded_at      TIMESTAMP_NTZ,
    node_name        VARCHAR(80),
    error_type       VARCHAR(50),
    error_message    VARCHAR(1000),
    fix_applied      VARCHAR(1000),
    retry_number     INTEGER,
    pipeline_status  VARCHAR(30),
    clean_rows       INTEGER,
    quarantine_rows  INTEGER,
    raw_row_count    INTEGER,
    schema_drifts    INTEGER,
    heal_count       INTEGER,
    inserted_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS PIPELINE_LINEAGE (
    run_id          VARCHAR(50),
    dataset         VARCHAR(30),
    source_table    VARCHAR(80),
    target_table    VARCHAR(80),
    transformation  VARCHAR(80),
    description     VARCHAR(500),
    rows_in         INTEGER,
    rows_out        INTEGER,
    recorded_at     TIMESTAMP_NTZ,
    inserted_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS SCHEMA_DRIFT_LOG (
    run_id          VARCHAR(50),
    dataset         VARCHAR(30),
    drift_type      VARCHAR(40),
    column_name     VARCHAR(100),
    severity        VARCHAR(10),
    detected_at     TIMESTAMP_NTZ,
    inserted_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
