-- etl-pipeline/sql_scripts/02_transform_silver.sql
-- Stage: SILVER — Clean, validate and transform raw Bronze tables into Silver
-- Includes: null healing, deduplication, type normalisation, quarantining bad rows

USE DATABASE OLIST_DB;

-- ── Create Silver schema ──────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS SILVER;
USE SCHEMA SILVER;

-- ── Silver Customers ──────────────────────────────────────────────
CREATE OR REPLACE TABLE SILVER_OLIST_CUSTOMERS_CLEAN AS
WITH deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _loaded_at DESC) AS rn
    FROM RAW.RAW_OLIST_CUSTOMERS
    WHERE customer_id IS NOT NULL
),
healed AS (
    SELECT
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        COALESCE(LOWER(TRIM(customer_city)), 'unknown')  AS customer_city,
        CASE
            WHEN UPPER(TRIM(customer_state)) IN (
                'AC','AL','AP','AM','BA','CE','DF','ES','GO','MA',
                'MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN',
                'RS','RO','RR','SC','SP','SE','TO'
            ) THEN UPPER(TRIM(customer_state))
            ELSE 'UNKNOWN'
        END AS customer_state,
        _loaded_at
    FROM deduped
    WHERE rn = 1
)
SELECT * FROM healed;

-- ── Silver Orders ─────────────────────────────────────────────────
CREATE OR REPLACE TABLE SILVER_OLIST_ORDERS_CLEAN AS
WITH deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _loaded_at DESC) AS rn
    FROM RAW.RAW_OLIST_ORDERS
    WHERE order_id IS NOT NULL AND customer_id IS NOT NULL
),
healed AS (
    SELECT
        order_id,
        customer_id,
        CASE
            WHEN order_status IN ('delivered','shipped','canceled','processing',
                                  'invoiced','approved','unavailable')
            THEN order_status
            ELSE 'unknown'
        END AS order_status,
        TRY_TO_TIMESTAMP(order_purchase_timestamp)      AS order_purchase_timestamp,
        TRY_TO_TIMESTAMP(order_approved_at)             AS order_approved_at,
        TRY_TO_TIMESTAMP(order_delivered_carrier_date)  AS order_delivered_carrier_date,
        TRY_TO_TIMESTAMP(order_delivered_customer_date) AS order_delivered_customer_date,
        TRY_TO_TIMESTAMP(order_estimated_delivery_date) AS order_estimated_delivery_date,
        _loaded_at
    FROM deduped
    WHERE rn = 1
)
SELECT * FROM healed;

-- ── Silver Payments ───────────────────────────────────────────────
CREATE OR REPLACE TABLE SILVER_OLIST_PAYMENTS_CLEAN AS
SELECT
    order_id,
    payment_sequential,
    CASE
        WHEN payment_type IN ('credit_card','boleto','voucher','debit_card')
        THEN payment_type
        ELSE 'boleto'
    END                                                 AS payment_type,
    GREATEST(COALESCE(payment_installments, 1), 1)      AS payment_installments,
    GREATEST(COALESCE(payment_value, 0.0), 0.0)         AS payment_value,
    _loaded_at
FROM RAW.RAW_OLIST_PAYMENTS
WHERE order_id IS NOT NULL;

-- ── Silver Products ───────────────────────────────────────────────
CREATE OR REPLACE TABLE SILVER_OLIST_PRODUCTS_CLEAN AS
SELECT
    product_id,
    COALESCE(product_category_name, 'unknown')          AS product_category_name,
    GREATEST(COALESCE(product_name_lenght, 0), 0)       AS product_name_lenght,
    GREATEST(COALESCE(product_description_lenght, 0), 0) AS product_description_lenght,
    GREATEST(COALESCE(product_photos_qty, 1), 1)        AS product_photos_qty,
    GREATEST(COALESCE(product_weight_g, 0), 0)          AS product_weight_g,
    GREATEST(COALESCE(product_length_cm, 0), 0)         AS product_length_cm,
    GREATEST(COALESCE(product_height_cm, 0), 0)         AS product_height_cm,
    GREATEST(COALESCE(product_width_cm, 0), 0)          AS product_width_cm,
    _loaded_at
FROM RAW.RAW_OLIST_PRODUCTS
WHERE product_id IS NOT NULL;

-- ── PII-masked Silver tables (SHA-256 IDs, partial zip) ───────────
CREATE OR REPLACE TABLE SILVER_OLIST_CUSTOMERS_MASKED AS
SELECT
    SHA2(customer_id, 256)          AS customer_id,
    SHA2(customer_unique_id, 256)   AS customer_unique_id,
    LEFT(customer_zip_code_prefix::VARCHAR, 2) || '***' AS customer_zip_code_prefix,
    customer_city,
    customer_state,
    _loaded_at
FROM SILVER_OLIST_CUSTOMERS_CLEAN;

CREATE OR REPLACE TABLE SILVER_OLIST_ORDERS_MASKED AS
SELECT
    SHA2(order_id, 256)     AS order_id,
    SHA2(customer_id, 256)  AS customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    _loaded_at
FROM SILVER_OLIST_ORDERS_CLEAN;

-- ── Quarantine bad rows (nulls/invalids not healed) ───────────────
INSERT INTO RAW.BRONZE_CUSTOMERS_QUARANTINE
    (customer_id, customer_unique_id, customer_zip_code_prefix,
     customer_city, customer_state, quarantine_reason)
SELECT
    customer_id, customer_unique_id, customer_zip_code_prefix,
    customer_city, customer_state,
    'null_customer_id_or_invalid_state'
FROM RAW.RAW_OLIST_CUSTOMERS
WHERE customer_id IS NULL
   OR customer_state NOT IN (
        'AC','AL','AP','AM','BA','CE','DF','ES','GO','MA',
        'MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN',
        'RS','RO','RR','SC','SP','SE','TO','UNKNOWN'
   );
