-- etl-pipeline/sql_scripts/01_load_bronze.sql
-- Stage: BRONZE — Load raw Olist CSVs from S3/stage into raw Snowflake tables
-- Run once to set up initial raw tables; Airflow DAG calls this via SnowflakeOperator

-- ── Create database and schema ────────────────────────────────────
CREATE DATABASE IF NOT EXISTS OLIST_DB;
USE DATABASE OLIST_DB;
CREATE SCHEMA IF NOT EXISTS RAW;
USE SCHEMA RAW;

-- ── Customers ─────────────────────────────────────────────────────
CREATE OR REPLACE TABLE RAW_OLIST_CUSTOMERS (
    customer_id              VARCHAR(36),
    customer_unique_id       VARCHAR(36),
    customer_zip_code_prefix VARCHAR(10),
    customer_city            VARCHAR(100),
    customer_state           VARCHAR(10),
    _loaded_at               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ── Orders ────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE RAW_OLIST_ORDERS (
    order_id                      VARCHAR(36),
    customer_id                   VARCHAR(36),
    order_status                  VARCHAR(30),
    order_purchase_timestamp      VARCHAR(30),
    order_approved_at             VARCHAR(30),
    order_delivered_carrier_date  VARCHAR(30),
    order_delivered_customer_date VARCHAR(30),
    order_estimated_delivery_date VARCHAR(30),
    _loaded_at                    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ── Payments ──────────────────────────────────────────────────────
CREATE OR REPLACE TABLE RAW_OLIST_PAYMENTS (
    order_id              VARCHAR(36),
    payment_sequential    INTEGER,
    payment_type          VARCHAR(20),
    payment_installments  INTEGER,
    payment_value         FLOAT,
    _loaded_at            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ── Products ──────────────────────────────────────────────────────
CREATE OR REPLACE TABLE RAW_OLIST_PRODUCTS (
    product_id                   VARCHAR(36),
    product_category_name        VARCHAR(100),
    product_name_lenght          FLOAT,
    product_description_lenght   FLOAT,
    product_photos_qty           FLOAT,
    product_weight_g             FLOAT,
    product_length_cm            FLOAT,
    product_height_cm            FLOAT,
    product_width_cm             FLOAT,
    _loaded_at                   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ── Quarantine tables ─────────────────────────────────────────────
CREATE OR REPLACE TABLE BRONZE_CUSTOMERS_QUARANTINE LIKE RAW_OLIST_CUSTOMERS;
CREATE OR REPLACE TABLE BRONZE_ORDERS_QUARANTINE    LIKE RAW_OLIST_ORDERS;
CREATE OR REPLACE TABLE BRONZE_PAYMENTS_QUARANTINE  LIKE RAW_OLIST_PAYMENTS;
CREATE OR REPLACE TABLE BRONZE_PRODUCTS_QUARANTINE  LIKE RAW_OLIST_PRODUCTS;

-- Add quarantine metadata
ALTER TABLE BRONZE_CUSTOMERS_QUARANTINE ADD COLUMN quarantined_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
ALTER TABLE BRONZE_CUSTOMERS_QUARANTINE ADD COLUMN quarantine_reason VARCHAR(500);

-- ── Load from stage (replace @OLIST_STAGE with your S3/GCS stage) ─
-- COPY INTO RAW_OLIST_CUSTOMERS
--   FROM @OLIST_STAGE/olist_customers_dataset.csv
--   FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
--
-- COPY INTO RAW_OLIST_ORDERS
--   FROM @OLIST_STAGE/olist_orders_dataset.csv
--   FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
--
-- COPY INTO RAW_OLIST_PAYMENTS
--   FROM @OLIST_STAGE/olist_order_payments_dataset.csv
--   FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
--
-- COPY INTO RAW_OLIST_PRODUCTS
--   FROM @OLIST_STAGE/olist_products_dataset.csv
--   FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
