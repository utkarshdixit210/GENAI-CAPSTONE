import os
import uuid
import random
import pandas as pd
import numpy as np

# Ensure data directory exists
os.makedirs("data", exist_ok=True)

print("Generating synthetic datasets to fill 'data/' folder...")

# Configuration
n_rows = 500

BR_STATES = ["SP", "RJ", "MG", "RS", "PR", "SC", "BA", "PE", "CE", "DF"]
BR_CITIES = ["sao paulo", "rio de janeiro", "belo horizonte", "porto alegre", "curitiba", "florianopolis", "salvador", "recife", "fortaleza", "brasilia"]
PAYMENT_TYPES = ["credit_card", "boleto", "voucher", "debit_card"]
ORDER_STATUSES = ["delivered", "shipped", "canceled", "processing", "invoiced", "approved"]
PRODUCT_CATEGORIES = ["cama_mesa_banho", "beleza_saude", "esporte_lazer", "informatica_acessorios", "moveis_decoracao", "utilidades_domesticas"]

# 1. Customers
customer_ids = [str(uuid.uuid4()) for _ in range(n_rows)]
customer_unique_ids = [str(uuid.uuid4()) for _ in range(n_rows)]
customers_df = pd.DataFrame({
    "customer_id": customer_ids,
    "customer_unique_id": customer_unique_ids,
    "customer_zip_code_prefix": [str(random.randint(1000, 99999)) for _ in range(n_rows)],
    "customer_city": [random.choice(BR_CITIES) for _ in range(n_rows)],
    "customer_state": [random.choice(BR_STATES) for _ in range(n_rows)]
})
customers_df.to_csv("data/olist_customers_dataset.csv", index=False)
print("  ✓ Created data/olist_customers_dataset.csv")

# 2. Orders
order_ids = [str(uuid.uuid4()) for _ in range(n_rows)]
orders_df = pd.DataFrame({
    "order_id": order_ids,
    "customer_id": [random.choice(customer_ids) for _ in range(n_rows)],
    "order_status": [random.choice(ORDER_STATUSES) for _ in range(n_rows)],
    "order_purchase_timestamp": ["2026-06-01 10:00:00" for _ in range(n_rows)],
    "order_approved_at": ["2026-06-01 10:05:00" for _ in range(n_rows)],
    "order_delivered_carrier_date": ["2026-06-02 14:00:00" for _ in range(n_rows)],
    "order_delivered_customer_date": ["2026-06-04 12:00:00" for _ in range(n_rows)],
    "order_estimated_delivery_date": ["2026-06-06 18:00:00" for _ in range(n_rows)]
})
orders_df.to_csv("data/olist_orders_dataset.csv", index=False)
print("  ✓ Created data/olist_orders_dataset.csv")

# 3. Payments
payments_df = pd.DataFrame({
    "order_id": [random.choice(order_ids) for _ in range(n_rows)],
    "payment_sequential": [1 for _ in range(n_rows)],
    "payment_type": [random.choice(PAYMENT_TYPES) for _ in range(n_rows)],
    "payment_installments": [random.randint(1, 12) for _ in range(n_rows)],
    "payment_value": [round(random.uniform(10.0, 500.0), 2) for _ in range(n_rows)]
})
payments_df.to_csv("data/olist_order_payments_dataset.csv", index=False)
print("  ✓ Created data/olist_order_payments_dataset.csv")

# 4. Products
product_ids = [str(uuid.uuid4()) for _ in range(n_rows)]
products_df = pd.DataFrame({
    "product_id": product_ids,
    "product_category_name": [random.choice(PRODUCT_CATEGORIES) for _ in range(n_rows)],
    "product_name_lenght": [random.randint(10, 70) for _ in range(n_rows)],
    "product_description_lenght": [random.randint(100, 1000) for _ in range(n_rows)],
    "product_photos_qty": [random.randint(1, 5) for _ in range(n_rows)],
    "product_weight_g": [random.randint(100, 15000) for _ in range(n_rows)],
    "product_length_cm": [random.randint(15, 60) for _ in range(n_rows)],
    "product_height_cm": [random.randint(5, 50) for _ in range(n_rows)],
    "product_width_cm": [random.randint(15, 60) for _ in range(n_rows)]
})
products_df.to_csv("data/olist_products_dataset.csv", index=False)
print("  ✓ Created data/olist_products_dataset.csv")

print("\nAll initial datasets generated successfully!")
