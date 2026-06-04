"""
pipeline/batch_generator.py
Continuous batch generator — creates a fresh batch of Olist-like rows
every call, with randomised quality issues so the pipeline always has
real work to do: nulls, bad states, negative values, invalid categories, etc.

Each batch is a dict: {dataset_name: pd.DataFrame}
Quality issue injection rate varies randomly per batch (5–25%)
so the Heal Agent fires unpredictably — just like real production data.
"""
import uuid
import random
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

BR_STATES = [
    "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA",
    "MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN",
    "RS","RO","RR","SC","SP","SE","TO",
]
BR_CITIES = [
    "sao paulo","rio de janeiro","belo horizonte","curitiba","fortaleza",
    "manaus","salvador","recife","porto alegre","belem","goiania","florianopolis",
]
PRODUCT_CATEGORIES = [
    "cama_mesa_banho","beleza_saude","esporte_lazer","informatica_acessorios",
    "moveis_decoracao","utilidades_domesticas","auto","brinquedos","relogios_presentes",
    "ferramentas_jardim","cool_stuff","fashion_bolsas_e_acessorios",
]
PAYMENT_TYPES  = ["credit_card","boleto","voucher","debit_card"]
ORDER_STATUSES = ["delivered","shipped","canceled","processing","invoiced","approved"]

# Bad values intentionally injected
BAD_STATES   = ["XX","INVALID","null","SÃO PAULO","sp","rj"]
BAD_PAY_TYPES= ["cash","check","UNKNOWN","wire_transfer"]
BAD_STATUSES = ["pending","UNKNOWN","error","null"]


def _uid():
    return str(uuid.uuid4())


def _rand_ts(days_back: int = 730) -> str:
    d = datetime.now() - timedelta(days=random.randint(0, days_back))
    return d.strftime("%Y-%m-%d %H:%M:%S")


def _inject(series: pd.Series, bad_values: list, null_rate: float, bad_rate: float) -> pd.Series:
    """Inject nulls and bad categorical values."""
    s = series.copy().astype(object)
    n = len(s)
    # Nulls
    null_mask = np.random.random(n) < null_rate
    s[null_mask] = np.nan
    # Bad values
    if bad_values:
        bad_mask = (~null_mask) & (np.random.random(n) < bad_rate)
        s[bad_mask] = np.random.choice(bad_values, bad_mask.sum())
    return s


def generate_batch(
    dataset:     str  = "customers",
    batch_size:  int  = 200,
    quality_seed: int = None,
) -> pd.DataFrame:
    """
    Generate one batch of synthetic Olist data for the given dataset.
    quality_seed controls how dirty this batch is (None = random each call).
    Returns a pd.DataFrame ready to be saved as a Bronze CSV.
    """
    rng = random.Random(quality_seed)
    np_rng = np.random.RandomState(quality_seed)

    # Randomise issue rates per batch so healing varies
    null_rate = rng.uniform(0.02, 0.18)
    bad_rate  = rng.uniform(0.01, 0.12)
    neg_rate  = rng.uniform(0.01, 0.08)
    dup_rate  = rng.uniform(0.00, 0.05)

    if dataset == "customers":
        ids = [_uid() for _ in range(batch_size)]
        df  = pd.DataFrame({
            "customer_id":              ids,
            "customer_unique_id":       [_uid() for _ in range(batch_size)],
            "customer_zip_code_prefix": np_rng.randint(1000, 99999, batch_size),
            "customer_city":            np_rng.choice(BR_CITIES, batch_size),
            "customer_state":           np_rng.choice(BR_STATES, batch_size),
        })
        df["customer_id"]    = _inject(df["customer_id"],    [],         null_rate * 0.5, 0)
        df["customer_state"] = _inject(df["customer_state"], BAD_STATES, null_rate, bad_rate)

    elif dataset == "orders":
        df = pd.DataFrame({
            "order_id":                       [_uid() for _ in range(batch_size)],
            "customer_id":                    [_uid() for _ in range(batch_size)],
            "order_status":                   np_rng.choice(ORDER_STATUSES, batch_size,
                                                  p=[0.65,0.12,0.08,0.05,0.05,0.05]),
            "order_purchase_timestamp":       [_rand_ts() for _ in range(batch_size)],
            "order_approved_at":              [_rand_ts() for _ in range(batch_size)],
            "order_delivered_carrier_date":   [_rand_ts() for _ in range(batch_size)],
            "order_delivered_customer_date":  [_rand_ts() for _ in range(batch_size)],
            "order_estimated_delivery_date":  [_rand_ts() for _ in range(batch_size)],
        })
        df["customer_id"]  = _inject(df["customer_id"],  [],          null_rate, 0)
        df["order_status"] = _inject(df["order_status"], BAD_STATUSES, null_rate * 0.5, bad_rate)
        df["order_approved_at"] = _inject(df["order_approved_at"], [], null_rate * 1.5, 0)

    elif dataset == "payments":
        values = np_rng.exponential(scale=150, size=batch_size)
        # Inject negative values
        neg_mask = np_rng.random(batch_size) < neg_rate
        values[neg_mask] *= -1
        df = pd.DataFrame({
            "order_id":             [_uid() for _ in range(batch_size)],
            "payment_sequential":   np_rng.randint(1, 5, batch_size),
            "payment_type":         np_rng.choice(PAYMENT_TYPES, batch_size,
                                        p=[0.74, 0.19, 0.05, 0.02]),
            "payment_installments": np_rng.randint(1, 12, batch_size),
            "payment_value":        np.round(values, 2),
        })
        df["order_id"]      = _inject(df["order_id"],      [], null_rate * 0.5, 0)
        df["payment_type"]  = _inject(df["payment_type"],  BAD_PAY_TYPES, null_rate * 0.3, bad_rate)
        df["payment_value"] = _inject(df["payment_value"], [], null_rate * 0.3, 0)

    elif dataset == "products":
        df = pd.DataFrame({
            "product_id":                 [_uid() for _ in range(batch_size)],
            "product_category_name":      np_rng.choice(PRODUCT_CATEGORIES, batch_size),
            "product_name_lenght":        np_rng.randint(10, 80, batch_size).astype(float),
            "product_description_lenght": np_rng.randint(50, 3000, batch_size).astype(float),
            "product_photos_qty":         np_rng.randint(1, 10, batch_size).astype(float),
            "product_weight_g":           np_rng.randint(100, 30000, batch_size).astype(float),
            "product_length_cm":          np_rng.randint(10, 100, batch_size).astype(float),
            "product_height_cm":          np_rng.randint(5, 80, batch_size).astype(float),
            "product_width_cm":           np_rng.randint(10, 80, batch_size).astype(float),
        })
        df["product_category_name"] = _inject(df["product_category_name"],
                                               ["unknown_cat","outros"], null_rate, bad_rate * 0.5)
        df["product_weight_g"]      = _inject(df["product_weight_g"], [], null_rate * 0.5, 0)

    else:
        raise ValueError(f"Unknown dataset: {dataset}")

    # Duplicate rows
    if dup_rate > 0 and len(df) > 0:
        n_dups = max(1, int(len(df) * dup_rate))
        dups   = df.sample(n=min(n_dups, len(df)), random_state=42)
        df     = pd.concat([df, dups], ignore_index=True)

    return df


def generate_all_datasets(batch_size: int = 200, quality_seed: int = None) -> dict:
    """Generate one batch for all 4 Olist datasets."""
    return {
        ds: generate_batch(ds, batch_size=batch_size, quality_seed=quality_seed)
        for ds in ["customers", "orders", "payments", "products"]
    }
