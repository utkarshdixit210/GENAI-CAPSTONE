"""
config/settings.py
Centralised settings loaded from environment variables.
Supports Snowflake (production) and local CSV (dev) modes.
Dataset: Olist Brazilian E-Commerce
"""
import os
from dotenv import load_dotenv

load_dotenv()


class SnowflakeConfig:
    ACCOUNT    = os.getenv("SNOWFLAKE_ACCOUNT")
    USER       = os.getenv("SNOWFLAKE_USER")
    PASSWORD   = os.getenv("SNOWFLAKE_PASSWORD")
    WAREHOUSE  = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    DATABASE   = os.getenv("SNOWFLAKE_DATABASE",  "OLIST_DB")
    SCHEMA     = os.getenv("SNOWFLAKE_SCHEMA",    "RAW")
    ROLE       = os.getenv("SNOWFLAKE_ROLE",       "SYSADMIN")


class LLMConfig:
    API_KEY    = os.getenv("ANTHROPIC_API_KEY") or os.getenv("OPENAI_API_KEY") or os.getenv("GROQ_API_KEY")
    MODEL      = os.getenv("LLM_MODEL",    "claude-sonnet-4-5")
    PROVIDER   = os.getenv("LLM_PROVIDER", "anthropic")   # 'anthropic' | 'openai' | 'groq'


class PipelineConfig:
    MAX_RETRIES           = int(os.getenv("MAX_RETRIES_PER_NODE", 3))
    LOG_LEVEL             = os.getenv("LOG_LEVEL", "INFO")

    # Raw table names (Snowflake) / CSV identifiers (local)
    RAW_TABLE_CUSTOMERS   = os.getenv("RAW_TABLE_CUSTOMERS", "RAW_OLIST_CUSTOMERS")
    RAW_TABLE_ORDERS      = os.getenv("RAW_TABLE_ORDERS",    "RAW_OLIST_ORDERS")
    RAW_TABLE_PAYMENTS    = os.getenv("RAW_TABLE_PAYMENTS",  "RAW_OLIST_PAYMENTS")
    RAW_TABLE_PRODUCTS    = os.getenv("RAW_TABLE_PRODUCTS",  "RAW_OLIST_PRODUCTS")

    # Which dataset to run (customers | orders | payments | products)
    ACTIVE_DATASET        = os.getenv("ACTIVE_DATASET", "customers")

    # Output table names
    CLEAN_TABLE           = os.getenv("CLEAN_TABLE",      "SILVER_OLIST_CLEAN")
    QUARANTINE_TABLE      = os.getenv("QUARANTINE_TABLE", "BRONZE_QUARANTINE")
    MASKED_TABLE          = os.getenv("MASKED_TABLE",     "SILVER_OLIST_MASKED")
    GOLD_TABLE            = os.getenv("GOLD_TABLE",       "GOLD_OLIST_KPIS")
    LINEAGE_TABLE         = os.getenv("LINEAGE_TABLE",    "PIPELINE_LINEAGE")
    AUDIT_TABLE           = os.getenv("AUDIT_TABLE",      "PIPELINE_AUDIT_LOG")
    DRIFT_LOG_TABLE       = os.getenv("DRIFT_LOG_TABLE",  "SCHEMA_DRIFT_LOG")

    # Paths
    DATA_DIR              = os.getenv("DATA_DIR",     "data")
    OUTPUTS_DIR           = os.getenv("OUTPUTS_DIR",  "outputs")

    # Set to "true" to run without Snowflake (uses local CSVs)
    USE_LOCAL_CSV         = os.getenv("USE_LOCAL_CSV", "true").lower() == "true"
