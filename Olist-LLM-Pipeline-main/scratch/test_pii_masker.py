import pandas as pd
from tools.snowflake_mcp_tool import mcp_tool

for tbl in ["SILVER_CUSTOMERS_CLEAN", "SILVER_OLIST_CLEAN", "SILVER_CUSTOMERS_MASKED", "SILVER_OLIST_MASKED"]:
    try:
        count = mcp_tool.call("snowflake_row_count", {"table": tbl})
        print(f"Row count of {tbl}: {count}")
    except Exception as e:
        print(f"Error querying {tbl}: {e}")

