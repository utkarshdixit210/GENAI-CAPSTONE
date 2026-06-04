"""
tools/snowflake_mcp_tool.py
MCP connection layer — all data interactions go through here.
Nodes never touch data directly; they call mcp_tool.call().

Two modes:
  USE_LOCAL_CSV=true  → pandas/CSV on disk (default, no Snowflake required)
  USE_LOCAL_CSV=false → real Snowflake connector
"""
import os
import json
import pandas as pd
from pathlib import Path
from loguru import logger
from config.settings import SnowflakeConfig, PipelineConfig


class SnowflakeMCPTool:
    """
    Singleton MCP tool wrapping all Snowflake (or local CSV) operations.
    reconnect() lets the Heal Agent recover from connection failures.
    """

    def __init__(self):
        self.conn      = None
        self.use_local = PipelineConfig.USE_LOCAL_CSV
        if not self.use_local:
            self._connect()
        else:
            logger.info("MCP | LOCAL CSV mode — no Snowflake required")
            for layer in ["bronze", "silver", "gold"]:
                os.makedirs(f"{PipelineConfig.OUTPUTS_DIR}/{layer}", exist_ok=True)
            os.makedirs("metadata", exist_ok=True)

    # ── Connection ────────────────────────────────────────────────
    def _connect(self):
        import snowflake.connector
        logger.info("MCP | Connecting to Snowflake...")
        self.conn = snowflake.connector.connect(
            account=SnowflakeConfig.ACCOUNT, user=SnowflakeConfig.USER,
            password=SnowflakeConfig.PASSWORD, warehouse=SnowflakeConfig.WAREHOUSE,
            database=SnowflakeConfig.DATABASE, schema=SnowflakeConfig.SCHEMA,
            role=SnowflakeConfig.ROLE,
        )
        logger.success("MCP | Connected to Snowflake.")

    def reconnect(self):
        if self.use_local:
            logger.info("MCP | LOCAL mode — reconnect no-op"); return
        logger.warning("MCP | Reconnecting to Snowflake...")
        try: self.conn.close()
        except Exception: pass
        self._connect()

    # ── Unified call interface ────────────────────────────────────
    def call(self, tool_name: str, params: dict):
        logger.debug(f"MCP | {tool_name} | params={[k for k in params if k != 'df']}")
        return self._call_local(tool_name, params) if self.use_local \
               else self._call_snowflake(tool_name, params)

    # ── LOCAL CSV mode ────────────────────────────────────────────
    def _call_local(self, tool_name: str, params: dict):

        if tool_name == "snowflake_get_schema":
            df = self._load_csv(params["table"])
            schema = {col: str(df[col].dtype) for col in df.columns}
            logger.info(f"MCP | Schema: {list(schema.keys())}")
            return schema

        elif tool_name == "snowflake_sample_rows":
            df = self._load_csv(params["table"])
            rows = df.head(params.get("limit", 5)).fillna("").astype(str).to_dict(orient="records")
            logger.info(f"MCP | Sample: {len(rows)} rows")
            return rows

        elif tool_name == "snowflake_row_count":
            count = len(self._load_csv(params["table"]))
            logger.info(f"MCP | Row count {params['table']}: {count:,}")
            return count

        elif tool_name == "snowflake_execute_sql":
            sql = params.get("sql", "")
            logger.info(f"MCP | [LOCAL] SQL hint: {sql[:120]}")
            return {"status": "executed", "sql": sql, "local_mode": True}

        elif tool_name == "snowflake_fetch_data":
            df = self._load_csv(params["table"])
            logger.info(f"MCP | Fetched {len(df):,} rows from {params['table']}")
            return df

        elif tool_name == "snowflake_write_df":
            df    = params["df"]
            table = params["table"]
            layer = params.get("layer", "silver")
            path  = f"{PipelineConfig.OUTPUTS_DIR}/{layer}/{table}.csv"
            df.to_csv(path, index=False)
            logger.success(f"MCP | Written {len(df):,} rows → {path}")
            return {"status": "written", "path": path, "rows": len(df)}

        elif tool_name == "snowflake_read_table":
            table = params["table"]
            layer = params.get("layer", "silver")
            path  = f"{PipelineConfig.OUTPUTS_DIR}/{layer}/{table}.csv"
            if not Path(path).exists():
                raise FileNotFoundError(f"Table '{table}' not found at {path}")
            df = pd.read_csv(path)
            logger.info(f"MCP | Read {len(df):,} rows from {path}")
            return df

        elif tool_name == "snowflake_append_json":
            file    = params["file"]
            record  = params["record"]
            path    = f"metadata/{file}.json"
            records = []
            if Path(path).exists():
                with open(path) as f:
                    try: records = json.load(f)
                    except json.JSONDecodeError: records = []
            records.append(record)
            with open(path, "w") as f:
                json.dump(records, f, indent=2, default=str)
            logger.debug(f"MCP | Appended record → {path}")
            return {"status": "appended", "path": path}

        else:
            raise ValueError(f"Unknown MCP tool: {tool_name}")

    def _load_csv(self, table_name: str) -> pd.DataFrame:
        csv_map = {
            "RAW_OLIST_CUSTOMERS": f"{PipelineConfig.DATA_DIR}/olist_customers_dataset.csv",
            "RAW_OLIST_ORDERS":    f"{PipelineConfig.DATA_DIR}/olist_orders_dataset.csv",
            "RAW_OLIST_PAYMENTS":  f"{PipelineConfig.DATA_DIR}/olist_order_payments_dataset.csv",
            "RAW_OLIST_PRODUCTS":  f"{PipelineConfig.DATA_DIR}/olist_products_dataset.csv",
        }
        path = csv_map.get(table_name)
        if path and Path(path).exists():
            return pd.read_csv(path)
        for layer in ["silver", "bronze", "gold"]:
            p = f"{PipelineConfig.OUTPUTS_DIR}/{layer}/{table_name}.csv"
            if Path(p).exists():
                return pd.read_csv(p)
        raise FileNotFoundError(
            f"CSV for table '{table_name}' not found. "
            f"Expected: {path or f'outputs/*/{table_name}.csv'}"
        )

    # ── SNOWFLAKE mode ────────────────────────────────────────────
    def _call_snowflake(self, tool_name: str, params: dict):
        import snowflake.connector
        logger.info(f"DEBUG_MCP_CALL | {tool_name} | table={params.get('table')}")
        cursor = self.conn.cursor()
        try:
            if tool_name == "snowflake_get_schema":
                cursor.execute(f"DESCRIBE TABLE {params['table']}")
                return {row[0].lower(): row[1] for row in cursor.fetchall()}

            elif tool_name == "snowflake_sample_rows":
                cursor.execute(f"SELECT * FROM {params['table']} LIMIT {params.get('limit',5)}")
                cols = [d[0].lower() for d in cursor.description]
                return [dict(zip(cols, row)) for row in cursor.fetchall()]

            elif tool_name == "snowflake_row_count":
                cursor.execute(f"SELECT COUNT(*) FROM {params['table']}")
                return cursor.fetchone()[0]

            elif tool_name == "snowflake_execute_sql":
                sql = params["sql"].strip()
                # If there are multiple statements, execute them one by one
                statements = [s.strip() for s in sql.split(";") if s.strip()]
                for stmt in statements:
                    cursor.execute(stmt)
                self.conn.commit()
                return {"status": "executed", "statements_count": len(statements)}

            elif tool_name in ("snowflake_fetch_data", "snowflake_read_table"):
                cursor.execute(f"SELECT * FROM {params['table']}")
                cols = [d[0].lower() for d in cursor.description]
                df = pd.DataFrame([dict(zip(cols, r)) for r in cursor.fetchall()])
                return df

            elif tool_name == "snowflake_write_df":
                from snowflake.connector.pandas_tools import write_pandas
                df  = params["df"].copy()
                tbl = params["table"].upper()
                # Always uppercase column names — Snowflake requirement
                df.columns = [c.upper() for c in df.columns]
                # Drop the table first so stale column schemas never block the write
                drop_cur = self.conn.cursor()
                try:
                    drop_cur.execute(f"DROP TABLE IF EXISTS {tbl}")
                    self.conn.commit()
                except Exception:
                    pass
                finally:
                    drop_cur.close()
                success, _, nrows, _ = write_pandas(
                    self.conn, df, tbl,
                    auto_create_table=True, overwrite=False
                )
                logger.success(f"MCP | Written {nrows:,} rows → {tbl}")
                return {"status": "written", "rows": nrows, "table": tbl}


            elif tool_name == "snowflake_append_json":
                record = params["record"]
                table  = params.get("table", "PIPELINE_METADATA")
                json_str = json.dumps(record, default=str, ensure_ascii=True)
                # Use parameterized query to avoid JSON escaping issues
                sql = (
                    f"CREATE TABLE IF NOT EXISTS {table} "
                    f"(RECORD_JSON VARIANT, RECORDED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())"
                )
                cursor.execute(sql)
                insert_sql = f"INSERT INTO {table} (RECORD_JSON) SELECT PARSE_JSON(%s)"
                cursor.execute(insert_sql, (json_str,))
                self.conn.commit()
                return {"status": "appended"}


            else:
                raise ValueError(f"Unknown MCP tool: {tool_name}")

        except Exception as e:
            logger.error(f"MCP | Snowflake error in {tool_name}: {e}")
            raise
        finally:
            cursor.close()


# Singleton — imported by all nodes
mcp_tool = SnowflakeMCPTool()
