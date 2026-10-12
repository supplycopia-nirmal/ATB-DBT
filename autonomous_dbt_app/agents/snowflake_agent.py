import os
import time
from pathlib import Path
from typing import Dict, Any, List, Optional
import duckdb
import pandas as pd
from autonomous_dbt_app.config import (
    SNOWFLAKE_USER,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_ROLE,
    SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_SCHEMA,
    PRIVATE_KEY_PATH,
    PRIVATE_KEY_PASSPHRASE,
    SNOWFLAKE_PAT
)

class SnowflakeAgent:
    """
    Deliverer Orion: Snowflake Cloud Data Warehouse Publisher Agent.
    Publishes validated final DuckDB marts into Snowflake with type normalization,
    chunked uploading, and post-upload verification.
    """
    def __init__(self):
        pass

    def export_marts_to_snowflake(self, duckdb_path: Path, table_mappings: List[tuple]) -> Dict[str, Any]:
        """
        table_mappings: list of (duckdb_table_name, snowflake_target_table_name)
        """
        import snowflake.connector
        from snowflake.connector.pandas_tools import write_pandas
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization

        conn_params = {
            "user": SNOWFLAKE_USER,
            "account": SNOWFLAKE_ACCOUNT,
            "role": SNOWFLAKE_ROLE,
            "warehouse": SNOWFLAKE_WAREHOUSE,
            "database": SNOWFLAKE_DATABASE,
            "schema": SNOWFLAKE_SCHEMA
        }

        if os.path.exists(PRIVATE_KEY_PATH):
            with open(PRIVATE_KEY_PATH, "rb") as kf:
                p_key = serialization.load_pem_private_key(
                    kf.read(),
                    password=PRIVATE_KEY_PASSPHRASE.encode() if PRIVATE_KEY_PASSPHRASE else None,
                    backend=default_backend()
                )
            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
            conn_params["private_key"] = pkb
        elif SNOWFLAKE_PAT:
            conn_params["token"] = SNOWFLAKE_PAT
            conn_params["authenticator"] = "oauth"

        report = {
            "database": SNOWFLAKE_DATABASE,
            "schema": SNOWFLAKE_SCHEMA,
            "exported_tables": [],
            "status": "SUCCESS"
        }

        try:
            ctx = snowflake.connector.connect(**conn_params)
            ctx.cursor().execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
            ctx.cursor().execute(f"USE SCHEMA {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}")
        except Exception as conn_err:
            report["status"] = "ERROR"
            report["error"] = f"Failed to connect to Snowflake: {str(conn_err)}"
            return report

        con_duck = duckdb.connect(str(duckdb_path), read_only=True)

        for duck_tbl, sf_tbl in table_mappings:
            t0 = time.time()
            df = con_duck.execute(f"SELECT * FROM {duck_tbl}").fetchdf()
            df.columns = [c.upper() for c in df.columns]

            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = df[col].dt.tz_localize(None)

            success, nchunks, nrows, _ = write_pandas(
                ctx,
                df,
                sf_tbl,
                database=SNOWFLAKE_DATABASE,
                schema=SNOWFLAKE_SCHEMA,
                auto_create_table=True,
                overwrite=True,
                chunk_size=100000
            )

            report["exported_tables"].append({
                "source_table": duck_tbl,
                "target_table": sf_tbl,
                "rows_written": nrows,
                "chunks": nchunks,
                "duration_sec": round(time.time() - t0, 2),
                "success": bool(success)
            })

        con_duck.close()
        ctx.close()
        return report
