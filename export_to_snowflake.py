import os
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv
import duckdb
import pandas as pd

def main():
    print("Loading environment variables...")
    load_dotenv()
    
    os.chdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), "uc_health"))

    private_key_path = os.getenv("PRIVATE_KEY_PATH", "")
    if not os.path.exists(private_key_path):
        private_key_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "lambda_migration._rsa_key.p8")
    private_key_passphrase = os.getenv("PRIVATE_KEY_PASSPHRASE")
    
    with open(private_key_path, "rb") as key_file:
        p_key = serialization.load_pem_private_key(
            key_file.read(),
            password=private_key_passphrase.encode() if private_key_passphrase else None,
            backend=default_backend()
        )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    ctx = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        private_key=pkb,
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

    db_name = os.getenv("SNOWFLAKE_DATABASE")
    schema_name = os.getenv("SNOWFLAKE_SCHEMA")
    try:
        ctx.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {db_name}.{schema_name}")
    except Exception as e:
        pass
    ctx.cursor().execute(f"USE DATABASE {db_name}")
    ctx.cursor().execute(f"USE SCHEMA {db_name}.{schema_name}")

    con = duckdb.connect('uc_health.duckdb', read_only=True)

    tables_to_export = [
        "int_consumption_profiled",
        "stg_invoice",
        "int_item_master_profiled",
        "fct_po_cost_savings_analysis_v3",
        "fct_consumption_cost_savings_analysis_v3",
        "fct_gap_analysis_v4",
        "fct_po_cost_savings_v4",
        "fct_consumption_cost_savings_v4"
    ]

    for duckdb_table in tables_to_export:
        print(f"\n--- Processing table: {duckdb_table} ---")
        df = con.execute(f'SELECT * FROM main.{duckdb_table}').fetchdf()
        print(f"Loaded {len(df)} rows from DuckDB.")
        
        df.columns = [c.upper() for c in df.columns]

        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.tz_localize(None)

        if duckdb_table.endswith('_v3') or duckdb_table.endswith('_v4'):
            snowflake_table = f"UC_HEALTH_{duckdb_table.upper()}"
        else:
            snowflake_table = f"UC_HEALTH_{duckdb_table.upper()}_V1"

        print(f"Pushing data to Snowflake table: {snowflake_table}...")

        success, nchunks, nrows, _ = write_pandas(
            ctx, 
            df, 
            snowflake_table, 
            database=db_name,
            schema=schema_name,
            auto_create_table=True, 
            overwrite=True
        )
        
        if success:
            print(f"Success! {nrows} rows written to {snowflake_table}.")
        else:
            print(f"Failed to write to {snowflake_table}.")

    print("\\nDropping UC_HEALTH_FCT_COST_SAVINGS_ANALYSIS_V1...")
    ctx.cursor().execute("DROP TABLE IF EXISTS UC_HEALTH_FCT_COST_SAVINGS_ANALYSIS_V1")

    con.close()
    ctx.close()
    print("\\nAll exports completed successfully!")

if __name__ == "__main__":
    main()
