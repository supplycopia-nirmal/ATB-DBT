import os, sys, time
sys.path.insert(0, '/Users/piyu/Library/Python/3.9/lib/python/site-packages')
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
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    key_path = os.getenv("PRIVATE_KEY_PATH", "")
    if not os.path.exists(key_path):
        key_path = os.path.join(script_dir, "lambda_migration._rsa_key.p8")
    
    print(f"Using private key from: {key_path}")
    private_key_passphrase = os.getenv("PRIVATE_KEY_PASSPHRASE")
    
    with open(key_path, "rb") as key_file:
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

    db_name = os.getenv("SNOWFLAKE_DATABASE")
    schema_name = os.getenv("SNOWFLAKE_SCHEMA")
    
    print(f"Connecting to Snowflake ({db_name}.{schema_name})...")
    ctx = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        private_key=pkb,
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=db_name,
        schema=schema_name
    )

    ctx.cursor().execute(f"USE DATABASE {db_name}")
    ctx.cursor().execute(f"USE SCHEMA {db_name}.{schema_name}")

    duckdb_path = os.path.join(script_dir, "uc_health", "uc_health.duckdb")
    print(f"Opening DuckDB at {duckdb_path}...")
    con = duckdb.connect(duckdb_path, read_only=True)

    tables_to_export = [
        ("fct_gap_analysis_v4", "UC_HEALTH_FCT_GAP_ANALYSIS_V4"),
        ("fct_po_cost_savings_v4", "UC_HEALTH_FCT_PO_COST_SAVINGS_V4"),
        ("fct_consumption_cost_savings_v4", "UC_HEALTH_FCT_CONSUMPTION_COST_SAVINGS_V4")
    ]

    for duckdb_table, snowflake_table in tables_to_export:
        t0 = time.time()
        print(f"\n==========================================")
        print(f"Exporting {duckdb_table} -> {snowflake_table}")
        print(f"==========================================")
        
        row_count = con.execute(f"SELECT count(*) FROM main.{duckdb_table}").fetchone()[0]
        print(f"Source row count in DuckDB: {row_count:,}")
        
        # Load in chunks or full dataframe
        df = con.execute(f"SELECT * FROM main.{duckdb_table}").fetchdf()
        df.columns = [c.upper() for c in df.columns]

        # Convert timestamps for snowflake compatibility
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.tz_localize(None)

        print(f"Pushing {len(df):,} rows to Snowflake ({snowflake_table})...")
        success, nchunks, nrows, _ = write_pandas(
            ctx, 
            df, 
            snowflake_table, 
            database=db_name,
            schema=schema_name,
            auto_create_table=True, 
            overwrite=True,
            chunk_size=100000
        )
        
        if success:
            print(f"SUCCESS! {nrows:,} rows written across {nchunks} chunks in {time.time()-t0:.1f}s.")
        else:
            print(f"FAILED to write to {snowflake_table}.")
            sys.exit(1)

    con.close()
    ctx.close()
    print("\nALL V4 TABLES EXPORTED TO SNOWFLAKE SUCCESSFULLY!")

if __name__ == "__main__":
    main()
