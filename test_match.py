import duckdb
import pandas as pd

def main():
    con = duckdb.connect('uc_health/uc_health.duckdb', read_only=True)
    df_dbt = con.execute("SELECT * FROM test_source").fetchdf()
    
    df_raw = pd.read_csv('Data/UHC_PO_20260930010955.csv', sep='|', dtype=str, keep_default_na=False, engine='pyarrow', encoding='utf-8')
    df_raw.columns = [c.strip() for c in df_raw.columns]
    df_raw = df_raw.apply(lambda s: s.str.strip())
    
    print(f"DBT row count: {len(df_dbt)}")
    print(f"RAW row count: {len(df_raw)}")
    
    # Check if they are completely identical
    df_dbt = df_dbt.astype(str).fillna('')
    df_raw = df_raw.astype(str).fillna('')
    
    diff = len(df_dbt) - len(df_raw)
    print(f"Row diff: {diff}")

if __name__ == '__main__':
    main()
