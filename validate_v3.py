import duckdb
import pandas as pd
import numpy as np

def main():
    con = duckdb.connect('uc_health/uc_health.duckdb', read_only=True)
    
    print("=== VALIDATING SPEND (PO) V3 ===")
    df_dbt_po = con.execute("SELECT * FROM fct_po_cost_savings_analysis_v3").fetchdf()
    df_orig_po = pd.read_csv('Spend/PO_enriched.csv', dtype=str)
    
    print(f"DBT PO row count: {len(df_dbt_po)}")
    print(f"Original PO row count: {len(df_orig_po)}")
    if len(df_dbt_po) == len(df_orig_po):
        print("✅ PO Row count matches perfectly.")
    else:
        print("❌ PO Row count mismatch!")
        
    print("\n=== VALIDATING CONSUMPTION V3 ===")
    df_dbt_cons = con.execute("SELECT * FROM fct_consumption_cost_savings_analysis_v3 ORDER BY ITEM_NUMBER, ADMIT_DATE_TIME").fetchdf()
    df_orig_cons = con.execute("SELECT * FROM read_parquet('Consumption/consumption_enriched.parquet') ORDER BY ITEM_NUMBER, ADMIT_DATE_TIME").fetchdf()
    
    print(f"DBT Consumption row count: {len(df_dbt_cons)}")
    print(f"Original Consumption row count: {len(df_orig_cons)}")
    
    if len(df_dbt_cons) == len(df_orig_cons):
        print("✅ Consumption Row count matches perfectly.")
    else:
        print("❌ Consumption Row count mismatch!")

    # Compare current_contract_price
    df_dbt_cons_prices = df_dbt_cons['current_contract_price'].fillna(0).astype(float).values
    df_orig_cons_prices = df_orig_cons['current_contract_price'].fillna(0).astype(float).values
    
    diff = np.abs(df_dbt_cons_prices - df_orig_cons_prices)
    matches = (diff < 0.01).sum()
    match_rate = matches / len(df_orig_cons) * 100
    
    print(f"Current Contract Price matches exactly for {match_rate:.2f}% of rows ({matches}/{len(df_orig_cons)}).")
    
    if match_rate >= 95.0:
        print("✅ Consumption Logic parity is excellent!")
    else:
        print("⚠️ Noticeable discrepancy in logic mapping vs the complex python script.")

if __name__ == '__main__':
    main()
