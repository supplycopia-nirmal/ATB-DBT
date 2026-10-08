import duckdb
import pandas as pd
import numpy as np

def main():
    con = duckdb.connect('uc_health/uc_health.duckdb', read_only=True)
    
    print("=== VALIDATING SPEND (PO) V3 ===")
    df_dbt_po = con.execute("SELECT * FROM fct_po_cost_savings_analysis_v3").fetchdf()
    df_orig_po = pd.read_csv('Spend/PO_enriched.csv', dtype=str, keep_default_na=False)
    
    print(f"DBT PO row count: {len(df_dbt_po)}")
    print(f"Original PO row count: {len(df_orig_po)}")
    if len(df_dbt_po) == len(df_orig_po):
        print("✅ PO Row count matches perfectly.")
    else:
        print("❌ PO Row count mismatch!")
        
    # Sort both dataframes by po_number and po_line_no
    df_dbt_po = df_dbt_po.sort_values(['po_number', 'po_line_no']).reset_index(drop=True)
    df_orig_po = df_orig_po.sort_values(['po_number', 'po_line_no']).reset_index(drop=True)
    
    df_dbt_po_prices = df_dbt_po['current_contract_price'].fillna('0').astype(str).str.replace(r'^\s*$', '0', regex=True).astype(float).values
    df_orig_po_prices = pd.to_numeric(df_orig_po['current_contract_price'].replace('', '0'), errors='coerce').fillna(0).values
    
    diff = np.abs(df_dbt_po_prices - df_orig_po_prices)
    matches = (diff < 0.01).sum()
    match_rate = matches / len(df_orig_po) * 100
    
    print(f"Current Contract Price matches exactly for {match_rate:.2f}% of rows ({matches}/{len(df_orig_po)}).")
    
    if match_rate >= 95.0:
        print("✅ PO Logic parity is excellent!")
    else:
        print("⚠️ Noticeable discrepancy in logic mapping.")

if __name__ == '__main__':
    main()
