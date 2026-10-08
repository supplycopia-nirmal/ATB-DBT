def model(dbt, session):
    dbt.config(materialized="table", packages=["pandas", "pyarrow"])
    import sys
    import os
    import shutil
    from pathlib import Path
    import pandas as pd
    import pyarrow as pa
    
    # Lineage registration
    df_cons = dbt.source("supplycopia_raw", "UHC_Consumption").df()
    df_con = dbt.source("supplycopia_raw", "UHC_CON_20260930010958").df()
    df_im = dbt.source("supplycopia_raw", "UHC_IM_20260930010918").df()
    
    root_dir = Path.cwd().parent
    sys.path.insert(0, str(root_dir))
    import uc_health_contract_mapping as ucm
    
    # We must wipe the output folder so it actually processes!
    out_dir = root_dir / "output"
    if out_dir.exists():
        shutil.rmtree(out_dir)
        
    def get_df_by_path(path):
        if "UHC_Consumption" in str(path):
            df = df_cons.copy()
        elif "UHC_CON" in str(path):
            df = df_con.copy()
        elif "UHC_IM" in str(path):
            df = df_im.copy()
        else:
            raise ValueError(f"Unknown path {path}")
            
        df = df.astype(str).replace('nan', '')
        return df
        
    def my_iter_csv_chunks(path, header, chunk_rows):
        df = get_df_by_path(path)
        # Yield in chunks
        for i in range(0, len(df), chunk_rows):
            chunk = df.iloc[i:i+chunk_rows]
            yield pa.Table.from_pandas(chunk, preserve_index=False)
            
    ucm.iter_csv_chunks = my_iter_csv_chunks
    
    def my_scan_consumption_vendor_items(path, header, cons_cols):
        df = get_df_by_path(path)
        # We only need vendor and item_id columns
        v_col = cons_cols["vendor"]
        i_col = cons_cols["item_id"]
        
        df = df[[v_col, i_col]].copy()
        df.columns = ["v", "i"]
        df = df.groupby(["v", "i"], dropna=False).size().rename("rows").reset_index()
        return df
        
    ucm.scan_consumption_vendor_items = my_scan_consumption_vendor_items
    
    cfg = ucm.Config(
        base_dir=root_dir,
        input_dir=root_dir / "Data",
        output_dir=out_dir,
        consumption_glob="UHC_Consumption.csv",
        contracts_file="UHC_CON_20260930010958.csv",
        item_master_file="UHC_IM_20260930010918.csv"
    )
    
    # Run the full pipeline logic!
    r = ucm.run_pipeline(cfg)
    
    out_parquet = cfg.output_dir / "final" / "consumption_enriched.parquet"
    df = pd.read_parquet(out_parquet)
    
    return df
