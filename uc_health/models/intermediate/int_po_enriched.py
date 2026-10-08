def model(dbt, session):
    dbt.config(materialized="table", packages=["pandas", "pyarrow", "openpyxl"])
    import sys
    import os
    from pathlib import Path
    import pandas as pd
    import pyarrow as pa
    
    # Lineage registration
    df_po = dbt.source("supplycopia_raw", "UHC_PO_20260930010955").df()
    df_con = dbt.source("supplycopia_raw", "UHC_CON_20260930010958").df()
    df_im = dbt.source("supplycopia_raw", "UHC_IM_20260930010918").df()
    df_inv = dbt.source("supplycopia_raw", "UHC_INV_20260930010902").df()
    
    root_dir = Path.cwd().parent
    sys.path.insert(0, str(root_dir / "Spend"))
    import spend_pipeline as sp
    
    def my_read_raw(file_name):
        if "PO_" in file_name:
            df = df_po.copy()
        elif "CON_" in file_name:
            df = df_con.copy()
        elif "IM_" in file_name:
            df = df_im.copy()
        elif "INV_" in file_name:
            df = df_inv.copy()
            
        df.columns = [c.strip() for c in df.columns]
        df = df.astype(str).replace('nan', '')
        df = df.apply(lambda s: s.str.strip())
        return df
        
    sp.read_raw = my_read_raw
    sp.PROJECT_ROOT = str(root_dir)
    sp.ALIAS_FILE = str(root_dir / "Spend" / "vendor_alias_master.csv")
    sp.CLASS_MASTER = str(root_dir / "Spend" / "Product_Class_Master.xlsx")
    
    po_raw = sp.read_raw("UHC_PO_20260930010955.csv")
    con_raw = sp.read_raw("UHC_CON_20260930010958.csv")
    im_raw = sp.read_raw("UHC_IM_20260930010918.csv")
    inv_raw = sp.read_raw("UHC_INV_20260930010902.csv")
    
    po = sp.clean_po(po_raw)
    alias = sp.load_vendor_alias()
    cres, _ = sp.map_contract(po, con_raw, alias)
    ires = sp.map_item_master(po, im_raw)
    cres["contract_status"], cres["off_contract_reason"] = sp.contract_status(cres.contract_match_rule)
    full = pd.concat([po, cres.drop(columns=["contract_review"]), ires], axis=1)
    
    inv = sp.clean_invoice(inv_raw)
    inv_res, _ = sp.map_invoice(full, inv)
    full = full.join(inv_res[sp.INV_COLS])
    
    current_out = sp.map_current_contract(full, con_raw, alias)
    full = full.join(current_out)
    
    class_out, _, _ = sp.classify_products(full, con_raw, sp.load_master())
    full = full.join(class_out)
    
    # Cast datetime/datetimes to string for DuckDB compatibility
    for c in full.columns:
        if pd.api.types.is_datetime64_any_dtype(full[c]):
            full[c] = full[c].astype(str)
            
    return full
