import ast
import astunparse

with open("Spend/spend_pipeline.py", "r") as f:
    tree = ast.parse(f.read())

to_remove = {'main', 'stage_contract_im', 'stage_invoice', 'stage_current_contract', 'stage_classify', 'write_outputs', 'manifest_load', 'manifest_save', 'fingerprint', 'code_hash', 'latest_file', 'read_raw'}

new_body = []
for node in tree.body:
    if isinstance(node, ast.FunctionDef) and node.name in to_remove:
        continue
    new_body.append(node)

tree.body = new_body

dbt_model = """

def model(dbt, session):
    dbt.config(materialized="table", packages=["pandas", "numpy", "openpyxl"])
    
    import os
    import pandas as pd
    import numpy as np
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    global ALIAS_FILE, CLASS_MASTER
    ALIAS_FILE = os.path.join(PROJECT_ROOT, "Spend", "vendor_alias_master.csv")
    CLASS_MASTER = os.path.join(PROJECT_ROOT, "Spend", "Product_Class_Master.xlsx")
    
    # Read the RAW data exactly like read_raw did, bypassing stg models that dropped/renamed columns
    po_raw = dbt.source("supplycopia_raw", "UHC_PO_20260930010955").df()
    con_raw = dbt.source("supplycopia_raw", "UHC_CON_20260930010958").df()
    im_raw = dbt.source("supplycopia_raw", "UHC_IM_20260930010918").df()
    inv_raw = dbt.source("supplycopia_raw", "UHC_INV_20260930010902").df()
    
    # 1. Clean
    po = clean_po(po_raw)
    
    # 2. Stage 1: Contract & IM mapping
    alias = load_vendor_alias()
    cres, _ = map_contract(po, con_raw, alias)
    ires = map_item_master(po, im_raw)
    cres["contract_status"], cres["off_contract_reason"] = contract_status(cres.contract_match_rule)
    full = pd.concat([po, cres.drop(columns=["contract_review"]), ires], axis=1)
    
    # 3. Stage 2: Invoice
    inv = clean_invoice(inv_raw)
    inv_res, _ = map_invoice(full, inv)
    full = full.join(inv_res[INV_COLS])
    
    # 4. Stage 1c: Current contract
    current_out = map_current_contract(full, con_raw, alias)
    full = full.join(current_out)
    
    # 5. Stage 3: Classify products
    class_out, _, _ = classify_products(full, con_raw, load_master())
    full = full.join(class_out)
    
    for col in full.select_dtypes(include=['datetime64[ns]']):
        full[col] = full[col].astype(str)

    # Some remaining empty string as nat
    full.replace("NaT", None, inplace=True)
    return full
"""

with open("uc_health/models/intermediate/int_po_enriched.py", "w") as f:
    f.write(astunparse.unparse(tree) + "\n" + dbt_model)
