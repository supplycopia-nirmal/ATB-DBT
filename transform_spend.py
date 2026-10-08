import os
import re

with open("Spend/spend_pipeline.py", "r") as f:
    code = f.read()

# Replace file cache references
code = re.sub(r'def stage_contract_im.*?return full', '', code, flags=re.DOTALL)
code = re.sub(r'def stage_invoice.*?return full\.join\(res\[INV_COLS\]\)', '', code, flags=re.DOTALL)
code = re.sub(r'def stage_current_contract.*?return full\.join\(out\)', '', code, flags=re.DOTALL)
code = re.sub(r'def stage_classify.*?return full\.join\(out\)', '', code, flags=re.DOTALL)
code = re.sub(r'def write_outputs.*', '', code, flags=re.DOTALL)
code = re.sub(r'def main\(.*', '', code, flags=re.DOTALL)

# Delete unwanted utility functions that break the dbt logic
for func in ['class_sig', 'manifest_load', 'manifest_save', 'fingerprint', 'code_hash', 'latest_file', 'read_raw']:
    code = re.sub(r'def ' + func + r'\(.*?\n(?=def )', '', code, flags=re.DOTALL)

# Wrap it
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
    
    # Read references and undo dbt renaming to match the python script's original raw column names
    po_raw = dbt.ref("stg_purchase_orders").df().rename(columns={
        "mfr_name": "manufacture_name",
        "mfr_part_number": "manufacturer_part_number"
    })
    
    con_raw = dbt.ref("stg_contracts").df().rename(columns={
        "contract_start_date": "contract_start",
        "contract_end_date": "contract_end",
        "mfr_part_number": "manufacturer_part_number"
    })
    
    im_raw = dbt.ref("stg_item_master").df().rename(columns={
        "mfr_part_number": "manufacturer_part_number",
        "mfr_name": "manufacture_name",
        "contract_start_date": "contract_start",
        "contract_end_date": "contract_end",
        "unspsc_code": "unspsc"
    })
    
    inv_raw = dbt.ref("stg_invoice").df().rename(columns={
        "invoice_uom_conv_factor": "Invoice_uom_conv_factor"
    })
    
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
    
    # Fix types for DuckDB output
    full["po_contract_effective_date"] = full["po_contract_effective_date"].astype(str)
    full["current_contract_as_of"] = full["current_contract_as_of"].astype(str)
    for col in full.select_dtypes(include=['datetime64[ns]']):
        full[col] = full[col].astype(str)

    return full
"""

# Let's extract only the valid functions from spend_pipeline.py by reading it block by block
import ast
valid_code = ""
with open("Spend/spend_pipeline.py", "r") as f:
    source = f.read()

class FuncExtractor(ast.NodeVisitor):
    def __init__(self, source):
        self.source = source.splitlines()
        self.code_blocks = []
    
    def visit_FunctionDef(self, node):
        if not node.name.startswith("stage_") and node.name not in ['main', 'write_outputs', 'manifest_load', 'manifest_save', 'fingerprint', 'code_hash', 'latest_file', 'read_raw']:
            start = node.lineno - 1
            end = node.end_lineno
            self.code_blocks.append("\n".join(self.source[start:end]) + "\n")
        self.generic_visit(node)
        
    def visit_Assign(self, node):
        if node.col_offset == 0:
            start = node.lineno - 1
            end = node.end_lineno
            self.code_blocks.append("\n".join(self.source[start:end]) + "\n")
        self.generic_visit(node)

# I will just manually strip the file!
