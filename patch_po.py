import re

with open('uc_health/models/intermediate/int_po_enriched.py', 'r') as f:
    content = f.read()

# Replace dbt.source calls with pd.read_csv calls
content = re.sub(
    r'po_raw = dbt\.source.*?\n\s*con_raw = dbt\.source.*?\n\s*im_raw = dbt\.source.*?\n\s*inv_raw = dbt\.source.*?df\(\)',
    r'''
    po_raw = pd.read_csv(os.path.join(PROJECT_ROOT, "Data", "UHC_PO_20260930010955.csv"), dtype=str, keep_default_na=False)
    con_raw = pd.read_csv(os.path.join(PROJECT_ROOT, "Data", "UHC_CON_20260930010958.csv"), dtype=str, keep_default_na=False)
    im_raw = pd.read_csv(os.path.join(PROJECT_ROOT, "Data", "UHC_IM_20260930010918.csv"), dtype=str, keep_default_na=False)
    inv_raw = pd.read_csv(os.path.join(PROJECT_ROOT, "Data", "UHC_INV_20260930010902.csv"), dtype=str, keep_default_na=False)''',
    content, flags=re.DOTALL
)

with open('uc_health/models/intermediate/int_po_enriched.py', 'w') as f:
    f.write(content)
