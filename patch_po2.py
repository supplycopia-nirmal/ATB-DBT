import re

with open('uc_health/models/intermediate/int_po_enriched.py', 'r') as f:
    content = f.read()

# Replace the existing pd.read_csv calls with the correct read_raw logic
content = re.sub(
    r'po_raw = pd.read_csv.*?\n\s*con_raw = pd.read_csv.*?\n\s*im_raw = pd.read_csv.*?\n\s*inv_raw = pd.read_csv.*?\)',
    r'''
    def read_raw(file_name):
        df = pd.read_csv(os.path.join(PROJECT_ROOT, "Data", file_name), sep="|", dtype=str, keep_default_na=False, engine="pyarrow", encoding="utf-8")
        df.columns = [c.strip() for c in df.columns]
        df = df.apply(lambda s: s.str.strip())
        return df

    po_raw = read_raw("UHC_PO_20260930010955.csv")
    con_raw = read_raw("UHC_CON_20260930010958.csv")
    im_raw = read_raw("UHC_IM_20260930010918.csv")
    inv_raw = read_raw("UHC_INV_20260930010902.csv")''',
    content, flags=re.DOTALL
)

with open('uc_health/models/intermediate/int_po_enriched.py', 'w') as f:
    f.write(content)
