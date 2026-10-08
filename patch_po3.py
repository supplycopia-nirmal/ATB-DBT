import re

with open('uc_health/models/intermediate/int_po_enriched.py', 'r') as f:
    content = f.read()

content = content.replace(
    'inv_raw = read_raw("UHC_INV_20260930010902.csv"), dtype=str, keep_default_na=False)',
    'inv_raw = read_raw("UHC_INV_20260930010902.csv")'
)

with open('uc_health/models/intermediate/int_po_enriched.py', 'w') as f:
    f.write(content)
