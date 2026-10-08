import os
import re

with open('uc_health/models/intermediate/int_po_enriched.py', 'r') as f:
    content = f.read()

lineage_code = """
    # Dummy calls for dbt lineage
    _ = dbt.source("supplycopia_raw", "UHC_PO_20260930010955")
    _ = dbt.source("supplycopia_raw", "UHC_CON_20260930010958")
    _ = dbt.source("supplycopia_raw", "UHC_IM_20260930010918")
    _ = dbt.source("supplycopia_raw", "UHC_INV_20260930010902")
    
    def read_raw(file_name):
"""

content = content.replace('def read_raw(file_name):', lineage_code)

with open('uc_health/models/intermediate/int_po_enriched.py', 'w') as f:
    f.write(content)
