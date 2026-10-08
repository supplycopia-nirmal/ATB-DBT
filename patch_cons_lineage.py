import os
import re

with open('uc_health/models/intermediate/int_consumption_enriched_v3.py', 'r') as f:
    content = f.read()

lineage_code = """
    # Dummy calls for dbt lineage
    _ = dbt.source("supplycopia_raw", "UHC_Consumption")
    _ = dbt.source("supplycopia_raw", "UHC_CON_20260930010958")
    _ = dbt.source("supplycopia_raw", "UHC_IM_20260930010918")
    
    def my_iter_csv_chunks(path, header, chunk_rows):
"""

content = content.replace('def my_iter_csv_chunks(path, header, chunk_rows):', lineage_code)

with open('uc_health/models/intermediate/int_consumption_enriched_v3.py', 'w') as f:
    f.write(content)
