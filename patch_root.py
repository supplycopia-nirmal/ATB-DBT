import re

with open('uc_health/models/intermediate/int_po_enriched.py', 'r') as f:
    content = f.read()

content = content.replace(
    "PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))",
    "PROJECT_ROOT = os.path.dirname(os.getcwd())"
)

with open('uc_health/models/intermediate/int_po_enriched.py', 'w') as f:
    f.write(content)
