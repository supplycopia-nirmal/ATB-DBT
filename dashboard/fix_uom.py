import re

with open('/Users/nirmalrayan/Documents/UC_Health/uc_health/models/marts/fct_cost_savings_analysis.sql', 'r') as f:
    content = f.read()

content = content.replace("case when c.item_uom = po.uom then 1 else 2 end,", "case when c.item_uom = po_uom then 1 else 2 end,")
content = content.replace("case when c.item_uom = inv.Invoice_uom then 1 else 2 end,", "case when c.item_uom = inv_Invoice_uom then 1 else 2 end,")

with open('/Users/nirmalrayan/Documents/UC_Health/uc_health/models/marts/fct_cost_savings_analysis.sql', 'w') as f:
    f.write(content)
