import re

with open('/Users/nirmalrayan/Documents/UC_Health/uc_health/models/marts/fct_cost_savings_analysis.sql', 'r') as f:
    content = f.read()

# 1. Remove strict WHERE from item_master
im_pattern = r"""(inner join item_master im on c\.item_number = im\.item_id\n)\s+where try_cast\(c\.admit_date_time as date\) >= try_cast\(im\.contract_start_date as date\) \n\s+and try_cast\(c\.admit_date_time as date\) <= coalesce\(try_cast\(im\.contract_end_date as date\), '2099-12-31'\)\n\s+(QUALIFY)"""
content = re.sub(im_pattern, r"\1    \2", content, flags=re.MULTILINE)

# 2. Add UOM logic to PO QUALIFY
po_pattern = r"""(QUALIFY row_number\(\) over \(\n\s+partition by c\.consumption_row_id \n\s+order by )(abs\(date_diff)"""
po_replacement = r"\1case when c.item_uom = po.uom then 1 else 2 end,\n            \2"
content = re.sub(po_pattern, po_replacement, content, flags=re.MULTILINE)

# 3. Add UOM logic to Inv QUALIFY
inv_pattern = r"""(QUALIFY row_number\(\) over \(\n\s+partition by c\.consumption_row_id \n\s+order by )(abs\(date_diff\('day', coalesce\(try_cast\(inv\.invoice_raised)"""
inv_replacement = r"\1case when c.item_uom = inv.Invoice_uom then 1 else 2 end,\n            \2"
content = re.sub(inv_pattern, inv_replacement, content, flags=re.MULTILINE)


with open('/Users/nirmalrayan/Documents/UC_Health/uc_health/models/marts/fct_cost_savings_analysis.sql', 'w') as f:
    f.write(content)

print("Rewrite successful.")
