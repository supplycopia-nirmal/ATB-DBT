import re

with open('/Users/nirmalrayan/Documents/UC_Health/uc_health/models/marts/fct_cost_savings_analysis.sql', 'r') as f:
    sql = f.read()

# Replace prefix usage in expressions
sql = sql.replace('con.con_', 'con.')
sql = sql.replace('im.im_', 'im.')
sql = sql.replace('po.po_', 'po.')
sql = sql.replace('inv.inv_', 'inv.')
sql = sql.replace('inv.Invoice_uom', 'inv."Invoice_uom"')
sql = sql.replace('inv.Invoice_uom_conv_factor', 'inv."Invoice_uom_conv_factor"')

# In the select list, they are just selected directly, but we want them aliased:
# Wait, if they were selected as `con.con_contract_number as con_contract_number`, now they should be `con.contract_number as con_contract_number`.
# Since I replaced `con.con_` with `con.`, it will become `con.contract_number as con_contract_number`! Which is PERFECT!

with open('/Users/nirmalrayan/Documents/UC_Health/uc_health/models/marts/fct_cost_savings_analysis.sql', 'w') as f:
    f.write(sql)
print("Select fix applied!")
