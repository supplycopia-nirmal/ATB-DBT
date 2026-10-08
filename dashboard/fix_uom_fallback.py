import re

with open('/Users/nirmalrayan/Documents/UC_Health/uc_health/models/marts/fct_cost_savings_analysis.sql', 'r') as f:
    sql = f.read()

# Fix mapped_po
po_where_old = "where try_cast(po.po_date as date) >= try_cast(c.admit_date_time as date) - interval 180 day\n      and try_cast(po.po_date as date) <= try_cast(c.admit_date_time as date) + interval 180 day"
po_where_new = "where (try_cast(po.po_date as date) >= try_cast(c.admit_date_time as date) - interval 180 day and try_cast(po.po_date as date) <= try_cast(c.admit_date_time as date) + interval 180 day) OR c.item_uom = po.uom"
sql = sql.replace(po_where_old, po_where_new)

# Fix mapped_inv
inv_where_old = "where coalesce(try_cast(inv.invoice_raised_date as date), try_cast(inv.po_date as date)) >= try_cast(c.admit_date_time as date) - interval 180 day\n      and coalesce(try_cast(inv.invoice_raised_date as date), try_cast(inv.po_date as date)) <= try_cast(c.admit_date_time as date) + interval 180 day"
inv_where_new = "where (coalesce(try_cast(inv.invoice_raised_date as date), try_cast(inv.po_date as date)) >= try_cast(c.admit_date_time as date) - interval 180 day and coalesce(try_cast(inv.invoice_raised_date as date), try_cast(inv.po_date as date)) <= try_cast(c.admit_date_time as date) + interval 180 day) OR c.item_uom = inv.\"Invoice_uom\""
sql = sql.replace(inv_where_old, inv_where_new)

with open('/Users/nirmalrayan/Documents/UC_Health/uc_health/models/marts/fct_cost_savings_analysis.sql', 'w') as f:
    f.write(sql)
print("Fallback logic applied")
