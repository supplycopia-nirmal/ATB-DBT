with open('/Users/nirmalrayan/Documents/UC_Health/uc_health/models/marts/fct_cost_savings_analysis.sql', 'r') as f:
    sql = f.read()

# Replace po mapped date diff
po_where_old = "where abs(date_diff('day', try_cast(po.po_date as date), try_cast(c.admit_date_time as date))) <= 180"
po_where_new = "where try_cast(po.po_date as date) >= try_cast(c.admit_date_time as date) - interval 180 day and try_cast(po.po_date as date) <= try_cast(c.admit_date_time as date) + interval 180 day"
sql = sql.replace(po_where_old, po_where_new)

# Replace inv mapped date diff
inv_where_old = "where abs(date_diff('day', coalesce(try_cast(inv.invoice_raised_date as date), try_cast(inv.po_date as date)), try_cast(c.admit_date_time as date))) <= 180"
inv_where_new = "where coalesce(try_cast(inv.invoice_raised_date as date), try_cast(inv.po_date as date)) >= try_cast(c.admit_date_time as date) - interval 180 day and coalesce(try_cast(inv.invoice_raised_date as date), try_cast(inv.po_date as date)) <= try_cast(c.admit_date_time as date) + interval 180 day"
sql = sql.replace(inv_where_old, inv_where_new)

with open('/Users/nirmalrayan/Documents/UC_Health/uc_health/models/marts/fct_cost_savings_analysis.sql', 'w') as f:
    f.write(sql)
print("Range join fix applied")
