import re

with open('/Users/nirmalrayan/Documents/UC_Health/uc_health/models/marts/fct_cost_savings_analysis.sql', 'r') as f:
    sql = f.read()

po_old = "purchase_orders as ( select *, row_number() over() as __po_id from {{ ref('stg_purchase_orders') }} ),"
po_new = """purchase_orders as (
    select *, row_number() over() as __po_id 
    from (
        select *, row_number() over(partition by item_id, uom, date_trunc('month', try_cast(po_date as date)) order by try_cast(po_date as date) desc) as rn
        from {{ ref('stg_purchase_orders') }}
        where item_id is not null
    ) where rn = 1
),"""
sql = sql.replace(po_old, po_new)


inv_old = "invoices as ( select *, row_number() over() as __inv_id from {{ ref('stg_invoice') }} ),"
inv_new = """invoices as (
    select *, row_number() over() as __inv_id
    from (
        select *, row_number() over(partition by item_id, "Invoice_uom", date_trunc('month', coalesce(try_cast(invoice_raised_date as date), try_cast(po_date as date))) order by coalesce(try_cast(invoice_raised_date as date), try_cast(po_date as date)) desc) as rn
        from {{ ref('stg_invoice') }}
        where item_id is not null
    ) where rn = 1
),"""
sql = sql.replace(inv_old, inv_new)

with open('/Users/nirmalrayan/Documents/UC_Health/uc_health/models/marts/fct_cost_savings_analysis.sql', 'w') as f:
    f.write(sql)
print("Deduplication applied")
