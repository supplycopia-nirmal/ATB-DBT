import re

with open('/Users/nirmalrayan/Documents/UC_Health/uc_health/models/marts/fct_cost_savings_analysis.sql', 'r') as f:
    sql = f.read()

# 1. Add row_number to the CTEs at the top
sql = sql.replace("invoices as ( select * from {{ ref('stg_invoice') }} ),",
                  "invoices as ( select *, row_number() over() as __inv_id from {{ ref('stg_invoice') }} ),")
sql = sql.replace("purchase_orders as ( select * from {{ ref('stg_purchase_orders') }} ),",
                  "purchase_orders as ( select *, row_number() over() as __po_id from {{ ref('stg_purchase_orders') }} ),")
sql = sql.replace("item_master as ( select * from {{ ref('int_item_master_profiled') }} where contract_price is not null ),",
                  "item_master as ( select *, row_number() over() as __im_id from {{ ref('int_item_master_profiled') }} where contract_price is not null ),")
sql = sql.replace("contracts as ( select * from {{ ref('stg_contracts') }} where contract_price is not null ),",
                  "contracts as ( select *, row_number() over() as __con_id from {{ ref('stg_contracts') }} where contract_price is not null ),")


# 2. Rewrite mapped_contracts
con_pattern = r"mapped_contracts as \([\s\S]*?\) = 1\n\),"
con_replacement = """mapped_contracts as (
    select c.consumption_row_id, con.__con_id
    from consumption c
    inner join contracts con on c.item_number = con.item_id
    where try_cast(c.admit_date_time as date) >= try_cast(con.contract_start_date as date)
      and try_cast(c.admit_date_time as date) <= coalesce(try_cast(con.contract_end_date as date), '2099-12-31')
    QUALIFY row_number() over (
        partition by c.consumption_row_id
        order by
            case when c.item_uom = con.contract_uom then 1 else 2 end,
            try_cast(con.contract_start_date as date) desc
    ) = 1
),"""
sql = re.sub(con_pattern, con_replacement, sql)


# 3. Rewrite mapped_item_master
im_pattern = r"mapped_item_master as \([\s\S]*?\) = 1\n\),"
im_replacement = """mapped_item_master as (
    select c.consumption_row_id, im.__im_id
    from consumption c
    inner join item_master im on c.item_number = im.item_id
    QUALIFY row_number() over (
        partition by c.consumption_row_id
        order by
            case when try_cast(c.admit_date_time as date) >= try_cast(im.contract_start_date as date) and try_cast(c.admit_date_time as date) <= coalesce(try_cast(im.contract_end_date as date), '2099-12-31') then 1 else 2 end,
            case when c.item_uom = im.contract_uom then 1 else 2 end,
            try_cast(im.contract_start_date as date) desc
    ) = 1
),"""
sql = re.sub(im_pattern, im_replacement, sql)


# 4. Rewrite mapped_po
po_pattern = r"mapped_po as \([\s\S]*?\) = 1\n\),"
po_replacement = """mapped_po as (
    select c.consumption_row_id, po.__po_id
    from consumption c
    inner join purchase_orders po on c.item_number = po.item_id
    where abs(date_diff('day', try_cast(po.po_date as date), try_cast(c.admit_date_time as date))) <= 180
    QUALIFY row_number() over (
        partition by c.consumption_row_id
        order by case when c.item_uom = po_uom then 1 else 2 end,
            abs(date_diff('day', try_cast(po.po_date as date), try_cast(c.admit_date_time as date))) asc
    ) = 1
),"""
sql = re.sub(po_pattern, po_replacement, sql)


# 5. Rewrite mapped_inv
inv_pattern = r"mapped_inv as \([\s\S]*?\) = 1\n\)"
inv_replacement = """mapped_inv as (
    select c.consumption_row_id, inv.__inv_id
    from consumption c
    inner join invoices inv on c.item_number = inv.item_id
    where abs(date_diff('day', coalesce(try_cast(inv.invoice_raised_date as date), try_cast(inv.po_date as date)), try_cast(c.admit_date_time as date))) <= 180
    QUALIFY row_number() over (
        partition by c.consumption_row_id
        order by case when c.item_uom = inv_Invoice_uom then 1 else 2 end,
            abs(date_diff('day', coalesce(try_cast(inv.invoice_raised_date as date), try_cast(inv.po_date as date)), try_cast(c.admit_date_time as date))) asc
    ) = 1
)"""
sql = re.sub(inv_pattern, inv_replacement, sql)


# 6. Fix the final from/joins
from_pattern = r"from consumption c\s+left join mapped_contracts con on con\.consumption_row_id = c\.consumption_row_id\s+left join mapped_item_master im on im\.consumption_row_id = c\.consumption_row_id\s+left join mapped_po po on po\.consumption_row_id = c\.consumption_row_id\s+left join mapped_inv inv on inv\.consumption_row_id = c\.consumption_row_id"
from_replacement = """from consumption c
left join mapped_contracts m_con on m_con.consumption_row_id = c.consumption_row_id
left join contracts con on con.__con_id = m_con.__con_id
left join mapped_item_master m_im on m_im.consumption_row_id = c.consumption_row_id
left join item_master im on im.__im_id = m_im.__im_id
left join mapped_po m_po on m_po.consumption_row_id = c.consumption_row_id
left join purchase_orders po on po.__po_id = m_po.__po_id
left join mapped_inv m_inv on m_inv.consumption_row_id = c.consumption_row_id
left join invoices inv on inv.__inv_id = m_inv.__inv_id"""

sql = re.sub(from_pattern, from_replacement, sql)

# Ensure aliases in the final select match what they expect. The original query aliased columns like `im."packaging_string" as im_packaging_string`. Since we are directly joining the tables aliased as `im`, `con`, `po`, `inv`, this works perfectly without changing the SELECT list!

with open('/Users/nirmalrayan/Documents/UC_Health/uc_health/models/marts/fct_cost_savings_analysis.sql', 'w') as f:
    f.write(sql)
print("Rewrite successful!")
