import duckdb

con = duckdb.connect('uc_health.duckdb', read_only=True)

c_cols = [c[0] for c in con.execute('DESCRIBE main.int_consumption_profiled').fetchall()]
im_cols = [c[0] for c in con.execute('DESCRIBE main.int_item_master_profiled').fetchall()]
po_cols = [c[0] for c in con.execute('DESCRIBE main.stg_purchase_orders').fetchall()]
inv_cols = [c[0] for c in con.execute('DESCRIBE main.stg_invoice').fetchall()]

print("with consumption as ( select * from {{ ref('int_consumption_profiled') }} ),")
print("invoices as ( select * from {{ ref('stg_invoice') }} ),")
print("purchase_orders as ( select * from {{ ref('stg_purchase_orders') }} ),")
print("item_master as ( select * from {{ ref('int_item_master_profiled') }} )")
print("select")

# Consumption columns
for c in c_cols:
    print(f"    c.\"{c}\",")

# Item Master columns
for c in im_cols:
    if c not in ('item_id', 'item_description'):
        print(f"    im.\"{c}\" as im_{c},")

# PO columns
for c in po_cols:
    if c not in ('item_id', 'item_description', 'facility_name'):
        print(f"    po.\"{c}\" as po_{c},")

# Invoice columns
for c in inv_cols:
    if c not in ('item_id', 'item_description', 'po_number', 'facility_name'):
        print(f"    inv.\"{c}\" as inv_{c},")

print("""
    -- Computed Cost Savings Metrics
    case 
        when im.contract_price > 0 then (c.supply_unit_price - im.contract_price) * c.total_quantity
        else 0
    end as price_variance,
    
    case
        when c.contract_flag = 'OFF CONTRACT' then true
        else false
    end as is_off_contract

from consumption c
left join item_master im 
    on c.item_number = im.item_id
left join purchase_orders po 
    on c.item_number = po.item_id 
    and c.facility = po.facility_name
left join invoices inv 
    on po.po_number = inv.po_number 
    and c.item_number = inv.item_id
""")

