with open('/Users/nirmalrayan/Documents/UC_Health/uc_health/models/marts/fct_cost_savings_analysis.sql', 'r') as f:
    sql = f.read()

# Fix inv.po.po_date -> inv.po_date
sql = sql.replace('inv.po.po_date', 'inv.po_date')
# Fix po.uom -> po_uom if it's supposed to be po_uom (the column is po_uom in purchase_orders)
sql = sql.replace('c.item_uom = po.uom', 'c.item_uom = po.po_uom')

# Now fix the FROM clause
start_idx = sql.find('from consumption c')
if start_idx != -1:
    new_from = """from consumption c
left join mapped_contracts m_con on c.consumption_row_id = m_con.consumption_row_id
left join contracts con on m_con.__con_id = con.__con_id
left join mapped_item_master m_im on c.consumption_row_id = m_im.consumption_row_id
left join item_master im on m_im.__im_id = im.__im_id
left join mapped_po m_po on c.consumption_row_id = m_po.consumption_row_id
left join purchase_orders po on m_po.__po_id = po.__po_id
left join mapped_inv m_inv on c.consumption_row_id = m_inv.consumption_row_id
left join invoices inv on m_inv.__inv_id = inv.__inv_id"""
    sql = sql[:start_idx] + new_from

# Fix EXCLUDE clause in select
# Since mapped tables only provide the join keys, we want to exclude __im_id, __con_id etc. instead of consumption_row_id (which isn't even in those tables)
sql = sql.replace('im.* EXCLUDE(consumption_row_id)', 'im.* EXCLUDE(__im_id)')
sql = sql.replace('po.* EXCLUDE(consumption_row_id)', 'po.* EXCLUDE(__po_id)')
sql = sql.replace('inv.* EXCLUDE(consumption_row_id)', 'inv.* EXCLUDE(__inv_id)')
sql = sql.replace('con.* EXCLUDE(consumption_row_id)', 'con.* EXCLUDE(__con_id)')

with open('/Users/nirmalrayan/Documents/UC_Health/uc_health/models/marts/fct_cost_savings_analysis.sql', 'w') as f:
    f.write(sql)
print("Fix applied")
