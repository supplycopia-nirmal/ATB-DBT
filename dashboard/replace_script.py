with open('../uc_health/models/marts/fct_cost_savings_analysis.sql', 'r') as f:
    sql = f.read()

target = """    c."account_number",
    c."contract_price" as c_contract_price,
    c."total_acquisition_cost",
    c."supply_unit_price",
    c."total_quantity",
    c."total_charges",
    c."manufacturer_name",
    c."manufacturer_catalog_number",
    c."item_number",
    c."item_description",
    c."item_uom",
    c."supplier",
    c."contract_category",
    c."spend_category",
    c."unspsc_code",
    c."contract_flag",
    c."primary_drg_code",
    c."primary_procedure_group",
    
    im.* EXCLUDE(__im_id, item_id),
    po.* EXCLUDE(__po_id, item_id),
    inv.* EXCLUDE(__inv_id, item_id),
    con.* EXCLUDE(__con_id, item_id),"""

with open('replacement.txt', 'r') as f:
    replacement = f.read()

if target in sql:
    sql = sql.replace(target, replacement)
    with open('../uc_health/models/marts/fct_cost_savings_analysis.sql', 'w') as f:
        f.write(sql)
    print("Success")
else:
    print("Target not found. Please check spacing.")
