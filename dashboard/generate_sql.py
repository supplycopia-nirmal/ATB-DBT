import json

im_cols = ['packaging_string', 'manufacture_id', 'brand_name', 'latex', 'ndc', 'contract_uom', 'contract_qoe', 'hcpcs', 'item_id', 'item_description', 'mfr_part_number', 'mfr_name', 'vendor_name', 'vendor_part_number', 'vendor_code', 'contract_number', 'contract_description', 'contract_start_date', 'contract_end_date', 'contract_price', 'unspsc_code', 'unspsc_description', 'is_active', 'im_unit_contract_price', 'is_missing_vendor_code', 'is_missing_mfr_name', 'custom_category', 'data_quality_score']
po_cols = ['facility_entity_code', 'uom', 'uom_conv_factor', 'manufacture_ERP_id', 'vendor_code', 'vendor_part_number', 'po_number', 'po_line_no', 'po_date', 'facility_name', 'contract_number', 'quantity', 'unit_price', 'total_value', 'item_id', 'item_description', 'mfr_name', 'mfr_part_number', 'vendor_name']
inv_cols = ['Invoice_uom', 'Invoice_uom_conv_factor', 'po_date', 'facility_entity_code', 'po_uom', 'po_uom_conv_factor', 'po_qty', 'po_unit_price', 'po_total_value', 'invoice_number', 'invoice_line_number', 'invoice_paid_date', 'invoice_raised_date', 'invoice_payable_date', 'invoice_qty', 'invoice_unit_price', 'invoice_total_value', 'po_number', 'po_line_no', 'facility_name', 'item_id', 'item_description']
con_cols = ['contract_number', 'contract_description', 'contract_start_date', 'contract_end_date', 'contract_uom', 'contract_qoe', 'contract_price', 'contract_ea_price', 'item_id', 'item_description', 'manufacturer_part_number', 'manufacture_name', 'vendor_name', 'contract_category', 'list_price', 'pricing_tier', 'tier_requirements']

def filter_and_format(prefix, cols, exclude):
    res = []
    for c in cols:
        if c not in exclude:
            res.append(f'    {prefix}."{c}" as {prefix}_{c},')
    return "\n".join(res)

im_exclude = ['__im_id', 'item_id', 'contract_uom', 'contract_price', 'im_unit_contract_price']
po_exclude = ['__po_id', 'item_id', 'uom', 'unit_price']
inv_exclude = ['__inv_id', 'item_id', 'Invoice_uom', 'invoice_unit_price', 'po_uom', 'po_unit_price']
con_exclude = ['__con_id', 'item_id', 'contract_uom', 'contract_price', 'contract_ea_price']

output = []
output.append("-- ITEM MASTER")
output.append(filter_and_format('im', im_cols, im_exclude))
output.append("-- PO")
output.append(filter_and_format('po', po_cols, po_exclude))
output.append("-- INVOICES")
output.append(filter_and_format('inv', inv_cols, inv_exclude))
output.append("-- CONTRACTS")
output.append(filter_and_format('con', con_cols, con_exclude))

output.append("""
    -- ---------------------------------------------------------
    -- UOM AND PRICE COLUMNS FOR SIDE-BY-SIDE COMPARISON
    -- ---------------------------------------------------------
    c.item_uom as consumption_uom,
    c.supply_unit_price as consumption_unit_price,
    c.contract_price as consumption_contract_price,

    po.uom as po_uom,
    po.unit_price as po_unit_price,

    inv.Invoice_uom as inv_invoice_uom,
    inv.invoice_unit_price as inv_invoice_unit_price,
    inv.po_uom as inv_po_uom,
    inv.po_unit_price as inv_po_unit_price,

    im.contract_uom as im_contract_uom,
    im.contract_price as im_contract_price,
    im.im_unit_contract_price as im_im_unit_contract_price,

    con.contract_uom as con_contract_uom,
    con.contract_price as con_contract_price,
    con.contract_ea_price as con_contract_ea_price,""")

with open('columns_to_replace.txt', 'w') as f:
    f.write("\n".join(output))

