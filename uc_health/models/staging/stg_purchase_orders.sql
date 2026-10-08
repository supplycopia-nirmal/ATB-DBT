-- Depends on: {{ source('supplycopia_raw', 'UHC_PO_20260930010955') }}
with source as (
    select * from read_csv('../Data/UHC_PO_20260930010955.csv', 
        delim='|', header=True, 
        columns={'po_number': 'VARCHAR', 'po_line_no': 'VARCHAR', 'po_date': 'VARCHAR', 'po_last_update_date': 'VARCHAR', 'facility_entity_code': 'VARCHAR', 'facility_name': 'VARCHAR', 'contract_no': 'VARCHAR', 'uom': 'VARCHAR', 'uom_conv_factor': 'VARCHAR', 'quantity': 'VARCHAR', 'unit_price': 'VARCHAR', 'total_value': 'VARCHAR', 'item_id': 'VARCHAR', 'item_description': 'VARCHAR', 'manufacture_ERP_id': 'VARCHAR', 'manufacture_name': 'VARCHAR', 'manufacturer_part_number': 'VARCHAR', 'vendor_code': 'VARCHAR', 'vendor_name': 'VARCHAR', 'vendor_part_number': 'VARCHAR'},
        ignore_errors=True, null_padding=True, auto_detect=False, strict_mode=False, parallel=False)
),

renamed as (
    select
        * EXCLUDE (
            po_number,
            po_line_no,
            po_date,
            facility_name,
            contract_no,
            quantity,
            unit_price,
            total_value,
            item_id,
            item_description,
            manufacture_name,
            manufacturer_part_number,
            vendor_name
        ),
        po_number,
        po_line_no,
        try_cast(po_date as timestamp) as po_date,
        facility_name,
        contract_no as contract_number,
        try_cast(quantity as double) as quantity,
        try_cast(unit_price as double) as unit_price,
        try_cast(total_value as double) as total_value,
        item_id,
        item_description,
        manufacture_name as mfr_name,
        manufacturer_part_number as mfr_part_number,
        vendor_name
    from source
)

select * from renamed
