with raw_source as (
    select * from {{ source('supplycopia_raw', var('po_source_file')) }}
),

cleaned as (
    select
        {{ clean_string('po_number') }} as po_number,
        {{ clean_string('po_line_no') }} as po_line_no,
        {{ safe_cast('po_date', 'timestamp') }} as po_date,
        {{ safe_cast('po_last_update_date', 'timestamp') }} as po_last_update_date,
        {{ clean_string('facility_entity_code') }} as facility_entity_code,
        {{ clean_string('facility_name') }} as facility_name,
        {{ clean_string('contract_no') }} as contract_number,
        {{ clean_upper('uom') }} as uom,
        {{ safe_cast('uom_conv_factor', 'double') }} as uom_conv_factor,
        {{ safe_cast('quantity', 'double') }} as quantity,
        {{ safe_cast('unit_price', 'double') }} as unit_price,
        {{ safe_cast('total_value', 'double') }} as total_value,
        {{ clean_string('item_id') }} as item_id,
        {{ clean_string('item_description') }} as item_description,
        {{ clean_string('manufacture_ERP_id') }} as mfr_erp_id,
        {{ clean_string('manufacture_name') }} as mfr_name,
        {{ clean_string('manufacturer_part_number') }} as mfr_part_number,
        {{ clean_string('vendor_code') }} as vendor_code,
        {{ clean_upper('vendor_name') }} as vendor_name,
        {{ clean_string('vendor_part_number') }} as vendor_part_number,
        row_number() over (
            partition by trim(po_number), trim(po_line_no)
            order by {{ safe_cast('po_last_update_date', 'timestamp') }} desc nulls last
        ) as _dedup_rn
    from raw_source
    where trim(po_number) is not null 
      and trim(po_line_no) is not null
),

deduped as (
    select
        po_number,
        po_line_no,
        po_date,
        po_last_update_date,
        facility_entity_code,
        facility_name,
        contract_number,
        uom,
        coalesce(uom_conv_factor, 1.0) as uom_conv_factor,
        quantity,
        unit_price,
        total_value,
        item_id,
        item_description,
        mfr_erp_id,
        mfr_name,
        mfr_part_number,
        vendor_code,
        vendor_name,
        vendor_part_number,
        {{ append_metadata('po_source_file', ['po_number', 'po_line_no', 'vendor_name', 'item_id']) }}
    from cleaned
    where _dedup_rn = 1
)

select * from deduped
