with raw_source as (
    select * from {{ source('supplycopia_raw', var('po_source_file')) }}
),

cleaned as (
    select
        trim(po_number) as po_number,
        trim(po_line_no) as po_line_no,
        {{ safe_cast('po_date', 'timestamp') }} as po_date,
        {{ safe_cast('po_last_update_date', 'timestamp') }} as po_last_update_date,
        trim(facility_entity_code) as facility_entity_code,
        trim(facility_name) as facility_name,
        trim(contract_no) as contract_number,
        upper(trim(uom)) as uom,
        {{ safe_cast('uom_conv_factor', 'double') }} as uom_conv_factor,
        {{ safe_cast('quantity', 'double') }} as quantity,
        {{ safe_cast('unit_price', 'double') }} as unit_price,
        {{ safe_cast('total_value', 'double') }} as total_value,
        trim(item_id) as item_id,
        trim(item_description) as item_description,
        trim(manufacture_ERP_id) as mfr_erp_id,
        trim(manufacture_name) as mfr_name,
        trim(manufacturer_part_number) as mfr_part_number,
        trim(vendor_code) as vendor_code,
        trim(vendor_name) as vendor_name,
        trim(vendor_part_number) as vendor_part_number,
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
