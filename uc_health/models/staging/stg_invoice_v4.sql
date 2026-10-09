with raw_source as (
    select * from {{ source('supplycopia_raw', var('invoice_source_file')) }}
),

cleaned as (
    select
        regexp_replace({{ clean_string('invoice_number') }}, '^#\s*', '') as invoice_number,
        {{ clean_string('Invoice_line_number') }} as invoice_line_number,
        {{ safe_cast('Invoice_paid_date', 'timestamp') }} as invoice_paid_date,
        {{ safe_cast('invoice_raised_date', 'timestamp') }} as invoice_raised_date,
        {{ clean_string('facility_entity_code') }} as facility_entity_code,
        {{ clean_string('po_number') }} as po_number,
        {{ clean_string('po_line_no') }} as po_line_no,
        {{ safe_cast('invoice_qty', 'double') }} as invoice_qty,
        {{ safe_cast('invoice_unit_price', 'double') }} as invoice_unit_price,
        {{ safe_cast('invoice_total_value', 'double') }} as invoice_total_value,
        {{ clean_upper('invoice_uom') }} as invoice_uom,
        {{ clean_string('item_id') }} as item_id,
        {{ clean_string('vendor_code') }} as vendor_code
    from raw_source
    where trim(invoice_number) is not null
),

final as (
    select
        *,
        {{ append_metadata('invoice_source_file', ['invoice_number', 'invoice_line_number', 'po_number', 'item_id']) }}
    from cleaned
)

select * from final
