with raw_source as (
    select * from {{ source('supplycopia_raw', var('invoice_source_file')) }}
),

cleaned as (
    select
        regexp_replace(trim(invoice_number), '^#\s*', '') as invoice_number,
        trim(Invoice_line_number) as invoice_line_number,
        {{ safe_cast('Invoice_paid_date', 'timestamp') }} as invoice_paid_date,
        {{ safe_cast('invoice_raised_date', 'timestamp') }} as invoice_raised_date,
        {{ safe_cast('invoice_payable_date', 'timestamp') }} as invoice_payable_date,
        {{ safe_cast('invoice_qty', 'double') }} as invoice_qty,
        {{ safe_cast('invoice_unit_price', 'double') }} as invoice_unit_price,
        {{ safe_cast('invoice_total_value', 'double') }} as invoice_total_value,
        trim(po_number) as po_number,
        trim(po_line_no) as po_line_no,
        trim(facility_name) as facility_name,
        trim(item_id) as item_id,
        trim(item_description) as item_description
    from raw_source
    where invoice_number is not null
)

select
    *,
    {{ append_metadata('invoice_source_file', ['invoice_number', 'invoice_line_number', 'po_number', 'po_line_no']) }}
from cleaned
