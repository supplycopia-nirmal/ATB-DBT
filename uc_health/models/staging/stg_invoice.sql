with source as (
    select * from {{ source('supplycopia_raw', 'UHC_INV_20260930010902') }}
),

renamed as (
    select
        * EXCLUDE (
            invoice_number,
            Invoice_line_number,
            Invoice_paid_date,
            invoice_raised_date,
            invoice_payable_date,
            invoice_qty,
            invoice_unit_price,
            invoice_total_value,
            po_number,
            po_line_no,
            facility_name,
            item_id,
            item_description
        ),
        invoice_number,
        Invoice_line_number as invoice_line_number,
        try_cast(Invoice_paid_date as timestamp) as invoice_paid_date,
        try_cast(invoice_raised_date as timestamp) as invoice_raised_date,
        try_cast(invoice_payable_date as timestamp) as invoice_payable_date,
        try_cast(invoice_qty as double) as invoice_qty,
        try_cast(invoice_unit_price as double) as invoice_unit_price,
        try_cast(invoice_total_value as double) as invoice_total_value,
        po_number,
        po_line_no,
        facility_name,
        item_id,
        item_description
    from source
)

select * from renamed
