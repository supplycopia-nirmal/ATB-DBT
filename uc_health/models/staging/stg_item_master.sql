with source as (
    select * from {{ source('supplycopia_raw', 'UHC_IM_20260930010918') }}
),

renamed as (
    select
        * EXCLUDE (
            item_id,
            item_description,
            manufacturer_part_number,
            manufacture_name,
            vendor_name,
            vendor_part_number,
            vendor_code,
            contract_number,
            contract_description,
            contract_start,
            contract_end,
            contract_price,
            unspsc,
            unspsc_description,
            is_active
        ),
        item_id,
        item_description,
        manufacturer_part_number as mfr_part_number,
        manufacture_name as mfr_name,
        vendor_name,
        vendor_part_number,
        vendor_code,
        contract_number,
        contract_description,
        try_cast(contract_start as timestamp) as contract_start_date,
        try_cast(contract_end as timestamp) as contract_end_date,
        try_cast(contract_price as double) as contract_price,
        unspsc as unspsc_code,
        unspsc_description,
        is_active
    from source
)

select * from renamed
