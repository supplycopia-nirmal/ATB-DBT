with raw_source as (
    select * from {{ source('supplycopia_raw', var('contracts_source_file')) }}
),

cleaned as (
    select
        trim(contract_number) as contract_number,
        trim(contract_description) as contract_description,
        {{ safe_cast('contract_start', 'timestamp') }} as contract_start_date,
        {{ safe_cast('contract_end', 'timestamp') }} as contract_end_date,
        upper(trim(contract_uom)) as contract_uom,
        {{ safe_cast('contract_qoe', 'integer') }} as contract_qoe,
        {{ safe_cast('contract_price', 'double') }} as contract_price,
        {{ safe_cast('contract_ea_price', 'double') }} as contract_ea_price,
        trim(item_id) as item_id,
        trim(item_description) as item_description,
        trim(manufacturer_part_number) as manufacturer_part_number,
        trim(manufacture_name) as manufacture_name,
        trim(vendor_name) as vendor_name,
        trim(contract_category) as contract_category,
        {{ safe_cast('list_price', 'double') }} as list_price,
        trim(pricing_tier) as pricing_tier,
        trim(tier_requirements) as tier_requirements
    from raw_source
    where trim(contract_number) is not null
      and trim(item_id) is not null
      and {{ safe_cast('contract_price', 'double') }} is not null
),

filtered as (
    select
        *,
        {{ append_metadata('contracts_source_file', ['contract_number', 'item_id', 'contract_price', 'contract_start_date']) }}
    from cleaned
    where contract_end_date is null or contract_end_date >= contract_start_date
)

select * from filtered
