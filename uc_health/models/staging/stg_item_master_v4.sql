with raw_source as (
    select * from {{ source('supplycopia_raw', var('item_master_source_file')) }}
),

cleaned as (
    select
        trim(item_id) as item_id,
        trim(item_description) as item_description,
        trim(manufacturer_part_number) as mfr_part_number,
        trim(manufacture_name) as mfr_name,
        trim(vendor_name) as vendor_name,
        trim(vendor_part_number) as vendor_part_number,
        trim(vendor_code) as vendor_code,
        trim(contract_number) as contract_number,
        trim(contract_description) as contract_description,
        {{ safe_cast('contract_start', 'timestamp') }} as contract_start_date,
        {{ safe_cast('contract_end', 'timestamp') }} as contract_end_date,
        {{ safe_cast('contract_price', 'double') }} as contract_price,
        {{ safe_cast('contract_qoe', 'integer') }} as contract_qoe,
        trim(unspsc) as unspsc_code,
        trim(unspsc_description) as unspsc_description,
        case 
            when upper(trim(is_active)) in ('ACTIVE', 'Y', 'TRUE', '1') then true
            else false
        end as is_active,
        row_number() over (
            partition by trim(item_id)
            order by 
                case when upper(trim(is_active)) in ('ACTIVE', 'Y', 'TRUE', '1') then 1 else 0 end desc,
                {{ safe_cast('contract_start', 'timestamp') }} desc nulls last
        ) as _dedup_rn
    from raw_source
    where trim(item_id) is not null
),

deduped as (
    select
        item_id,
        item_description,
        mfr_part_number,
        mfr_name,
        vendor_name,
        vendor_part_number,
        vendor_code,
        contract_number,
        contract_description,
        contract_start_date,
        contract_end_date,
        contract_price,
        contract_qoe,
        unspsc_code,
        unspsc_description,
        is_active,
        {{ append_metadata('item_master_source_file', ['item_id', 'mfr_part_number', 'vendor_code', 'unspsc_code']) }}
    from cleaned
    where _dedup_rn = 1
)

select * from deduped
