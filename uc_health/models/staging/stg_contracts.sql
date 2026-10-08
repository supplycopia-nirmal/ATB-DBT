select
    contract_number,
    contract_description,
    try_cast(contract_start as timestamp) as contract_start_date,
    try_cast(contract_end as timestamp) as contract_end_date,
    contract_uom,
    try_cast(contract_qoe as integer) as contract_qoe,
    try_cast(contract_price as double) as contract_price,
    try_cast(contract_ea_price as double) as contract_ea_price,
    item_id,
    item_description,
    manufacturer_part_number,
    manufacture_name,
    vendor_name,
    contract_category,
    try_cast(list_price as double) as list_price,
    pricing_tier,
    tier_requirements
from {{ source('supplycopia_raw', 'UHC_CON_20260930010958') }}
