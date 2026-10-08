{{ config(materialized='table') }}

with cons as (
    select * from {{ ref('int_consumption_normalized_v4') }}
),

im_match as (
    select * from {{ ref('int_item_matching_v4') }}
),

con as (
    select * from {{ ref('stg_contracts_v4') }}
),

vendor_alias as (
    select * from {{ ref('vendor_alias_table') }}
),

cons_with_keys as (
    select
        c.*,
        im_m.matched_item_id,
        coalesce(va.standard_vendor, c.supplier) as standard_supplier,
        coalesce(im_m.matched_item_id, c.item_number) as effective_item_id
    from cons c
    left join im_match im_m on c.row_id = im_m.row_id
    left join vendor_alias va on upper(trim(c.supplier)) = upper(trim(va.alias_name))
),

-- Tier 1: effective_item_id + standard_supplier + manufacturer_catalog_number + item_uom
tier1 as (
    select
        ck.row_id,
        ck.log_id,
        co.contract_number,
        co.contract_price,
        co.contract_ea_price,
        co.contract_uom,
        co.contract_category as item_contract_category,
        co.contract_start_date,
        co.contract_end_date,
        1 as contract_match_tier,
        'item_vendor_part_uom' as contract_match_rule
    from cons_with_keys ck
    inner join con co 
        on ck.effective_item_id = co.item_id
       and upper(trim(ck.standard_supplier)) = upper(trim(co.vendor_name))
       and upper(trim(coalesce(ck.manufacturer_catalog_number, ''))) = upper(trim(coalesce(co.manufacturer_part_number, '')))
       and upper(trim(ck.item_uom)) = upper(trim(co.contract_uom))
    where (ck.consumption_date between cast(co.contract_start_date as date) and coalesce(cast(co.contract_end_date as date), cast('2099-12-31' as date)))
),

-- Tier 2: effective_item_id + standard_supplier + item_uom
tier2 as (
    select
        ck.row_id,
        ck.log_id,
        co.contract_number,
        co.contract_price,
        co.contract_ea_price,
        co.contract_uom,
        co.contract_category as item_contract_category,
        co.contract_start_date,
        co.contract_end_date,
        2 as contract_match_tier,
        'item_vendor_uom' as contract_match_rule
    from cons_with_keys ck
    inner join con co 
        on ck.effective_item_id = co.item_id
       and upper(trim(ck.standard_supplier)) = upper(trim(co.vendor_name))
       and upper(trim(ck.item_uom)) = upper(trim(co.contract_uom))
    where ck.row_id not in (select row_id from tier1)
      and (ck.consumption_date between cast(co.contract_start_date as date) and coalesce(cast(co.contract_end_date as date), cast('2099-12-31' as date)))
),

-- Tier 3: effective_item_id + standard_supplier (any UOM)
tier3 as (
    select
        ck.row_id,
        ck.log_id,
        co.contract_number,
        co.contract_price,
        co.contract_ea_price,
        co.contract_uom,
        co.contract_category as item_contract_category,
        co.contract_start_date,
        co.contract_end_date,
        3 as contract_match_tier,
        'item_vendor_any_uom' as contract_match_rule
    from cons_with_keys ck
    inner join con co 
        on ck.effective_item_id = co.item_id
       and upper(trim(ck.standard_supplier)) = upper(trim(co.vendor_name))
    where ck.row_id not in (select row_id from tier1)
      and ck.row_id not in (select row_id from tier2)
      and (ck.consumption_date between cast(co.contract_start_date as date) and coalesce(cast(co.contract_end_date as date), cast('2099-12-31' as date)))
),

-- Tier 4: manufacturer_catalog_number + standard_supplier
tier4 as (
    select
        ck.row_id,
        ck.log_id,
        co.contract_number,
        co.contract_price,
        co.contract_ea_price,
        co.contract_uom,
        co.contract_category as item_contract_category,
        co.contract_start_date,
        co.contract_end_date,
        4 as contract_match_tier,
        'part_vendor_match' as contract_match_rule
    from cons_with_keys ck
    inner join con co 
        on upper(trim(ck.standard_supplier)) = upper(trim(co.vendor_name))
       and upper(trim(ck.manufacturer_catalog_number)) = upper(trim(co.manufacturer_part_number))
    where ck.row_id not in (select row_id from tier1)
      and ck.row_id not in (select row_id from tier2)
      and ck.row_id not in (select row_id from tier3)
      and ck.manufacturer_catalog_number is not null
      and trim(ck.manufacturer_catalog_number) not in ('', 'N/A', 'NA', 'NONE', 'NULL')
      and (ck.consumption_date between cast(co.contract_start_date as date) and coalesce(cast(co.contract_end_date as date), cast('2099-12-31' as date)))
),

combined_contract_matches as (
    select * from tier1
    union all
    select * from tier2
    union all
    select * from tier3
    union all
    select * from tier4
),

ranked_matches as (
    select
        *,
        row_number() over (
            partition by row_id
            order by contract_match_tier asc, contract_end_date desc nulls last
        ) as _rn
    from combined_contract_matches
)

select
    ck.row_id,
    ck.log_id,
    ck.standard_supplier as vendor_name_standard,
    upper(trim(ck.supplier)) as vendor_name_normalized,
    rm.contract_number,
    rm.contract_price,
    rm.contract_ea_price,
    rm.contract_ea_price as mapped_contract_ea_price,
    rm.contract_uom,
    rm.item_contract_category,
    rm.contract_start_date,
    rm.contract_end_date,
    coalesce(rm.contract_match_tier, 99) as contract_match_tier,
    coalesce(rm.contract_match_rule, 'no_match') as contract_match_rule,
    case when rm.contract_number is not null then true else false end as is_contract_matched,
    case 
        when rm.contract_number is not null then 'ON_CONTRACT'
        when ck.effective_item_id is null then 'NO_ITEM_IDENTIFIER'
        else 'NO_CONTRACT'
    end as contract_gap_code,
    case 
        when rm.contract_number is not null then concat('Matched on Tier ', cast(rm.contract_match_tier as varchar), ' (', rm.contract_match_rule, ')')
        when ck.effective_item_id is null then 'Missing item number/ID on consumption record'
        else 'No active contract for standard vendor and item/part on consumption date'
    end as contract_gap_detail
from cons_with_keys ck
left join ranked_matches rm on ck.row_id = rm.row_id and rm._rn = 1
