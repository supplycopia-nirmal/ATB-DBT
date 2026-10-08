{{ config(materialized='table') }}

with cons as (
    select * from {{ ref('int_consumption_normalized_v4') }}
),

im as (
    select * from {{ ref('int_item_master_enriched_v4') }}
),

-- Tier 1: exact item_number = item_id
tier1 as (
    select
        c.row_id,
        c.log_id,
        im.item_id as matched_item_id,
        1 as im_match_tier,
        'exact_item_id' as im_match_rule,
        1.0 as im_match_confidence_score
    from cons c
    inner join im on c.item_number = im.item_id
    where c.item_number is not null and c.item_number != ''
),

-- Tier 2: manufacturer_catalog_number = mfr_part_number
tier2 as (
    select
        c.row_id,
        c.log_id,
        im.item_id as matched_item_id,
        2 as im_match_tier,
        'mfr_part_number' as im_match_rule,
        0.90 as im_match_confidence_score
    from cons c
    inner join im on upper(trim(c.manufacturer_catalog_number)) = upper(trim(im.mfr_part_number))
    where c.row_id not in (select row_id from tier1)
      and c.manufacturer_catalog_number is not null 
      and trim(c.manufacturer_catalog_number) not in ('', 'N/A', 'NA', 'NONE', 'NULL')
),

-- Tier 3: vendor part number fallback
tier3 as (
    select
        c.row_id,
        c.log_id,
        im.item_id as matched_item_id,
        3 as im_match_tier,
        'vendor_part_number' as im_match_rule,
        0.80 as im_match_confidence_score
    from cons c
    inner join im on upper(trim(c.manufacturer_catalog_number)) = upper(trim(im.vendor_part_number))
    where c.row_id not in (select row_id from tier1)
      and c.row_id not in (select row_id from tier2)
      and c.manufacturer_catalog_number is not null
      and trim(c.manufacturer_catalog_number) not in ('', 'N/A', 'NA', 'NONE', 'NULL')
),

combined_matches as (
    select * from tier1
    union all
    select * from tier2
    union all
    select * from tier3
),

deduped_matches as (
    select 
        row_id,
        log_id,
        matched_item_id,
        im_match_tier,
        im_match_rule,
        im_match_confidence_score,
        row_number() over (
            partition by row_id 
            order by im_match_tier asc, im_match_confidence_score desc
        ) as _rn
    from combined_matches
)

select
    c.row_id,
    c.log_id,
    dm.matched_item_id,
    im_ref.unspsc_code as mapped_unspsc,
    coalesce(dm.im_match_tier, 99) as im_match_tier,
    coalesce(dm.im_match_rule, 'no_match') as im_match_rule,
    coalesce(dm.im_match_confidence_score, 0.0) as im_match_confidence_score,
    case when dm.matched_item_id is not null then true else false end as is_item_master_matched
from cons c
left join deduped_matches dm on c.row_id = dm.row_id and dm._rn = 1
left join im im_ref on dm.matched_item_id = im_ref.item_id
