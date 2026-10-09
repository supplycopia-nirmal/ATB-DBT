{{ config(materialized='table') }}

with item_master as (
    select * from {{ ref('stg_item_master_v4') }}
),

pcm as (
    select * from {{ ref('product_class_master_1') }}
),

classified as (
    select
        im.*,
        pcm.final_class as pcm_final_class,
        pcm.final_subclass as pcm_final_subclass,
        pcm.unspsc_description as pcm_unspsc_description,
        case 
            when im.contract_qoe is not null and im.contract_qoe > 0 then im.contract_price / im.contract_qoe
            else im.contract_price
        end as im_unit_contract_price,
        case when im.vendor_code is null or trim(im.vendor_code) in ('', 'N/A', 'NA', 'NULL') then true else false end as is_missing_vendor_code,
        case when im.mfr_name is null or trim(im.mfr_name) in ('', 'N/A', 'NA', 'NULL') then true else false end as is_missing_mfr_name,
        coalesce(
            pcm.final_class,
            case
                when upper(coalesce(im.item_description, '')) like '%IMPLANT%'
                  or upper(coalesce(im.item_description, '')) like '%SCREW%'
                  or upper(coalesce(im.item_description, '')) like '%PLATE%'
                  or upper(coalesce(im.item_description, '')) like '%SPINE%'
                  or upper(coalesce(im.item_description, '')) like '%BONE%' then 'Orthopedic / Implants'
                when upper(coalesce(im.item_description, '')) like '%STENT%'
                  or upper(coalesce(im.item_description, '')) like '%PACEMAKER%'
                  or upper(coalesce(im.item_description, '')) like '%BALLOON%'
                  or upper(coalesce(im.item_description, '')) like '%CATH%' then 'Cardiology'
                when upper(coalesce(im.item_description, '')) like '%GLOVE%'
                  or upper(coalesce(im.item_description, '')) like '%MASK%'
                  or upper(coalesce(im.item_description, '')) like '%GOWN%'
                  or upper(coalesce(im.item_description, '')) like '%PPE%' then 'PPE / Apparel'
                when upper(coalesce(im.item_description, '')) like '%SUTURE%'
                  or upper(coalesce(im.item_description, '')) like '%STAPLE%'
                  or upper(coalesce(im.item_description, '')) like '%BLADE%' then 'Surgical Supplies'
                when upper(coalesce(im.item_description, '')) like '%DRESSING%'
                  or upper(coalesce(im.item_description, '')) like '%GAUZE%'
                  or upper(coalesce(im.item_description, '')) like '%WOUND%' then 'Wound Care'
                when upper(coalesce(im.item_description, '')) like '%SYRINGE%'
                  or upper(coalesce(im.item_description, '')) like '%NEEDLE%'
                  or upper(coalesce(im.item_description, '')) like '%IV%' then 'IV & Injection'
                when im.unspsc_description is not null and trim(im.unspsc_description) != '' then trim(im.unspsc_description)
                else 'General Medical (Unclassified)'
            end
        ) as custom_category
    from item_master im
    left join pcm on im.unspsc_code = pcm.unspsc_code
),

scored as (
    select
        c.*,
        coalesce(c.pcm_final_subclass, 'Unclassified') as product_subclass,
        coalesce(c.pcm_unspsc_description, c.unspsc_description) as final_unspsc_description,
        (
            100 
            - (case when c.is_missing_vendor_code then 20 else 0 end)
            - (case when c.is_missing_mfr_name then 20 else 0 end)
            - (case when c.custom_category = 'General Medical (Unclassified)' then 10 else 0 end)
        ) as data_quality_score
    from classified c
)

select * from scored
