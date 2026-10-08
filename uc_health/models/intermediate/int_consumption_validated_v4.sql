{{ config(materialized='table') }}

with cons as (
    select * from {{ ref('int_consumption_normalized_v4') }}
),

im_m as (
    select * from {{ ref('int_item_matching_v4') }}
),

con_m as (
    select * from {{ ref('int_contract_matching_v4') }}
),

flags as (
    select
        c.*,
        im.matched_item_id,
        im.mapped_unspsc,
        im.im_match_tier,
        im.im_match_rule,
        im.is_item_master_matched,
        con.vendor_name_standard,
        con.vendor_name_normalized,
        con.contract_number,
        con.contract_price,
        con.contract_ea_price,
        con.mapped_contract_ea_price,
        con.contract_uom,
        con.item_contract_category,
        con.contract_match_tier,
        con.contract_match_rule,
        con.is_contract_matched,
        con.contract_gap_code,
        con.contract_gap_detail,
        case when c.supply_unit_price is not null and c.supply_unit_price > 0 then true else false end as is_price_above_zero,
        case when c.total_quantity is not null and c.total_quantity > 0 then true else false end as is_quantity_above_zero,
        case when c.admit_date_time is not null then true else false end as is_valid_admit_date,
        case when con.contract_start_date is not null then true else false end as is_contract_date_valid
    from cons c
    left join im_m im on c.row_id = im.row_id
    left join con_m con on c.row_id = con.row_id
),

errors as (
    select
        f.*,
        (
            (case when not f.is_price_above_zero then 1 else 0 end) +
            (case when not f.is_quantity_above_zero then 1 else 0 end) +
            (case when not f.is_valid_admit_date then 1 else 0 end) +
            (case when not f.is_item_master_matched then 1 else 0 end) +
            (case when not f.is_contract_matched then 1 else 0 end)
        ) as validation_error_count,
        concat(
            case when not f.is_price_above_zero then ';NO_PRICE' else '' end,
            case when not f.is_quantity_above_zero then ';ZERO_QTY' else '' end,
            case when not f.is_valid_admit_date then ';INVALID_DATE' else '' end,
            case when not f.is_item_master_matched then ';NO_ITEM_MASTER' else '' end,
            case when not f.is_contract_matched then ';NO_CONTRACT' else '' end
        ) as validation_error_flags
    from flags f
),

eligibility as (
    select
        e.*,
        case
            when e.is_price_above_zero and e.is_quantity_above_zero and e.is_valid_admit_date and e.is_contract_matched then 'ELIGIBLE'
            when not e.is_price_above_zero or not e.is_quantity_above_zero or not e.is_valid_admit_date then 'INELIGIBLE'
            else 'REVIEW_REQUIRED'
        end as row_eligibility
    from errors e
)

select * from eligibility
