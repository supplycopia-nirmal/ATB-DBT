{{ config(materialized='table') }}

with cons as (
    select * from {{ ref('int_consumption_validated_v4') }}
),

drg as (
    select * from {{ ref('int_drg_mapping_v4') }}
),

enriched as (
    select
        c.*,
        d.primary_drg_code,
        d.primary_procedure_group,
        {{ price_variance2('c.supply_unit_price', 'c.contract_price', 'c.contract_ea_price', "case when c.item_uom = c.contract_uom then 'Y' else 'N' end", 'c.uom_conversion_factor', 'c.total_quantity') }} as price_variance2
    from cons c
    left join drg d 
        on coalesce(c.drg_code, '') = coalesce(d.drg_code, '')
       and coalesce(c.primary_procedure, '') = coalesce(d.primary_procedure, '')
),

calculated as (
    select
        e.*,
        case 
            when e.price_variance2 > 0 then e.price_variance2 
            else 0.0 
        end as overpayment_amount,
        case 
            when e.is_contract_matched and e.price_variance2 > 0 then e.price_variance2
            when not e.is_contract_matched then (e.supply_unit_price * e.total_quantity) * 0.15 -- Estimated 15% benchmark saving for off-contract
            else 0.0
        end as savings_opportunity,
        case 
            when e.is_contract_matched and abs(coalesce(e.price_variance2, 0.0)) <= {{ var('price_variance_threshold') }} * coalesce(e.line_spend, 1.0) then true
            else false
        end as is_contract_compliant
    from enriched e
)

select * from calculated
