{{ config(materialized='table') }}

with consumption as (
    select * from {{ ref('stg_consumption_enriched_v3') }}
),
drg_mapping as (
    select * from {{ ref('int_consumption_drg_mapping') }}
),
cons_data as (
    select
        c.*,
        d.primary_drg_code,
        d.primary_procedure_group,
        ROUND(
          case 
              when try_cast(current_contract_price as double) > 0 then 
                  (try_cast(supply_unit_price as double) - coalesce(try_cast(case when ITEM_UOM = current_contract_uom then current_contract_price else current_contract_ea_price end as double), 0)) * try_cast(TOTAL_QUANTITY as double)
              else 0
          end,
        2) as price_variance,
        ROUND(
          case 
              when try_cast(current_contract_price as double) > 0 then 
                  (try_cast(supply_unit_price as double) - coalesce(try_cast(case when ITEM_UOM = current_contract_uom then current_contract_price else current_contract_ea_price * try_cast(coalesce(try_cast(ITEM_QOE as double), 1) as double) end as double), 0)) * try_cast(TOTAL_QUANTITY as double)
              else 0
          end,
        2) as price_variance2
    from consumption c
    left join drg_mapping d 
      on coalesce(c.DRG_CODE, '') = coalesce(d.drg_code, '') 
      and coalesce(c.PRIMARY_PROCEDURE, '') = coalesce(d.primary_procedure, '')
)

select
    * EXCLUDE (
        ITEM_UOM,
        supply_unit_price,
        TOTAL_QUANTITY,
        current_contract_price,
        current_contract_ea_price,
        current_contract_uom,
        price_variance,
        price_variance2
    ),
    ITEM_UOM,
    supply_unit_price,
    TOTAL_QUANTITY,
    current_contract_uom,
    current_contract_price,
    current_contract_ea_price,
    price_variance,
    price_variance2
from cons_data
