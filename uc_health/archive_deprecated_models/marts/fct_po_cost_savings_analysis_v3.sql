{{ config(materialized='table') }}

with po_data as (
    select
        *,
        ROUND(
          case 
              when try_cast(current_contract_price as double) > 0 then 
                  (try_cast(unit_price as double) - coalesce(try_cast(case when contract_uom_matches_po_uom = 'Y' then current_contract_price else current_contract_ea_price end as double), 0)) * try_cast(quantity as double)
              else 0
          end,
        2) as price_variance,
        ROUND(
          case 
              when try_cast(current_contract_price as double) > 0 then 
                  (try_cast(unit_price as double) - coalesce(try_cast(case when contract_uom_matches_po_uom = 'Y' then current_contract_price else current_contract_ea_price * try_cast(coalesce(try_cast(uom_conv_factor as double), 1) as double) end as double), 0)) * try_cast(quantity as double)
              else 0
          end,
        2) as price_variance2
    from {{ ref('int_po_enriched') }}
)

select 
    * EXCLUDE (
        uom,
        unit_price,
        quantity,
        current_contract_price,
        current_contract_ea_price,
        current_contract_uom,
        contract_uom_matches_po_uom,
        price_variance,
        price_variance2
    ),
    uom,
    unit_price,
    quantity,
    current_contract_uom,
    current_contract_price,
    current_contract_ea_price,
    contract_uom_matches_po_uom,
    price_variance,
    price_variance2
from po_data
