{{ config(materialized='table') }}

with cons as (
    select * from {{ ref('int_consumption_normalized_v4') }}
),

po as (
    select * from {{ ref('stg_po_v4') }}
),

matched as (
    select
        c.log_id,
        p.po_number,
        p.po_line_no,
        p.unit_price as po_unit_price,
        p.quantity as po_quantity,
        p.uom as po_uom,
        p.contract_number as po_contract_number,
        p.po_date,
        row_number() over (
            partition by c.log_id
            order by abs(date_diff('day', cast(c.admit_date_time as date), cast(p.po_date as date))) asc, p.po_date desc
        ) as _rn
    from cons c
    inner join po p
        on c.item_number = p.item_id
       and upper(trim(c.facility)) = upper(trim(p.facility_name))
       and abs(date_diff('day', cast(c.admit_date_time as date), cast(p.po_date as date))) <= {{ var('po_invoice_match_window_days') }}
)

select
    c.log_id,
    m.po_number,
    m.po_line_no,
    m.po_unit_price,
    m.po_quantity,
    m.po_uom,
    m.po_contract_number,
    m.po_date,
    case when m.po_number is not null then true else false end as is_po_matched
from cons c
left join matched m on c.log_id = m.log_id and m._rn = 1
