{{ config(materialized='table') }}

with consumption as (
    select * from {{ ref('stg_consumption_v4') }}
),

facility_map as (
    select * from {{ ref('facility_mapping') }}
),

normalized as (
    select
        c.*,
        cast(c.admit_date_time as date) as consumption_date,
        strftime(c.admit_date_time, '%Y-%m') as consumption_year_month,
        coalesce(f.standard_facility, c.facility) as standard_facility,
        f.facility_region,
        case when c.supply_unit_price is not null and c.supply_unit_price > 0 then true else false end as is_valid_price,
        case when c.total_quantity is not null and c.total_quantity > 0 then true else false end as is_valid_quantity,
        case when c.admit_date_time is not null then true else false end as is_valid_date,
        (c.supply_unit_price * c.total_quantity) as line_spend,
        row_number() over () as row_id
    from consumption c
    left join facility_map f on upper(trim(c.facility)) = upper(trim(f.source_facility))
)

select * from normalized
