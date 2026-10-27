{{ config(materialized='view') }}

select * from {{ ref('int_consumption_enriched_v3') }}
