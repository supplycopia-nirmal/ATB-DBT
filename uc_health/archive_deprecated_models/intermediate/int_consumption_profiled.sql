{{ config(materialized='table') }}

SELECT
    c.*,
    m.primary_drg_code,
    m.primary_procedure_group
FROM {{ ref('stg_consumption') }} c
LEFT JOIN {{ ref('int_consumption_drg_mapping') }} m
ON c.drg_code = m.drg_code AND c.primary_procedure = m.primary_procedure
