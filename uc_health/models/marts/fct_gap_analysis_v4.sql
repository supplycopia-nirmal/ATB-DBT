{{ config(materialized='table') }}

with cons_mart as (
    select * from {{ ref('fct_consumption_cost_savings_v4') }}
),

aggregated as (
    select
        coalesce(primary_procedure_group, 'Uncategorized Procedure') as primary_procedure_group,
        coalesce(service_line, 'General Medicine') as service_line,
        coalesce(facility, 'System-wide') as facility,
        coalesce(patient_type, 'Inpatient') as patient_type,
        count(*) as total_items,
        sum(case when is_contract_matched then 1 else 0 end) as matched_items,
        sum(case when not is_contract_matched then 1 else 0 end) as unmatched_items,
        round(
            cast(sum(case when is_contract_matched then 1 else 0 end) as double) / nullif(count(*), 0) * 100.0,
            2
        ) as match_rate,
        round(sum(coalesce(line_spend, 0.0)), 2) as total_spend,
        round(sum(case when is_contract_matched then coalesce(line_spend, 0.0) else 0.0 end), 2) as spend_with_contract_match,
        round(sum(case when not is_contract_matched then coalesce(line_spend, 0.0) else 0.0 end), 2) as spend_without_contract_match,
        mode(contract_gap_code) as top_gap_code,
        count(distinct contract_gap_code) as total_unique_gap_codes,
        round(sum(coalesce(savings_opportunity, 0.0)), 2) as total_savings_opportunity,
        round(sum(coalesce(overpayment_amount, 0.0)), 2) as total_overpayment_amount
    from cons_mart
    group by 
        coalesce(primary_procedure_group, 'Uncategorized Procedure'),
        coalesce(service_line, 'General Medicine'),
        coalesce(facility, 'System-wide'),
        coalesce(patient_type, 'Inpatient')
)

select * from aggregated
