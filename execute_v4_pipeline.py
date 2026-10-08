import sys, os
sys.path.insert(0, '/Users/piyu/Library/Python/3.9/lib/python/site-packages')
import duckdb

con = duckdb.connect('uc_health/uc_health.duckdb')

con.execute("PRAGMA threads=4;")
con.execute("PRAGMA preserve_insertion_order=false;")

print('Step 4: Deterministic 1-to-1 match for int_contract_matching_v4...')

con.execute("""
CREATE OR REPLACE TABLE _tmp_cons_keys AS
SELECT
    c.log_id,
    c.consumption_date,
    c.item_uom,
    c.manufacturer_catalog_number,
    coalesce(va.standard_vendor, c.supplier) as standard_supplier,
    coalesce(im_m.matched_item_id, c.item_number) as effective_item_id
FROM int_consumption_normalized_v4 c
LEFT JOIN int_item_matching_v4 im_m ON c.log_id = im_m.log_id
LEFT JOIN vendor_alias_table va ON upper(trim(c.supplier)) = upper(trim(va.alias_name));
""")

# Build distinct candidates table with best contract selection (latest end date, priority tier)
con.execute("""
CREATE OR REPLACE TABLE _tmp_best_contract_candidates AS
WITH 
t1 AS (
    SELECT DISTINCT
        k.effective_item_id, k.standard_supplier, k.item_uom, k.manufacturer_catalog_number,
        co.contract_number, co.contract_price, co.contract_ea_price, co.contract_uom,
        co.contract_start_date, co.contract_end_date, 1 as contract_match_tier, 'item_vendor_part_uom' as contract_match_rule
    FROM (SELECT DISTINCT effective_item_id, standard_supplier, item_uom, manufacturer_catalog_number FROM _tmp_cons_keys) k
    JOIN stg_contracts_v4 co
      ON k.effective_item_id = co.item_id
     AND upper(trim(k.standard_supplier)) = upper(trim(co.vendor_name))
     AND upper(trim(coalesce(k.manufacturer_catalog_number, ''))) = upper(trim(coalesce(co.manufacturer_part_number, '')))
     AND upper(trim(k.item_uom)) = upper(trim(co.contract_uom))
),
t2 AS (
    SELECT DISTINCT
        k.effective_item_id, k.standard_supplier, k.item_uom, k.manufacturer_catalog_number,
        co.contract_number, co.contract_price, co.contract_ea_price, co.contract_uom,
        co.contract_start_date, co.contract_end_date, 2 as contract_match_tier, 'item_vendor_uom' as contract_match_rule
    FROM (SELECT DISTINCT effective_item_id, standard_supplier, item_uom, manufacturer_catalog_number FROM _tmp_cons_keys) k
    JOIN stg_contracts_v4 co
      ON k.effective_item_id = co.item_id
     AND upper(trim(k.standard_supplier)) = upper(trim(co.vendor_name))
     AND upper(trim(k.item_uom)) = upper(trim(co.contract_uom))
    WHERE (k.effective_item_id, k.standard_supplier, k.item_uom, k.manufacturer_catalog_number) NOT IN 
          (SELECT (effective_item_id, standard_supplier, item_uom, manufacturer_catalog_number) FROM t1)
),
t3 AS (
    SELECT DISTINCT
        k.effective_item_id, k.standard_supplier, k.item_uom, k.manufacturer_catalog_number,
        co.contract_number, co.contract_price, co.contract_ea_price, co.contract_uom,
        co.contract_start_date, co.contract_end_date, 3 as contract_match_tier, 'item_vendor_any_uom' as contract_match_rule
    FROM (SELECT DISTINCT effective_item_id, standard_supplier, item_uom, manufacturer_catalog_number FROM _tmp_cons_keys) k
    JOIN stg_contracts_v4 co
      ON k.effective_item_id = co.item_id
     AND upper(trim(k.standard_supplier)) = upper(trim(co.vendor_name))
    WHERE (k.effective_item_id, k.standard_supplier, k.item_uom, k.manufacturer_catalog_number) NOT IN 
          (SELECT (effective_item_id, standard_supplier, item_uom, manufacturer_catalog_number) FROM t1)
      AND (k.effective_item_id, k.standard_supplier, k.item_uom, k.manufacturer_catalog_number) NOT IN 
          (SELECT (effective_item_id, standard_supplier, item_uom, manufacturer_catalog_number) FROM t2)
),
all_matches AS (
    SELECT * FROM t1
    UNION ALL SELECT * FROM t2
    UNION ALL SELECT * FROM t3
),
deduped AS (
    SELECT *, row_number() OVER (
        PARTITION BY effective_item_id, standard_supplier, item_uom, manufacturer_catalog_number
        ORDER BY contract_match_tier ASC, contract_end_date DESC NULLS LAST
    ) as rn
    FROM all_matches
)
SELECT * FROM deduped WHERE rn = 1;
""")
print('Candidate table rows:', con.execute('SELECT count(*) FROM _tmp_best_contract_candidates').fetchone()[0])

con.execute("""
CREATE OR REPLACE TABLE int_contract_matching_v4 AS
SELECT
    k.log_id,
    c.contract_number,
    c.contract_price,
    c.contract_ea_price,
    c.contract_uom,
    c.contract_start_date,
    c.contract_end_date,
    coalesce(c.contract_match_tier, 99) as contract_match_tier,
    coalesce(c.contract_match_rule, 'no_match') as contract_match_rule,
    (c.contract_number IS NOT NULL AND k.consumption_date BETWEEN cast(c.contract_start_date as date) AND coalesce(cast(c.contract_end_date as date), date '2099-12-31')) as is_contract_matched,
    CASE 
        WHEN c.contract_number IS NOT NULL AND k.consumption_date BETWEEN cast(c.contract_start_date as date) AND coalesce(cast(c.contract_end_date as date), date '2099-12-31') THEN 'ON_CONTRACT'
        WHEN c.contract_number IS NOT NULL THEN 'LAPSED_CONTRACT'
        WHEN k.effective_item_id IS NULL THEN 'NO_ITEM_IDENTIFIER'
        ELSE 'NO_CONTRACT'
    END as contract_gap_code,
    CASE 
        WHEN c.contract_number IS NOT NULL AND k.consumption_date BETWEEN cast(c.contract_start_date as date) AND coalesce(cast(c.contract_end_date as date), date '2099-12-31') THEN concat('Matched on Tier ', cast(c.contract_match_tier as varchar), ' (', c.contract_match_rule, ')')
        WHEN c.contract_number IS NOT NULL THEN 'Contract expired or not yet active on consumption date'
        WHEN k.effective_item_id IS NULL THEN 'Missing item number/ID on consumption record'
        ELSE 'No active contract for standard vendor and item on consumption date'
    END as contract_gap_detail
FROM _tmp_cons_keys k
LEFT JOIN _tmp_best_contract_candidates c
  ON k.effective_item_id = c.effective_item_id
 AND k.standard_supplier = c.standard_supplier
 AND k.item_uom = c.item_uom
 AND coalesce(k.manufacturer_catalog_number, '') = coalesce(c.manufacturer_catalog_number, '');
""")

con.execute("DROP TABLE IF EXISTS _tmp_cons_keys;")
con.execute("DROP TABLE IF EXISTS _tmp_best_contract_candidates;")
print('int_contract_matching_v4 exact rows:', con.execute('SELECT count(*) FROM int_contract_matching_v4').fetchone()[0])

print('Step 5: Building int_consumption_validated_v4...')
con.execute("""
CREATE OR REPLACE TABLE int_consumption_validated_v4 AS
with cons as (
    select * from int_consumption_normalized_v4
),
im_m as (
    select * from int_item_matching_v4
),
con_m as (
    select * from int_contract_matching_v4
),
flags as (
    select
        c.*,
        im.matched_item_id,
        im.im_match_tier,
        im.im_match_rule,
        im.is_item_master_matched,
        con.contract_number,
        con.contract_price,
        con.contract_ea_price,
        con.contract_uom,
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
    left join im_m im on c.log_id = im.log_id
    left join con_m con on c.log_id = con.log_id
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
select * from eligibility;
""")
print('int_consumption_validated_v4 rows:', con.execute('SELECT count(*) FROM int_consumption_validated_v4').fetchone()[0])

print('Step 6: Building int_drg_mapping_v4...')
con.execute("""
CREATE OR REPLACE TABLE int_drg_mapping_v4 AS
with distinct_drg as (
    select distinct drg_code, primary_procedure
    from int_consumption_normalized_v4
    where drg_code is not null or primary_procedure is not null
)
select
    drg_code,
    primary_procedure,
    split_part(coalesce(drg_code, ''), '|', 1) as primary_drg_code,
    case
        when upper(coalesce(primary_procedure, '')) like '%KNEE%' or upper(coalesce(primary_procedure, '')) like '%HIP%' or upper(coalesce(primary_procedure, '')) like '%ARTHROPLASTY%' then 'Orthopedic Reconstruction'
        when upper(coalesce(primary_procedure, '')) like '%SPINE%' or upper(coalesce(primary_procedure, '')) like '%FUSION%' then 'Spinal Surgery'
        when upper(coalesce(primary_procedure, '')) like '%CORONARY%' or upper(coalesce(primary_procedure, '')) like '%VALVE%' or upper(coalesce(primary_procedure, '')) like '%CARDIAC%' then 'Cardiovascular Surgery'
        when upper(coalesce(primary_procedure, '')) like '%COLON%' or upper(coalesce(primary_procedure, '')) like '%BOWEL%' or upper(coalesce(primary_procedure, '')) like '%HERNIA%' then 'General & Colorectal Surgery'
        when upper(coalesce(primary_procedure, '')) like '%NEURO%' or upper(coalesce(primary_procedure, '')) like '%BRAIN%' or upper(coalesce(primary_procedure, '')) like '%CRANIAL%' then 'Neurosurgery'
        else 'General Clinical Procedure'
    end as primary_procedure_group,
    'v4_pipeline_rule_engine' as llm_model_used,
    'v4.0' as llm_prompt_version,
    current_timestamp as llm_generated_at
from distinct_drg;
""")
print('int_drg_mapping_v4 rows:', con.execute('SELECT count(*) FROM int_drg_mapping_v4').fetchone()[0])

print('Step 7: Building fct_consumption_cost_savings_v4...')
con.execute("""
CREATE OR REPLACE TABLE fct_consumption_cost_savings_v4 AS
with cons as (
    select * from int_consumption_validated_v4
),
drg as (
    select * from int_drg_mapping_v4
),
enriched as (
    select
        c.*,
        d.primary_drg_code,
        d.primary_procedure_group,
        round(
            case 
                when coalesce(try_cast(c.contract_price as double), 0) > 0 then
                    (try_cast(c.supply_unit_price as double) - coalesce(
                        case 
                            when c.item_uom = c.contract_uom then try_cast(c.contract_price as double)
                            else try_cast(c.contract_ea_price as double) * coalesce(try_cast(c.uom_conversion_factor as double), 1.0)
                        end, 0.0)
                    ) * try_cast(c.total_quantity as double)
                else 0.0
            end,
            2
        ) as price_variance2
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
            when not e.is_contract_matched then (e.supply_unit_price * e.total_quantity) * 0.15
            else 0.0
        end as savings_opportunity,
        case 
            when e.is_contract_matched and abs(coalesce(e.price_variance2, 0.0)) <= 0.005 * coalesce(e.line_spend, 1.0) then true
            else false
        end as is_contract_compliant
    from enriched e
)
select * from calculated;
""")
print('fct_consumption_cost_savings_v4 rows:', con.execute('SELECT count(*) FROM fct_consumption_cost_savings_v4').fetchone()[0])

print('Step 8: Building fct_po_cost_savings_v4...')
con.execute("""
CREATE OR REPLACE TABLE fct_po_cost_savings_v4 AS
with po as (
    select * from stg_po_v4
),
im as (
    select * from int_item_master_enriched_v4
),
con as (
    select * from stg_contracts_v4
),
inv as (
    select * from stg_invoice_v4
),
inv_agg as (
    select
        po_number,
        po_line_no,
        count(distinct invoice_number) as invoice_count,
        sum(invoice_qty) as total_invoiced_qty,
        sum(invoice_total_value) as total_invoiced_amount,
        min(invoice_paid_date) as first_invoice_paid_date,
        max(invoice_paid_date) as last_invoice_paid_date
    from inv
    group by po_number, po_line_no
),
po_con as (
    select
        p.*,
        c.contract_number as matched_contract_number,
        c.contract_price as matched_contract_price,
        c.contract_ea_price as matched_contract_ea_price,
        c.contract_uom as matched_contract_uom,
        c.contract_category,
        row_number() over (
            partition by p.po_number, p.po_line_no
            order by c.contract_end_date desc nulls last
        ) as _rn
    from po p
    left join con c
        on p.item_id = c.item_id
       and (p.po_date between c.contract_start_date and coalesce(c.contract_end_date, cast('2099-12-31' as timestamp)))
),
joined as (
    select
        pc.*,
        im.custom_category as product_class,
        im.data_quality_score,
        ia.invoice_count,
        ia.total_invoiced_qty,
        ia.total_invoiced_amount,
        ia.first_invoice_paid_date,
        ia.last_invoice_paid_date,
        case when pc.matched_contract_number is not null then true else false end as is_contract_matched,
        round(
            case 
                when coalesce(try_cast(pc.matched_contract_price as double), 0) > 0 then
                    (try_cast(pc.unit_price as double) - coalesce(
                        case 
                            when pc.uom = pc.matched_contract_uom then try_cast(pc.matched_contract_price as double)
                            else try_cast(pc.matched_contract_ea_price as double) * coalesce(try_cast(pc.uom_conv_factor as double), 1.0)
                        end, 0.0)
                    ) * try_cast(pc.quantity as double)
                else 0.0
            end,
            2
        ) as price_variance2
    from po_con pc
    left join im on pc.item_id = im.item_id
    left join inv_agg ia on pc.po_number = ia.po_number and pc.po_line_no = ia.po_line_no
    where pc._rn = 1
),
finalized as (
    select
        j.*,
        case when j.price_variance2 > 0 then j.price_variance2 else 0.0 end as overpayment_amount,
        case 
            when j.is_contract_matched and j.price_variance2 > 0 then j.price_variance2
            when not j.is_contract_matched then coalesce(j.total_value, 0.0) * 0.15
            else 0.0
        end as savings_opportunity,
        case 
            when j.is_contract_matched and abs(coalesce(j.price_variance2, 0.0)) <= 0.005 * coalesce(j.total_value, 1.0) then true
            else false
        end as is_contract_compliant
    from joined j
)
select * from finalized;
""")
print('fct_po_cost_savings_v4 rows:', con.execute('SELECT count(*) FROM fct_po_cost_savings_v4').fetchone()[0])

print('Step 9: Building fct_gap_analysis_v4...')
con.execute("""
CREATE OR REPLACE TABLE fct_gap_analysis_v4 AS
with cons_mart as (
    select * from fct_consumption_cost_savings_v4
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
select * from aggregated;
""")
print('fct_gap_analysis_v4 rows:', con.execute('SELECT count(*) FROM fct_gap_analysis_v4').fetchone()[0])

print('ALL V4 TABLES COMPLETED SUCCESSFULLY!')
con.close()
