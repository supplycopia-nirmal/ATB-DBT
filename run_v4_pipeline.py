import sys, time
sys.path.insert(0, '/Users/piyu/Library/Python/3.9/lib/python/site-packages')
import duckdb

t0 = time.time()
con = duckdb.connect('uc_health/uc_health.duckdb')
con.execute("PRAGMA threads=4;")

print("0. Updating stg_consumption_v4 view...")
con.execute("""
CREATE OR REPLACE VIEW stg_consumption_v4 AS
with source as (
    select * from read_csv('Data/UHC_Consumption.csv', delim='|', header=True, all_varchar=True, null_padding=True, ignore_errors=True)
),
uom_mapping as (
    select * from uom_mappings
),
parsed as (
    select
        LOG_ID as log_id,
        FACILITY as facility,
        MEDICAL_RECORD_NUMBER as medical_record_number,
        "CASE_ID / ENCOUNTER FHIR ID" as case_id,
        "CASE_ID / ENCOUNTER FHIR ID" as "CASE_ID / ENCOUNTER FHIR ID",
        DRG_CODE as drg_code,
        trim(SURGICAL_HIERARCHY) as surgical_hierarchy,
        trim(SURGICAL_HIERARCHY) as "SURGICAL_HIERARCHY",
        trim(BILLED_CPT_CODE) as billed_cpt_code,
        trim(BILLED_CPT_CODE) as "BILLED_CPT_CODE",
        trim(PRIMARY_ICD10_PX_CODE) as primary_icd10_px_code,
        trim(PRIMARY_ICD10_PX_CODE) as "PRIMARY_ICD10_PX_CODE",
        PRIMARY_PROCEDURE as primary_procedure,
        SERVICE_LINE as service_line,
        PATIENT_TYPE as patient_type,
        LEAD_SURGEON as lead_surgeon,
        trim(PAYOR_GROUP) as payor_group,
        trim(PAYOR_GROUP) as "PAYOR_GROUP",
        try_cast(ADMIT_DATE_TIME as timestamp) as admit_date_time,
        try_cast(DISCHARGE_DATE_TIME as timestamp) as discharge_date_time,
        try_cast(LOS as double) as length_of_stay,
        try_cast(LOS as double) as "LOS",
        try_cast(GMLOS as double) as gmlos,
        try_cast(GMLOS as double) as "GMLOS",
        ACCOUNT_NUMBER as account_number,
        try_cast(CONTRACT_PRICE as double) as contract_price,
        try_cast(TOTAL_ACQUISITION_COST as double) as total_acquisition_cost,
        try_cast(SUPPLY_UNIT_PRICE as double) as supply_unit_price,
        try_cast(TOTAL_QUANTITY as double) as total_quantity,
        try_cast(IMPLANT_VAR_DIRECT_COST as double) as implant_var_direct_cost,
        try_cast(IMPLANT_VAR_DIRECT_COST as double) as "IMPLANT_VAR_DIRECT_COST",
        try_cast(MED_SUPPLY_VAR_DIRECT_COST as double) as med_supply_var_direct_cost,
        try_cast(MED_SUPPLY_VAR_DIRECT_COST as double) as "MED_SUPPLY_VAR_DIRECT_COST",
        try_cast(TOTAL_CHARGES as double) as total_charges,
        try_cast(TOTAL_ACCT_BAL as double) as total_acct_bal,
        try_cast(TOTAL_ACCT_BAL as double) as "TOTAL_ACCT_BAL",
        try_cast(TOTAL_ADJ as double) as total_adj,
        try_cast(TOTAL_ADJ as double) as "TOTAL_ADJ",
        try_cast(TOTAL_PMTS as double) as total_pmts,
        try_cast(TOTAL_PMTS as double) as "TOTAL_PMTS",
        trim("SSI (0/1)") as ssi_flag,
        trim("SSI (0/1)") as "SSI (0/1)",
        trim("BLOOD_TRANSFUSION_FLAG (0/1)") as blood_transfusion_flag,
        trim("BLOOD_TRANSFUSION_FLAG (0/1)") as "BLOOD_TRANSFUSION_FLAG (0/1)",
        trim("READMISSION_INDEX_CASE (0/1)") as readmission_index_case,
        trim("READMISSION_INDEX_CASE (0/1)") as "READMISSION_INDEX_CASE (0/1)",
        trim("MORTALITY (0/1)") as mortality_flag,
        trim("MORTALITY (0/1)") as "MORTALITY (0/1)",
        trim("RISK OF MORTALITY") as risk_of_mortality,
        trim("RISK OF MORTALITY") as "RISK OF MORTALITY",
        trim(MANUFACTURER_NAME) as manufacturer_name,
        trim(MANUFACTURER_CATALOG_NUMBER) as manufacturer_catalog_number,
        trim(ITEM_NUMBER) as item_number,
        trim(ITEM_DESCRIPTION) as item_description,
        upper(trim(coalesce(ITEM_UOM, 'EA'))) as raw_item_uom,
        try_cast(ITEM_QOE as double) as item_qoe,
        try_cast(ITEM_QOE as double) as "ITEM_QOE",
        trim(SUPPLIER) as supplier,
        trim(CONTRACT_CATEGORY) as contract_category,
        trim(SPEND_CATEGORY) as spend_category,
        trim(UNSPSC_CODE) as unspsc_code,
        trim(CONTRACT_FLAG) as contract_flag,
        trim(ASA_RATING) as asa_rating,
        trim(ASA_RATING) as "ASA_RATING",
        trim(BMI_BUCKET) as bmi_bucket,
        trim(BMI_BUCKET) as "BMI_BUCKET",
        trim("ROBOTICS (0/1)") as robotics_flag,
        trim("ROBOTICS (0/1)") as "ROBOTICS (0/1)",
        trim(SMOKING_STATUS) as smoking_status,
        trim(SMOKING_STATUS) as "SMOKING_STATUS",
        trim("DIABETIC_STATUS (0/1)") as diabetic_status,
        trim("DIABETIC_STATUS (0/1)") as "DIABETIC_STATUS (0/1)",
        trim(PATIENT_AGE_BUCKET) as patient_age_bucket,
        trim(PATIENT_AGE_BUCKET) as "PATIENT_AGE_BUCKET",
        trim(PATIENT_GENDER) as patient_gender,
        trim(PATIENT_GENDER) as "PATIENT_GENDER",
        trim(ETHNICITY) as ethnicity,
        trim(ETHNICITY) as "ETHNICITY"
    from source
),
standardized as (
    select
        p.*,
        coalesce(u.standard_uom, case when p.raw_item_uom in ('EACH', 'EACHES') then 'EA' when p.raw_item_uom in ('CASE', 'CS') then 'CA' when p.raw_item_uom in ('BOX', 'BX') then 'BX' else p.raw_item_uom end) as item_uom,
        coalesce(u.ea_conversion_factor, 1.0) as uom_conversion_factor,
        'UHC_Consumption.csv' as _source_file,
        row_number() over () as _source_row_number,
        md5(concat(coalesce(cast(p.log_id as varchar), ''), coalesce(cast(p.item_number as varchar), ''), coalesce(cast(p.admit_date_time as varchar), ''), coalesce(cast(p.supply_unit_price as varchar), ''))) as _row_hash,
        current_timestamp as _ingested_at
    from parsed p
    left join uom_mapping u on p.raw_item_uom = u.uom_code
)
select * from standardized;
""")

print("0b. Rebuilding int_consumption_normalized_v4...")
con.execute("""
CREATE OR REPLACE TABLE int_consumption_normalized_v4 AS
with consumption as (
    select * from stg_consumption_v4
),
facility_map as (
    select * from facility_mapping
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
select * from normalized;
""")
print(f"int_consumption_normalized_v4 refreshed, rows: {con.execute('SELECT count(*) FROM int_consumption_normalized_v4').fetchone()[0]}")

print("1. Rebuilding int_item_matching_v4 keyed on row_id...")
con.execute("""
CREATE OR REPLACE TABLE int_item_matching_v4 AS
with cons as (
    select * from int_consumption_normalized_v4
),
im as (
    select * from int_item_master_enriched_v4
),
tier1 as (
    select
        c.row_id,
        c.log_id,
        im.item_id as matched_item_id,
        1 as im_match_tier,
        'exact_item_id' as im_match_rule,
        1.0 as im_match_confidence_score
    from cons c
    inner join im on c.item_number = im.item_id
    where c.item_number is not null and c.item_number != ''
),
tier2 as (
    select
        c.row_id,
        c.log_id,
        im.item_id as matched_item_id,
        2 as im_match_tier,
        'mfr_part_number' as im_match_rule,
        0.90 as im_match_confidence_score
    from cons c
    inner join im on upper(trim(c.manufacturer_catalog_number)) = upper(trim(im.mfr_part_number))
    where c.row_id not in (select row_id from tier1)
      and c.manufacturer_catalog_number is not null 
      and trim(c.manufacturer_catalog_number) not in ('', 'N/A', 'NA', 'NONE', 'NULL')
),
tier3 as (
    select
        c.row_id,
        c.log_id,
        im.item_id as matched_item_id,
        3 as im_match_tier,
        'vendor_part_number' as im_match_rule,
        0.80 as im_match_confidence_score
    from cons c
    inner join im on upper(trim(c.manufacturer_catalog_number)) = upper(trim(im.vendor_part_number))
    where c.row_id not in (select row_id from tier1)
      and c.row_id not in (select row_id from tier2)
      and c.manufacturer_catalog_number is not null
      and trim(c.manufacturer_catalog_number) not in ('', 'N/A', 'NA', 'NONE', 'NULL')
),
combined_matches as (
    select * from tier1
    union all
    select * from tier2
    union all
    select * from tier3
),
deduped_matches as (
    select 
        row_id,
        log_id,
        matched_item_id,
        im_match_tier,
        im_match_rule,
        im_match_confidence_score,
        row_number() over (
            partition by row_id 
            order by im_match_tier asc, im_match_confidence_score desc
        ) as _rn
    from combined_matches
)
select
    c.row_id,
    c.log_id,
    dm.matched_item_id,
    im_ref.unspsc_code as mapped_unspsc,
    coalesce(dm.im_match_tier, 99) as im_match_tier,
    coalesce(dm.im_match_rule, 'no_match') as im_match_rule,
    coalesce(dm.im_match_confidence_score, 0.0) as im_match_confidence_score,
    case when dm.matched_item_id is not null then true else false end as is_item_master_matched
from cons c
left join deduped_matches dm on c.row_id = dm.row_id and dm._rn = 1
left join im im_ref on dm.matched_item_id = im_ref.item_id;
""")
print(f"int_item_matching_v4 done in {time.time()-t0:.1f}s, rows: {con.execute('SELECT count(*) FROM int_item_matching_v4').fetchone()[0]}")

t1 = time.time()
print("2. Building int_contract_matching_v4 keyed on row_id...")
con.execute("""
CREATE OR REPLACE TABLE _tmp_cons_keys AS
SELECT
    c.row_id,
    c.log_id,
    c.consumption_date,
    c.item_uom,
    c.manufacturer_catalog_number,
    c.supplier,
    coalesce(va.standard_vendor, c.supplier) as standard_supplier,
    coalesce(im_m.matched_item_id, c.item_number) as effective_item_id
FROM int_consumption_normalized_v4 c
LEFT JOIN int_item_matching_v4 im_m ON c.row_id = im_m.row_id
LEFT JOIN clean_vendor_alias va ON upper(trim(c.supplier)) = upper(trim(va.alias_name));
""")

con.execute("""
CREATE OR REPLACE TABLE _tmp_best_contract_candidates AS
WITH 
t1 AS (
    SELECT DISTINCT
        k.effective_item_id, k.standard_supplier, k.item_uom, k.manufacturer_catalog_number,
        co.contract_number, co.contract_price, co.contract_ea_price, co.contract_uom, co.contract_category as item_contract_category,
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
        co.contract_number, co.contract_price, co.contract_ea_price, co.contract_uom, co.contract_category as item_contract_category,
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
        co.contract_number, co.contract_price, co.contract_ea_price, co.contract_uom, co.contract_category as item_contract_category,
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

con.execute("""
CREATE OR REPLACE TABLE int_contract_matching_v4 AS
SELECT
    k.row_id,
    k.log_id,
    k.standard_supplier as vendor_name_standard,
    upper(trim(k.supplier)) as vendor_name_normalized,
    c.contract_number,
    c.contract_price,
    c.contract_ea_price,
    c.contract_ea_price as mapped_contract_ea_price,
    c.contract_uom,
    c.item_contract_category,
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
print(f"int_contract_matching_v4 done in {time.time()-t1:.1f}s, rows: {con.execute('SELECT count(*) FROM int_contract_matching_v4').fetchone()[0]}")

t2 = time.time()
print("3. Building int_consumption_validated_v4 keyed on row_id...")
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
select * from eligibility;
""")
print(f"int_consumption_validated_v4 done in {time.time()-t2:.1f}s, rows: {con.execute('SELECT count(*) FROM int_consumption_validated_v4').fetchone()[0]}")

t3 = time.time()
print("4. Building int_drg_mapping_v4...")
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
print(f"int_drg_mapping_v4 done in {time.time()-t3:.1f}s, rows: {con.execute('SELECT count(*) FROM int_drg_mapping_v4').fetchone()[0]}")

t4 = time.time()
print("5. Building fct_consumption_cost_savings_v4...")
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
print(f"fct_consumption_cost_savings_v4 done in {time.time()-t4:.1f}s, rows: {con.execute('SELECT count(*) FROM fct_consumption_cost_savings_v4').fetchone()[0]}")

t5 = time.time()
print("6. Building fct_po_cost_savings_v4...")
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
print(f"fct_po_cost_savings_v4 done in {time.time()-t5:.1f}s, rows: {con.execute('SELECT count(*) FROM fct_po_cost_savings_v4').fetchone()[0]}")

t6 = time.time()
print("7. Building fct_gap_analysis_v4...")
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
print(f"fct_gap_analysis_v4 done in {time.time()-t6:.1f}s, rows: {con.execute('SELECT count(*) FROM fct_gap_analysis_v4').fetchone()[0]}")

con.close()
print(f"Pipeline successfully completed in {time.time()-t0:.1f}s!")
