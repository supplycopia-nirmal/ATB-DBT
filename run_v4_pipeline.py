import sys, time
sys.path.insert(0, '/Users/piyu/Library/Python/3.9/lib/python/site-packages')
import duckdb

t0 = time.time()
con = duckdb.connect('uc_health/uc_health.duckdb')
con.execute("PRAGMA threads=4;")

print("0a. Ensuring seed views and cleaned staging views...")

# Create product_class_master_1 view
con.execute("""
CREATE OR REPLACE VIEW product_class_master_1 AS
SELECT * FROM read_csv('uc_health/seeds/product_class_master_1.csv', header=True, all_varchar=True);
""")

# Rebuild stg_item_master_v4 with whitespace cleaning and seed enrichment
con.execute("""
CREATE OR REPLACE VIEW stg_item_master_v4 AS
with raw_source as (
    select * from read_csv('Data/UHC_IM_20260930010918.csv', delim='|', header=True, all_varchar=True, null_padding=True, ignore_errors=True)
),
cleaned as (
    select
        case when item_id is null or trim(item_id) in ('', 'NULL', 'N/A') then null else regexp_replace(trim(item_id), '\\s+', ' ', 'g') end as item_id,
        case when item_description is null or trim(item_description) in ('', 'NULL', 'N/A') then null else regexp_replace(trim(item_description), '\\s+', ' ', 'g') end as item_description,
        case when manufacturer_part_number is null or trim(manufacturer_part_number) in ('', 'NULL', 'N/A') then null else regexp_replace(trim(manufacturer_part_number), '\\s+', ' ', 'g') end as mfr_part_number,
        case when manufacture_name is null or trim(manufacture_name) in ('', 'NULL', 'N/A') then null else regexp_replace(trim(manufacture_name), '\\s+', ' ', 'g') end as mfr_name,
        case when vendor_name is null or trim(vendor_name) in ('', 'NULL', 'N/A') then null else upper(regexp_replace(trim(vendor_name), '\\s+', ' ', 'g')) end as vendor_name,
        case when vendor_part_number is null or trim(vendor_part_number) in ('', 'NULL', 'N/A') then null else regexp_replace(trim(vendor_part_number), '\\s+', ' ', 'g') end as vendor_part_number,
        case when vendor_code is null or trim(vendor_code) in ('', 'NULL', 'N/A') then null else regexp_replace(trim(vendor_code), '\\s+', ' ', 'g') end as vendor_code,
        case when contract_number is null or trim(contract_number) in ('', 'NULL', 'N/A') then null else regexp_replace(trim(contract_number), '\\s+', ' ', 'g') end as contract_number,
        case when contract_description is null or trim(contract_description) in ('', 'NULL', 'N/A') then null else regexp_replace(trim(contract_description), '\\s+', ' ', 'g') end as contract_description,
        try_cast(contract_start as timestamp) as contract_start_date,
        try_cast(contract_end as timestamp) as contract_end_date,
        try_cast(contract_price as double) as contract_price,
        try_cast(contract_qoe as integer) as contract_qoe,
        case when unspsc is null or trim(unspsc) in ('', 'NULL', 'N/A') then null else regexp_replace(trim(unspsc), '\\s+', ' ', 'g') end as unspsc_code,
        case when unspsc_description is null or trim(unspsc_description) in ('', 'NULL', 'N/A') then null else regexp_replace(trim(unspsc_description), '\\s+', ' ', 'g') end as unspsc_description,
        case when upper(trim(is_active)) in ('ACTIVE', 'Y', 'TRUE', '1') then true else false end as is_active,
        row_number() over (
            partition by trim(item_id)
            order by case when upper(trim(is_active)) in ('ACTIVE', 'Y', 'TRUE', '1') then 1 else 0 end desc,
                     try_cast(contract_start as timestamp) desc nulls last
        ) as _dedup_rn
    from raw_source
    where trim(item_id) is not null
),
deduped as (
    select
        item_id, item_description, mfr_part_number, mfr_name, vendor_name, vendor_part_number,
        vendor_code, contract_number, contract_description, contract_start_date, contract_end_date,
        contract_price, contract_qoe, unspsc_code, unspsc_description, is_active,
        'UHC_IM_20260930010918.csv' as _source_file,
        row_number() over () as _source_row_number,
        md5(concat(coalesce(cast(item_id as varchar), ''), coalesce(cast(mfr_part_number as varchar), ''))) as _row_hash,
        current_timestamp as _ingested_at
    from cleaned
    where _dedup_rn = 1
)
select * from deduped;
""")

# Rebuild stg_contracts_v4 with whitespace cleaning
con.execute("""
CREATE OR REPLACE VIEW stg_contracts_v4 AS
with raw_source as (
    select * from read_csv('Data/UHC_CON_20260930010958.csv', delim='|', header=True, all_varchar=True, null_padding=True, ignore_errors=True, quote='\"', strict_mode=False)
),
cleaned as (
    select
        case when contract_number is null or trim(contract_number) in ('', 'NULL', 'N/A') then null else regexp_replace(trim(contract_number), '\\s+', ' ', 'g') end as contract_number,
        case when contract_description is null or trim(contract_description) in ('', 'NULL', 'N/A') then null else regexp_replace(trim(contract_description), '\\s+', ' ', 'g') end as contract_description,
        try_cast(contract_start as timestamp) as contract_start_date,
        try_cast(contract_end as timestamp) as contract_end_date,
        case when contract_uom is null or trim(contract_uom) in ('', 'NULL', 'N/A') then null else upper(regexp_replace(trim(contract_uom), '\\s+', ' ', 'g')) end as contract_uom,
        try_cast(contract_qoe as integer) as contract_qoe,
        try_cast(contract_price as double) as contract_price,
        try_cast(contract_ea_price as double) as contract_ea_price,
        case when item_id is null or trim(item_id) in ('', 'NULL', 'N/A') then null else regexp_replace(trim(item_id), '\\s+', ' ', 'g') end as item_id,
        case when item_description is null or trim(item_description) in ('', 'NULL', 'N/A') then null else regexp_replace(trim(item_description), '\\s+', ' ', 'g') end as item_description,
        case when manufacturer_part_number is null or trim(manufacturer_part_number) in ('', 'NULL', 'N/A') then null else regexp_replace(trim(manufacturer_part_number), '\\s+', ' ', 'g') end as manufacturer_part_number,
        case when manufacture_name is null or trim(manufacture_name) in ('', 'NULL', 'N/A') then null else regexp_replace(trim(manufacture_name), '\\s+', ' ', 'g') end as manufacture_name,
        case when vendor_name is null or trim(vendor_name) in ('', 'NULL', 'N/A') then null else upper(regexp_replace(trim(vendor_name), '\\s+', ' ', 'g')) end as vendor_name,
        case when contract_category is null or trim(contract_category) in ('', 'NULL', 'N/A') then null else regexp_replace(trim(contract_category), '\\s+', ' ', 'g') end as contract_category,
        try_cast(list_price as double) as list_price,
        case when pricing_tier is null or trim(pricing_tier) in ('', 'NULL', 'N/A') then null else regexp_replace(trim(pricing_tier), '\\s+', ' ', 'g') end as pricing_tier,
        case when tier_requirements is null or trim(tier_requirements) in ('', 'NULL', 'N/A') then null else regexp_replace(trim(tier_requirements), '\\s+', ' ', 'g') end as tier_requirements,
        'UHC_CON_20260930010958.csv' as _source_file,
        row_number() over () as _source_row_number,
        md5(concat(coalesce(cast(contract_number as varchar), ''), coalesce(cast(item_id as varchar), ''))) as _row_hash,
        current_timestamp as _ingested_at
    from raw_source
    where trim(contract_number) is not null
      and trim(item_id) is not null
      and try_cast(contract_price as double) is not null
)
select * from cleaned
where contract_end_date is null or contract_end_date >= contract_start_date;
""")

# Rebuild int_item_master_enriched_v4 with seed enrichment
con.execute("""
CREATE OR REPLACE TABLE int_item_master_enriched_v4 AS
with item_master as (
    select * from stg_item_master_v4
),
pcm as (
    select * from product_class_master_1
),
classified as (
    select
        im.*,
        pcm.final_class as pcm_final_class,
        pcm.final_subclass as pcm_final_subclass,
        pcm.unspsc_description as pcm_unspsc_description,
        case 
            when im.contract_qoe is not null and im.contract_qoe > 0 then im.contract_price / im.contract_qoe
            else im.contract_price
        end as im_unit_contract_price,
        case when im.vendor_code is null or trim(im.vendor_code) in ('', 'N/A', 'NA', 'NULL') then true else false end as is_missing_vendor_code,
        case when im.mfr_name is null or trim(im.mfr_name) in ('', 'N/A', 'NA', 'NULL') then true else false end as is_missing_mfr_name,
        coalesce(
            pcm.final_class,
            case
                when upper(coalesce(im.item_description, '')) like '%IMPLANT%'
                  or upper(coalesce(im.item_description, '')) like '%SCREW%'
                  or upper(coalesce(im.item_description, '')) like '%PLATE%'
                  or upper(coalesce(im.item_description, '')) like '%SPINE%'
                  or upper(coalesce(im.item_description, '')) like '%BONE%' then 'Orthopedic / Implants'
                when upper(coalesce(im.item_description, '')) like '%STENT%'
                  or upper(coalesce(im.item_description, '')) like '%PACEMAKER%'
                  or upper(coalesce(im.item_description, '')) like '%BALLOON%'
                  or upper(coalesce(im.item_description, '')) like '%CATH%' then 'Cardiology'
                when upper(coalesce(im.item_description, '')) like '%GLOVE%'
                  or upper(coalesce(im.item_description, '')) like '%MASK%'
                  or upper(coalesce(im.item_description, '')) like '%GOWN%'
                  or upper(coalesce(im.item_description, '')) like '%PPE%' then 'PPE / Apparel'
                when upper(coalesce(im.item_description, '')) like '%SUTURE%'
                  or upper(coalesce(im.item_description, '')) like '%STAPLE%'
                  or upper(coalesce(im.item_description, '')) like '%BLADE%' then 'Surgical Supplies'
                when upper(coalesce(im.item_description, '')) like '%DRESSING%'
                  or upper(coalesce(im.item_description, '')) like '%GAUZE%'
                  or upper(coalesce(im.item_description, '')) like '%WOUND%' then 'Wound Care'
                when upper(coalesce(im.item_description, '')) like '%SYRINGE%'
                  or upper(coalesce(im.item_description, '')) like '%NEEDLE%'
                  or upper(coalesce(im.item_description, '')) like '%IV%' then 'IV & Injection'
                when im.unspsc_description is not null and trim(im.unspsc_description) != '' then trim(im.unspsc_description)
                else 'General Medical (Unclassified)'
            end
        ) as custom_category
    from item_master im
    left join pcm on im.unspsc_code = pcm.unspsc_code
),
scored as (
    select
        c.*,
        coalesce(c.pcm_final_subclass, 'Unclassified') as product_subclass,
        coalesce(c.pcm_unspsc_description, c.unspsc_description) as final_unspsc_description,
        (
            100 
            - (case when c.is_missing_vendor_code then 20 else 0 end)
            - (case when c.is_missing_mfr_name then 20 else 0 end)
            - (case when c.custom_category = 'General Medical (Unclassified)' then 10 else 0 end)
        ) as data_quality_score
    from classified c
)
select * from scored;
""")

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
        case when LOG_ID is null or trim(LOG_ID) in ('', 'NULL') then null else regexp_replace(trim(LOG_ID), '\\s+', ' ', 'g') end as log_id,
        case when FACILITY is null or trim(FACILITY) in ('', 'NULL') then null else regexp_replace(trim(FACILITY), '\\s+', ' ', 'g') end as facility,
        case when MEDICAL_RECORD_NUMBER is null or trim(MEDICAL_RECORD_NUMBER) in ('', 'NULL') then null else regexp_replace(trim(MEDICAL_RECORD_NUMBER), '\\s+', ' ', 'g') end as medical_record_number,
        case when "CASE_ID / ENCOUNTER FHIR ID" is null or trim("CASE_ID / ENCOUNTER FHIR ID") in ('', 'NULL') then null else regexp_replace(trim("CASE_ID / ENCOUNTER FHIR ID"), '\\s+', ' ', 'g') end as case_id,
        case when "CASE_ID / ENCOUNTER FHIR ID" is null or trim("CASE_ID / ENCOUNTER FHIR ID") in ('', 'NULL') then null else regexp_replace(trim("CASE_ID / ENCOUNTER FHIR ID"), '\\s+', ' ', 'g') end as "CASE_ID / ENCOUNTER FHIR ID",
        case when DRG_CODE is null or trim(DRG_CODE) in ('', 'NULL') then null else regexp_replace(trim(DRG_CODE), '\\s+', ' ', 'g') end as drg_code,
        case when SURGICAL_HIERARCHY is null or trim(SURGICAL_HIERARCHY) in ('', 'NULL') then null else regexp_replace(trim(SURGICAL_HIERARCHY), '\\s+', ' ', 'g') end as surgical_hierarchy,
        case when SURGICAL_HIERARCHY is null or trim(SURGICAL_HIERARCHY) in ('', 'NULL') then null else regexp_replace(trim(SURGICAL_HIERARCHY), '\\s+', ' ', 'g') end as "SURGICAL_HIERARCHY",
        case when BILLED_CPT_CODE is null or trim(BILLED_CPT_CODE) in ('', 'NULL') then null else regexp_replace(trim(BILLED_CPT_CODE), '\\s+', ' ', 'g') end as billed_cpt_code,
        case when BILLED_CPT_CODE is null or trim(BILLED_CPT_CODE) in ('', 'NULL') then null else regexp_replace(trim(BILLED_CPT_CODE), '\\s+', ' ', 'g') end as "BILLED_CPT_CODE",
        case when PRIMARY_ICD10_PX_CODE is null or trim(PRIMARY_ICD10_PX_CODE) in ('', 'NULL') then null else regexp_replace(trim(PRIMARY_ICD10_PX_CODE), '\\s+', ' ', 'g') end as primary_icd10_px_code,
        case when PRIMARY_ICD10_PX_CODE is null or trim(PRIMARY_ICD10_PX_CODE) in ('', 'NULL') then null else regexp_replace(trim(PRIMARY_ICD10_PX_CODE), '\\s+', ' ', 'g') end as "PRIMARY_ICD10_PX_CODE",
        case when PRIMARY_PROCEDURE is null or trim(PRIMARY_PROCEDURE) in ('', 'NULL') then null else regexp_replace(trim(PRIMARY_PROCEDURE), '\\s+', ' ', 'g') end as primary_procedure,
        case when SERVICE_LINE is null or trim(SERVICE_LINE) in ('', 'NULL') then null else regexp_replace(trim(SERVICE_LINE), '\\s+', ' ', 'g') end as service_line,
        case when PATIENT_TYPE is null or trim(PATIENT_TYPE) in ('', 'NULL') then null else regexp_replace(trim(PATIENT_TYPE), '\\s+', ' ', 'g') end as patient_type,
        case when LEAD_SURGEON is null or trim(LEAD_SURGEON) in ('', 'NULL') then null else regexp_replace(trim(LEAD_SURGEON), '\\s+', ' ', 'g') end as lead_surgeon,
        case when PAYOR_GROUP is null or trim(PAYOR_GROUP) in ('', 'NULL') then null else regexp_replace(trim(PAYOR_GROUP), '\\s+', ' ', 'g') end as payor_group,
        case when PAYOR_GROUP is null or trim(PAYOR_GROUP) in ('', 'NULL') then null else regexp_replace(trim(PAYOR_GROUP), '\\s+', ' ', 'g') end as "PAYOR_GROUP",
        try_cast(ADMIT_DATE_TIME as timestamp) as admit_date_time,
        try_cast(DISCHARGE_DATE_TIME as timestamp) as discharge_date_time,
        try_cast(LOS as double) as length_of_stay,
        try_cast(LOS as double) as "LOS",
        try_cast(GMLOS as double) as gmlos,
        try_cast(GMLOS as double) as "GMLOS",
        case when ACCOUNT_NUMBER is null or trim(ACCOUNT_NUMBER) in ('', 'NULL') then null else regexp_replace(trim(ACCOUNT_NUMBER), '\\s+', ' ', 'g') end as account_number,
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
        case when "SSI (0/1)" is null or trim("SSI (0/1)") in ('', 'NULL') then null else regexp_replace(trim("SSI (0/1)"), '\\s+', ' ', 'g') end as ssi_flag,
        case when "SSI (0/1)" is null or trim("SSI (0/1)") in ('', 'NULL') then null else regexp_replace(trim("SSI (0/1)"), '\\s+', ' ', 'g') end as "SSI (0/1)",
        case when "BLOOD_TRANSFUSION_FLAG (0/1)" is null or trim("BLOOD_TRANSFUSION_FLAG (0/1)") in ('', 'NULL') then null else regexp_replace(trim("BLOOD_TRANSFUSION_FLAG (0/1)"), '\\s+', ' ', 'g') end as blood_transfusion_flag,
        case when "BLOOD_TRANSFUSION_FLAG (0/1)" is null or trim("BLOOD_TRANSFUSION_FLAG (0/1)") in ('', 'NULL') then null else regexp_replace(trim("BLOOD_TRANSFUSION_FLAG (0/1)"), '\\s+', ' ', 'g') end as "BLOOD_TRANSFUSION_FLAG (0/1)",
        case when "READMISSION_INDEX_CASE (0/1)" is null or trim("READMISSION_INDEX_CASE (0/1)") in ('', 'NULL') then null else regexp_replace(trim("READMISSION_INDEX_CASE (0/1)"), '\\s+', ' ', 'g') end as readmission_index_case,
        case when "READMISSION_INDEX_CASE (0/1)" is null or trim("READMISSION_INDEX_CASE (0/1)") in ('', 'NULL') then null else regexp_replace(trim("READMISSION_INDEX_CASE (0/1)"), '\\s+', ' ', 'g') end as "READMISSION_INDEX_CASE (0/1)",
        case when "MORTALITY (0/1)" is null or trim("MORTALITY (0/1)") in ('', 'NULL') then null else regexp_replace(trim("MORTALITY (0/1)"), '\\s+', ' ', 'g') end as mortality_flag,
        case when "MORTALITY (0/1)" is null or trim("MORTALITY (0/1)") in ('', 'NULL') then null else regexp_replace(trim("MORTALITY (0/1)"), '\\s+', ' ', 'g') end as "MORTALITY (0/1)",
        case when "RISK OF MORTALITY" is null or trim("RISK OF MORTALITY") in ('', 'NULL') then null else regexp_replace(trim("RISK OF MORTALITY"), '\\s+', ' ', 'g') end as risk_of_mortality,
        case when "RISK OF MORTALITY" is null or trim("RISK OF MORTALITY") in ('', 'NULL') then null else regexp_replace(trim("RISK OF MORTALITY"), '\\s+', ' ', 'g') end as "RISK OF MORTALITY",
        case when MANUFACTURER_NAME is null or trim(MANUFACTURER_NAME) in ('', 'NULL') then null else regexp_replace(trim(MANUFACTURER_NAME), '\\s+', ' ', 'g') end as manufacturer_name,
        case when MANUFACTURER_CATALOG_NUMBER is null or trim(MANUFACTURER_CATALOG_NUMBER) in ('', 'NULL') then null else regexp_replace(trim(MANUFACTURER_CATALOG_NUMBER), '\\s+', ' ', 'g') end as manufacturer_catalog_number,
        case when ITEM_NUMBER is null or trim(ITEM_NUMBER) in ('', 'NULL') then null else regexp_replace(trim(ITEM_NUMBER), '\\s+', ' ', 'g') end as item_number,
        case when ITEM_DESCRIPTION is null or trim(ITEM_DESCRIPTION) in ('', 'NULL') then null else regexp_replace(trim(ITEM_DESCRIPTION), '\\s+', ' ', 'g') end as item_description,
        upper(trim(coalesce(ITEM_UOM, 'EA'))) as raw_item_uom,
        try_cast(ITEM_QOE as double) as item_qoe,
        try_cast(ITEM_QOE as double) as "ITEM_QOE",
        case when SUPPLIER is null or trim(SUPPLIER) in ('', 'NULL') then null else upper(regexp_replace(trim(SUPPLIER), '\\s+', ' ', 'g')) end as supplier,
        case when CONTRACT_CATEGORY is null or trim(CONTRACT_CATEGORY) in ('', 'NULL') then null else regexp_replace(trim(CONTRACT_CATEGORY), '\\s+', ' ', 'g') end as contract_category,
        case when SPEND_CATEGORY is null or trim(SPEND_CATEGORY) in ('', 'NULL') then null else regexp_replace(trim(SPEND_CATEGORY), '\\s+', ' ', 'g') end as spend_category,
        case when UNSPSC_CODE is null or trim(UNSPSC_CODE) in ('', 'NULL') then null else upper(regexp_replace(trim(UNSPSC_CODE), '\\s+', ' ', 'g')) end as unspsc_code,
        case when CONTRACT_FLAG is null or trim(CONTRACT_FLAG) in ('', 'NULL') then null else regexp_replace(trim(CONTRACT_FLAG), '\\s+', ' ', 'g') end as contract_flag,
        case when ASA_RATING is null or trim(ASA_RATING) in ('', 'NULL') then null else regexp_replace(trim(ASA_RATING), '\\s+', ' ', 'g') end as asa_rating,
        case when ASA_RATING is null or trim(ASA_RATING) in ('', 'NULL') then null else regexp_replace(trim(ASA_RATING), '\\s+', ' ', 'g') end as "ASA_RATING",
        case when BMI_BUCKET is null or trim(BMI_BUCKET) in ('', 'NULL') then null else regexp_replace(trim(BMI_BUCKET), '\\s+', ' ', 'g') end as bmi_bucket,
        case when BMI_BUCKET is null or trim(BMI_BUCKET) in ('', 'NULL') then null else regexp_replace(trim(BMI_BUCKET), '\\s+', ' ', 'g') end as "BMI_BUCKET",
        case when "ROBOTICS (0/1)" is null or trim("ROBOTICS (0/1)") in ('', 'NULL') then null else regexp_replace(trim("ROBOTICS (0/1)"), '\\s+', ' ', 'g') end as robotics_flag,
        case when "ROBOTICS (0/1)" is null or trim("ROBOTICS (0/1)") in ('', 'NULL') then null else regexp_replace(trim("ROBOTICS (0/1)"), '\\s+', ' ', 'g') end as "ROBOTICS (0/1)",
        case when SMOKING_STATUS is null or trim(SMOKING_STATUS) in ('', 'NULL') then null else regexp_replace(trim(SMOKING_STATUS), '\\s+', ' ', 'g') end as smoking_status,
        case when SMOKING_STATUS is null or trim(SMOKING_STATUS) in ('', 'NULL') then null else regexp_replace(trim(SMOKING_STATUS), '\\s+', ' ', 'g') end as "SMOKING_STATUS",
        case when "DIABETIC_STATUS (0/1)" is null or trim("DIABETIC_STATUS (0/1)") in ('', 'NULL') then null else regexp_replace(trim("DIABETIC_STATUS (0/1)"), '\\s+', ' ', 'g') end as diabetic_status,
        case when "DIABETIC_STATUS (0/1)" is null or trim("DIABETIC_STATUS (0/1)") in ('', 'NULL') then null else regexp_replace(trim("DIABETIC_STATUS (0/1)"), '\\s+', ' ', 'g') end as "DIABETIC_STATUS (0/1)",
        case when PATIENT_AGE_BUCKET is null or trim(PATIENT_AGE_BUCKET) in ('', 'NULL') then null else regexp_replace(trim(PATIENT_AGE_BUCKET), '\\s+', ' ', 'g') end as patient_age_bucket,
        case when PATIENT_AGE_BUCKET is null or trim(PATIENT_AGE_BUCKET) in ('', 'NULL') then null else regexp_replace(trim(PATIENT_AGE_BUCKET), '\\s+', ' ', 'g') end as "PATIENT_AGE_BUCKET",
        case when PATIENT_GENDER is null or trim(PATIENT_GENDER) in ('', 'NULL') then null else regexp_replace(trim(PATIENT_GENDER), '\\s+', ' ', 'g') end as patient_gender,
        case when PATIENT_GENDER is null or trim(PATIENT_GENDER) in ('', 'NULL') then null else regexp_replace(trim(PATIENT_GENDER), '\\s+', ' ', 'g') end as "PATIENT_GENDER",
        case when ETHNICITY is null or trim(ETHNICITY) in ('', 'NULL') then null else regexp_replace(trim(ETHNICITY), '\\s+', ' ', 'g') end as ethnicity,
        case when ETHNICITY is null or trim(ETHNICITY) in ('', 'NULL') then null else regexp_replace(trim(ETHNICITY), '\\s+', ' ', 'g') end as "ETHNICITY"
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

print("1. Rebuilding int_item_matching_v4 keyed on row_id with categorization enrichment...")
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
    im_ref.unspsc_code as im_unspsc,
    im_ref.product_subclass,
    im_ref.final_unspsc_description as unspsc_description,
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
        im.im_unspsc,
        im.product_subclass,
        im.unspsc_description,
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
        con.contract_start_date,
        con.contract_end_date,
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
),
cortex_map as (
    select distinct
        raw_drg_code,
        raw_procedure,
        standardized_procedure,
        primary_drg_code,
        procedure_group
    from read_parquet('output/drg_procedure_mapping_v4.parquet')
)
select
    d.drg_code,
    d.primary_procedure,
    coalesce(m.standardized_procedure, d.primary_procedure) as standardized_procedure,
    coalesce(m.primary_drg_code, split_part(coalesce(d.drg_code, ''), ',', 1)) as primary_drg_code,
    coalesce(m.procedure_group, 'General Surgery') as primary_procedure_group,
    'gemini-3.5-flash-cortex' as llm_model_used,
    'v4.1' as llm_prompt_version,
    current_timestamp as llm_generated_at
from distinct_drg d
left join cortex_map m 
    on coalesce(d.drg_code, '') = coalesce(m.raw_drg_code, '')
    and coalesce(d.primary_procedure, '') = coalesce(m.raw_procedure, '');
""")
print(f"int_drg_mapping_v4 done in {time.time()-t3:.1f}s, rows: {con.execute('SELECT count(*) FROM int_drg_mapping_v4').fetchone()[0]}")

t4 = time.time()
print("5. Building fct_consumption_cost_savings_v4 with 17 dashboard parity columns...")
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
        d.standardized_procedure,
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
        end as is_contract_compliant,

        -- 17 Explicit Dashboard Parity Columns
        e.contract_start_date as contract_start,
        e.contract_end_date as contract_end,
        case when e.item_uom = e.contract_uom then 'Y' else 'N' end as contract_uom_matches_po_uom,
        case when e.is_contract_matched then 'On contract' else 'Off contract' end as contract_status,
        case when e.is_contract_matched then 'Y' else 'N' end as has_current_contract,
        current_date as current_contract_as_of,
        e.contract_number as current_contract_number,
        e.contract_price as current_contract_price,
        e.contract_uom as current_contract_uom,
        e.contract_start_date as current_contract_start,
        e.contract_end_date as current_contract_end,
        case when e.item_uom = e.contract_uom then 'Y' else 'N' end as current_contract_uom_matches_po_uom
    from enriched e
)
select * from calculated;
""")
print(f"fct_consumption_cost_savings_v4 done in {time.time()-t4:.1f}s, rows: {con.execute('SELECT count(*) FROM fct_consumption_cost_savings_v4').fetchone()[0]}")

t5 = time.time()
print("6. Building fct_po_cost_savings_v4 with 17 dashboard parity columns...")

# Rebuild stg_po_v4 view in DuckDB
con.execute("""
CREATE OR REPLACE VIEW stg_po_v4 AS
with raw_source as (
    select * from read_csv('Data/UHC_PO_20260930010955.csv', delim='|', header=True, all_varchar=True, null_padding=True, ignore_errors=True)
),
cleaned as (
    select
        case when po_number is null or trim(po_number) in ('', 'NULL') then null else regexp_replace(trim(po_number), '\\s+', ' ', 'g') end as po_number,
        case when po_line_no is null or trim(po_line_no) in ('', 'NULL') then null else regexp_replace(trim(po_line_no), '\\s+', ' ', 'g') end as po_line_no,
        try_cast(po_date as timestamp) as po_date,
        try_cast(po_last_update_date as timestamp) as po_last_update_date,
        case when facility_entity_code is null or trim(facility_entity_code) in ('', 'NULL') then null else regexp_replace(trim(facility_entity_code), '\\s+', ' ', 'g') end as facility_entity_code,
        case when facility_name is null or trim(facility_name) in ('', 'NULL') then null else regexp_replace(trim(facility_name), '\\s+', ' ', 'g') end as facility_name,
        case when contract_no is null or trim(contract_no) in ('', 'NULL') then null else regexp_replace(trim(contract_no), '\\s+', ' ', 'g') end as contract_number,
        case when uom is null or trim(uom) in ('', 'NULL') then null else upper(regexp_replace(trim(uom), '\\s+', ' ', 'g')) end as uom,
        coalesce(try_cast(uom_conv_factor as double), 1.0) as uom_conv_factor,
        try_cast(quantity as double) as quantity,
        try_cast(unit_price as double) as unit_price,
        try_cast(total_value as double) as total_value,
        case when item_id is null or trim(item_id) in ('', 'NULL') then null else regexp_replace(trim(item_id), '\\s+', ' ', 'g') end as item_id,
        case when item_description is null or trim(item_description) in ('', 'NULL') then null else regexp_replace(trim(item_description), '\\s+', ' ', 'g') end as item_description,
        case when manufacture_ERP_id is null or trim(manufacture_ERP_id) in ('', 'NULL') then null else regexp_replace(trim(manufacture_ERP_id), '\\s+', ' ', 'g') end as mfr_erp_id,
        case when manufacture_name is null or trim(manufacture_name) in ('', 'NULL') then null else regexp_replace(trim(manufacture_name), '\\s+', ' ', 'g') end as mfr_name,
        case when manufacturer_part_number is null or trim(manufacturer_part_number) in ('', 'NULL') then null else regexp_replace(trim(manufacturer_part_number), '\\s+', ' ', 'g') end as mfr_part_number,
        case when vendor_code is null or trim(vendor_code) in ('', 'NULL') then null else regexp_replace(trim(vendor_code), '\\s+', ' ', 'g') end as vendor_code,
        case when vendor_name is null or trim(vendor_name) in ('', 'NULL') then null else upper(regexp_replace(trim(vendor_name), '\\s+', ' ', 'g')) end as vendor_name,
        case when vendor_part_number is null or trim(vendor_part_number) in ('', 'NULL') then null else regexp_replace(trim(vendor_part_number), '\\s+', ' ', 'g') end as vendor_part_number,
        row_number() over (
            partition by trim(po_number), trim(po_line_no)
            order by try_cast(po_last_update_date as timestamp) desc nulls last
        ) as _dedup_rn
    from raw_source
    where trim(po_number) is not null 
      and trim(po_line_no) is not null
),
deduped as (
    select
        po_number, po_line_no, po_date, po_last_update_date, facility_entity_code, facility_name,
        contract_number, uom, uom_conv_factor, quantity, unit_price, total_value, item_id, item_description,
        mfr_erp_id, mfr_name, mfr_part_number, vendor_code, vendor_name, vendor_part_number,
        'UHC_PO_20260930010955.csv' as _source_file,
        row_number() over () as _source_row_number,
        md5(concat(coalesce(cast(po_number as varchar), ''), coalesce(cast(po_line_no as varchar), ''))) as _row_hash,
        current_timestamp as _ingested_at
    from cleaned
    where _dedup_rn = 1
)
select * from deduped;
""")

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
        c.contract_start_date as matched_contract_start_date,
        c.contract_end_date as matched_contract_end_date,
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
        im.product_subclass,
        im.final_unspsc_description as unspsc_description,
        im.unspsc_code as im_unspsc,
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
        end as is_contract_compliant,

        -- 17 Explicit Dashboard Parity Columns
        j.matched_contract_price as contract_price,
        j.matched_contract_start_date as contract_start,
        j.matched_contract_end_date as contract_end,
        j.matched_contract_uom as contract_uom,
        case when j.uom = j.matched_contract_uom then 'Y' else 'N' end as contract_uom_matches_po_uom,
        case when j.is_contract_matched then 'On contract' else 'Off contract' end as contract_status,
        case when j.is_contract_matched then 'Y' else 'N' end as has_current_contract,
        current_date as current_contract_as_of,
        j.matched_contract_number as current_contract_number,
        j.matched_contract_price as current_contract_price,
        j.matched_contract_uom as current_contract_uom,
        j.matched_contract_start_date as current_contract_start,
        j.matched_contract_end_date as current_contract_end,
        case when j.uom = j.matched_contract_uom then 'Y' else 'N' end as current_contract_uom_matches_po_uom
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
