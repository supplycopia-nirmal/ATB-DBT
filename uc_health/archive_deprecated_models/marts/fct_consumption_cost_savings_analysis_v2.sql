{{ config(materialized='table') }}

with consumption as (
    select *, row_number() over() as consumption_row_id
    from {{ ref('int_consumption_profiled') }}
),
item_master as ( select *, row_number() over() as __im_id from {{ ref('int_item_master_profiled') }} ),
contracts as ( select *, row_number() over() as __con_id from {{ ref('stg_contracts') }} ),

mapped_contracts as (
    select c.consumption_row_id, con.__con_id
    from consumption c
    inner join contracts con on c.item_number = con.item_id
    where (try_cast(c.admit_date_time as date) >= try_cast(con.contract_start_date as date) and try_cast(c.admit_date_time as date) <= coalesce(try_cast(con.contract_end_date as date), '2099-12-31')) AND c.item_uom = con.contract_uom
    QUALIFY row_number() over (
        partition by c.consumption_row_id
        order by abs(date_diff('day', try_cast(con.contract_start_date as date), try_cast(c.admit_date_time as date))) asc
    ) = 1
),

mapped_item_master as (
    select c.consumption_row_id, im.__im_id
    from consumption c
    inner join item_master im on c.item_number = im.item_id
    where (try_cast(c.admit_date_time as date) >= try_cast(im.contract_start_date as date) and try_cast(c.admit_date_time as date) <= coalesce(try_cast(im.contract_end_date as date), '2099-12-31')) AND c.item_uom = im.contract_uom
    QUALIFY row_number() over (
        partition by c.consumption_row_id
        order by abs(date_diff('day', try_cast(im.contract_start_date as date), try_cast(c.admit_date_time as date))) asc
    ) = 1
)

select
    c."SURGICAL_HIERARCHY",
    c."BILLED_CPT_CODE",
    c."PRIMARY_ICD10_PX_CODE",
    c."PAYOR_GROUP",
    c."GMLOS",
    c."IMPLANT_VAR_DIRECT_COST",
    c."MED_SUPPLY_VAR_DIRECT_COST",
    c."TOTAL_ACCT_BAL",
    c."TOTAL_ADJ",
    c."TOTAL_PMTS",
    c."SSI (0/1)",
    c."BLOOD_TRANSFUSION_FLAG (0/1)",
    c."READMISSION_INDEX_CASE (0/1)",
    c."MORTALITY (0/1)",
    c."RISK OF MORTALITY",
    c."ITEM_QOE",
    c."ASA_RATING",
    c."BMI_BUCKET",
    c."ROBOTICS (0/1)",
    c."SMOKING_STATUS",
    c."DIABETIC_STATUS (0/1)",
    c."PATIENT_AGE_BUCKET",
    c."PATIENT_GENDER",
    c."ETHNICITY",
    c."log_id",
    c."facility",
    c."medical_record_number",
    c."case_id",
    c."drg_code",
    c."primary_procedure",
    c."service_line",
    c."patient_type",
    c."lead_surgeon",
    c."admit_date_time",
    c."discharge_date_time",
    c."length_of_stay",
    c."account_number",
    c."total_acquisition_cost",
    c."total_quantity",
    c."total_charges",
    c."manufacturer_name",
    c."manufacturer_catalog_number",
    c."item_number",
    c."item_description",
    c."supplier",
    c."contract_category",
    c."spend_category",
    c."unspsc_code",
    c."contract_flag",
    c."primary_drg_code",
    c."primary_procedure_group",
    
    -- ITEM MASTER
    im."packaging_string" as im_packaging_string,
    im."manufacture_id" as im_manufacture_id,
    im."brand_name" as im_brand_name,
    im."latex" as im_latex,
    im."ndc" as im_ndc,
    im."contract_qoe" as im_contract_qoe,
    im."hcpcs" as im_hcpcs,
    im."item_description" as im_item_description,
    im."mfr_part_number" as im_mfr_part_number,
    im."mfr_name" as im_mfr_name,
    im."vendor_name" as im_vendor_name,
    im."vendor_part_number" as im_vendor_part_number,
    im."vendor_code" as im_vendor_code,
    im."contract_number" as im_contract_number,
    im."contract_description" as im_contract_description,
    im."contract_start_date" as im_contract_start_date,
    im."contract_end_date" as im_contract_end_date,
    im."unspsc_code" as im_unspsc_code,
    im."unspsc_description" as im_unspsc_description,
    im."is_active" as im_is_active,
    im."is_missing_vendor_code" as im_is_missing_vendor_code,
    im."is_missing_mfr_name" as im_is_missing_mfr_name,
    im."custom_category" as im_custom_category,
    im."data_quality_score" as im_data_quality_score,
    -- CONTRACTS
    con."contract_number" as con_contract_number,
    con."contract_description" as con_contract_description,
    con."contract_start_date" as con_contract_start_date,
    con."contract_end_date" as con_contract_end_date,
    con."contract_qoe" as con_contract_qoe,
    con."item_description" as con_item_description,
    con."manufacturer_part_number" as con_manufacturer_part_number,
    con."manufacture_name" as con_manufacture_name,
    con."vendor_name" as con_vendor_name,
    con."contract_category" as con_contract_category,
    con."list_price" as con_list_price,
    con."pricing_tier" as con_pricing_tier,
    con."tier_requirements" as con_tier_requirements,

    -- ---------------------------------------------------------
    -- UOM AND PRICE COLUMNS FOR SIDE-BY-SIDE COMPARISON
    -- ---------------------------------------------------------
    c.item_uom as consumption_uom,
    c.supply_unit_price as consumption_unit_price,
    c.contract_price as consumption_contract_price,

    im.contract_uom as im_contract_uom,
    im.contract_price as im_contract_price,
    im.im_unit_contract_price as im_im_unit_contract_price,

    con.contract_uom as con_contract_uom,
    con.contract_price as con_contract_price,
    con.contract_ea_price as con_contract_ea_price,

    -- Computed Cost Savings Metrics
    ROUND(
        case
            when try_cast(con.contract_price as double) > 0 then
                (try_cast(c.supply_unit_price as double) - coalesce(try_cast(case when c.item_uom = con.contract_uom then con.contract_price else con.contract_ea_price end as double), 0)) * try_cast(c.total_quantity as double)
            when try_cast(im.contract_price as double) > 0 then
                (try_cast(c.supply_unit_price as double) - coalesce(try_cast(case when c.item_uom = im.contract_uom then im.contract_price else im.im_unit_contract_price end as double), 0)) * try_cast(c.total_quantity as double)
            else 0
        end,
    2) as price_variance,

    case
        when c.contract_flag = 'OFF CONTRACT' then true
        else false
    end as is_off_contract

from consumption c
left join mapped_contracts m_con on c.consumption_row_id = m_con.consumption_row_id
left join contracts con on m_con.__con_id = con.__con_id
left join mapped_item_master m_im on c.consumption_row_id = m_im.consumption_row_id
left join item_master im on m_im.__im_id = im.__im_id
