with source as (
    select * from {{ source('supplycopia_raw', 'UHC_Consumption') }}
),

renamed as (
    select
        * EXCLUDE (
            LOG_ID,
            FACILITY,
            MEDICAL_RECORD_NUMBER,
            "CASE_ID / ENCOUNTER FHIR ID",
            DRG_CODE,
            PRIMARY_PROCEDURE,
            SERVICE_LINE,
            PATIENT_TYPE,
            LEAD_SURGEON,
            ADMIT_DATE_TIME,
            DISCHARGE_DATE_TIME,
            LOS,
            ACCOUNT_NUMBER,
            CONTRACT_PRICE,
            TOTAL_ACQUISITION_COST,
            SUPPLY_UNIT_PRICE,
            TOTAL_QUANTITY,
            TOTAL_CHARGES,
            MANUFACTURER_NAME,
            MANUFACTURER_CATALOG_NUMBER,
            ITEM_NUMBER,
            ITEM_DESCRIPTION,
            ITEM_UOM,
            SUPPLIER,
            CONTRACT_CATEGORY,
            SPEND_CATEGORY,
            UNSPSC_CODE,
            CONTRACT_FLAG
        ),
        LOG_ID as log_id,
        FACILITY as facility,
        MEDICAL_RECORD_NUMBER as medical_record_number,
        "CASE_ID / ENCOUNTER FHIR ID" as case_id,
        DRG_CODE as drg_code,
        PRIMARY_PROCEDURE as primary_procedure,
        SERVICE_LINE as service_line,
        PATIENT_TYPE as patient_type,
        LEAD_SURGEON as lead_surgeon,
        try_cast(ADMIT_DATE_TIME as timestamp) as admit_date_time,
        try_cast(DISCHARGE_DATE_TIME as timestamp) as discharge_date_time,
        LOS as length_of_stay,
        ACCOUNT_NUMBER as account_number,
        try_cast(CONTRACT_PRICE as double) as contract_price,
        try_cast(TOTAL_ACQUISITION_COST as double) as total_acquisition_cost,
        try_cast(SUPPLY_UNIT_PRICE as double) as supply_unit_price,
        try_cast(TOTAL_QUANTITY as double) as total_quantity,
        try_cast(TOTAL_CHARGES as double) as total_charges,
        MANUFACTURER_NAME as manufacturer_name,
        MANUFACTURER_CATALOG_NUMBER as manufacturer_catalog_number,
        ITEM_NUMBER as item_number,
        ITEM_DESCRIPTION as item_description,
        CASE 
            WHEN UPPER(ITEM_UOM) IN ('EACH', 'EACHES') THEN 'EA'
            WHEN UPPER(ITEM_UOM) = 'BOX' THEN 'BX'
            WHEN UPPER(ITEM_UOM) = 'CASE' THEN 'CA'
            WHEN UPPER(ITEM_UOM) IN ('PACKAGE', 'SHELF PACKAGE', 'PACK') THEN 'PK'
            WHEN UPPER(ITEM_UOM) = 'DOZEN' THEN 'DZ'
            WHEN UPPER(ITEM_UOM) = 'BOTTLE' THEN 'BO'
            WHEN UPPER(ITEM_UOM) = 'PAIR' THEN 'PR'
            WHEN UPPER(ITEM_UOM) = 'SET' THEN 'ST'
            WHEN UPPER(ITEM_UOM) = 'TUBE' THEN 'TB'
            WHEN UPPER(ITEM_UOM) = 'KIT' THEN 'KT'
            WHEN UPPER(ITEM_UOM) = 'BAG' THEN 'BG'
            WHEN UPPER(ITEM_UOM) = 'ROLL' THEN 'RL'
            WHEN UPPER(ITEM_UOM) = 'TRAY' THEN 'TR'
            WHEN UPPER(ITEM_UOM) = 'FOOT' THEN 'FT'
            WHEN UPPER(ITEM_UOM) = 'CENTIMETER' THEN 'CM'
            WHEN UPPER(ITEM_UOM) = 'VIAL' THEN 'VI'
            WHEN UPPER(ITEM_UOM) = 'CAN' THEN 'CN'
            WHEN UPPER(ITEM_UOM) = 'RACK' THEN 'RK'
            WHEN UPPER(ITEM_UOM) = 'UNIT' THEN 'UN'
            ELSE ITEM_UOM 
        END as item_uom,
        SUPPLIER as supplier,
        CONTRACT_CATEGORY as contract_category,
        SPEND_CATEGORY as spend_category,
        UNSPSC_CODE as unspsc_code,
        CONTRACT_FLAG as contract_flag
    from source
)

select * from renamed
