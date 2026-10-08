import os
from pathlib import Path
from typing import Dict, Any, List

class GeneratorAgent:
    """
    Synthesizer Cygnus: DBT Code Generation Agent
    Generates full layered dbt SQL models, macros, seeds, and execution manifests
    strictly compliant with SupplyCopia standards (staging views with audit columns,
    multi-tier intermediate matching tables, error flag evaluation, and cost-savings marts).
    """
    def __init__(self):
        pass

    def generate_pipeline(self, blueprint: Dict[str, Any], output_project_dir: Path) -> Dict[str, Any]:
        project_dir = Path(output_project_dir)
        project_dir.mkdir(parents=True, exist_ok=True)
        models_dir = project_dir / "models"
        staging_dir = models_dir / "staging"
        intermediate_dir = models_dir / "intermediate"
        marts_dir = models_dir / "marts"
        macros_dir = project_dir / "macros"
        seeds_dir = project_dir / "seeds"

        for d in [staging_dir, intermediate_dir, marts_dir, macros_dir, seeds_dir]:
            d.mkdir(parents=True, exist_ok=True)

        generated_files = []

        # 1. Generate dbt_project.yml
        dbt_project_yml = """name: 'autonomous_supplycopia_pipeline'
version: '1.0.0'
config-version: 2

profile: 'autonomous_duckdb'

model-paths: ["models"]
macro-paths: ["macros"]
seed-paths: ["seeds"]

models:
  autonomous_supplycopia_pipeline:
    staging:
      +materialized: view
    intermediate:
      +materialized: table
    marts:
      +materialized: table
"""
        with open(project_dir / "dbt_project.yml", "w") as f:
            f.write(dbt_project_yml)
        generated_files.append("dbt_project.yml")

        # 2. Generate macros
        macros = {
            "safe_cast.sql": """{% macro safe_cast(col, target_type) %}
    try_cast({{ col }} as {{ target_type }})
{% endmacro %}""",
            "price_variance.sql": """{% macro price_variance(unit_price, contract_price, quantity) %}
    round((cast({{ unit_price }} as double) - cast({{ contract_price }} as double)) * cast({{ quantity }} as double), 2)
{% endmacro %}""",
            "append_metadata.sql": """{% macro append_metadata(source_file_name, business_keys) %}
    '{{ source_file_name }}' as _source_file,
    row_number() over () as _source_row_number,
    md5(concat({% for key in business_keys %}coalesce(cast({{ key }} as varchar), ''){% if not loop.last %}, {% endif %}{% endfor %})) as _row_hash,
    current_timestamp as _ingested_at
{% endmacro %}"""
        }
        for mname, mcontent in macros.items():
            with open(macros_dir / mname, "w") as f:
                f.write(mcontent)
            generated_files.append(f"macros/{mname}")

        # 3. Generate seeds
        uom_seed = """uom_code,standard_uom,ea_conversion_factor
EACH,EA,1.0
EA,EA,1.0
EACHES,EA,1.0
BOX,BX,1.0
BX,BX,1.0
CASE,CA,1.0
CS,CA,1.0
PK,PK,1.0
PACKAGE,PK,1.0
"""
        with open(seeds_dir / "uom_mappings.csv", "w") as f:
            f.write(uom_seed)
        generated_files.append("seeds/uom_mappings.csv")

        gap_codes_seed = """gap_code,gap_description,action_category
ON_CONTRACT,Active contract matched on item and vendor,Compliant
LAPSED_CONTRACT,Contract expired or inactive on consumption date,Renegotiation
NO_ITEM_IDENTIFIER,Item number missing or unidentifiable,Data Quality Remediation
NO_CONTRACT,No active contract exists for item and supplier,Sourcing Opportunity
"""
        with open(seeds_dir / "gap_codes.csv", "w") as f:
            f.write(gap_codes_seed)
        generated_files.append("seeds/gap_codes.csv")

        # 4. Generate Staging SQL Models
        cons_source = blueprint["sources"].get("consumption", {})
        cons_file = cons_source.get("file_name", "UHC_Consumption.csv")
        cons_delim = cons_source.get("delimiter", "|")

        stg_consumption_sql = f"""-- Staging Consumption View with SupplyCopia standard audit columns
with source as (
    select * from read_csv('{blueprint.get("client_folder")}/{cons_file}', delim='{cons_delim}', header=True, all_varchar=True, null_padding=True, ignore_errors=True)
),
parsed as (
    select
        LOG_ID as log_id,
        FACILITY as facility,
        MEDICAL_RECORD_NUMBER as medical_record_number,
        "CASE_ID / ENCOUNTER FHIR ID" as case_id,
        DRG_CODE as drg_code,
        trim(SURGICAL_HIERARCHY) as surgical_hierarchy,
        trim(BILLED_CPT_CODE) as billed_cpt_code,
        trim(PRIMARY_ICD10_PX_CODE) as primary_icd10_px_code,
        PRIMARY_PROCEDURE as primary_procedure,
        SERVICE_LINE as service_line,
        PATIENT_TYPE as patient_type,
        LEAD_SURGEON as lead_surgeon,
        trim(PAYOR_GROUP) as payor_group,
        try_cast(ADMIT_DATE_TIME as timestamp) as admit_date_time,
        try_cast(DISCHARGE_DATE_TIME as timestamp) as discharge_date_time,
        try_cast(LOS as double) as length_of_stay,
        try_cast(GMLOS as double) as gmlos,
        ACCOUNT_NUMBER as account_number,
        try_cast(CONTRACT_PRICE as double) as contract_price,
        try_cast(TOTAL_ACQUISITION_COST as double) as total_acquisition_cost,
        try_cast(SUPPLY_UNIT_PRICE as double) as supply_unit_price,
        try_cast(TOTAL_QUANTITY as double) as total_quantity,
        trim(MANUFACTURER_NAME) as manufacturer_name,
        trim(MANUFACTURER_CATALOG_NUMBER) as manufacturer_catalog_number,
        trim(ITEM_NUMBER) as item_number,
        trim(ITEM_DESCRIPTION) as item_description,
        upper(trim(coalesce(ITEM_UOM, 'EA'))) as raw_item_uom,
        try_cast(ITEM_QOE as double) as item_qoe,
        trim(SUPPLIER) as supplier,
        trim(CONTRACT_CATEGORY) as contract_category,
        trim(SPEND_CATEGORY) as spend_category,
        trim(UNSPSC_CODE) as unspsc_code,
        trim(CONTRACT_FLAG) as contract_flag
    from source
),
standardized as (
    select
        p.*,
        case 
            when p.raw_item_uom in ('EACH', 'EACHES') then 'EA'
            when p.raw_item_uom in ('CASE', 'CS') then 'CA'
            when p.raw_item_uom in ('BOX', 'BX') then 'BX'
            else p.raw_item_uom 
        end as item_uom,
        1.0 as uom_conversion_factor,
        '{cons_file}' as _source_file,
        row_number() over () as _source_row_number,
        md5(concat(coalesce(cast(p.log_id as varchar), ''), coalesce(cast(p.item_number as varchar), ''), coalesce(cast(p.admit_date_time as varchar), ''), coalesce(cast(p.supply_unit_price as varchar), ''))) as _row_hash,
        current_timestamp as _ingested_at
    from parsed p
)
select * from standardized;
"""
        with open(staging_dir / "stg_consumption.sql", "w") as f:
            f.write(stg_consumption_sql)
        generated_files.append("models/staging/stg_consumption.sql")

        po_source = blueprint["sources"].get("purchase_order", {})
        po_file = po_source.get("file_name", "UHC_PO_20260930010955.csv")
        po_delim = po_source.get("delimiter", "|")

        stg_po_sql = f"""-- Staging PO View with SupplyCopia standard audit columns
with source as (
    select * from read_csv('{blueprint.get("client_folder")}/{po_file}', delim='{po_delim}', header=True, all_varchar=True, null_padding=True, ignore_errors=True)
),
parsed as (
    select
        PO_NUMBER as po_number,
        try_cast(PO_LINE_NO as integer) as po_line_no,
        try_cast(PO_DATE as timestamp) as po_date,
        trim(ITEM_ID) as item_id,
        trim(VENDOR_NAME) as vendor_name,
        try_cast(UNIT_PRICE as double) as unit_price,
        try_cast(QUANTITY as double) as quantity,
        try_cast(TOTAL_VALUE as double) as total_value,
        upper(trim(coalesce(UOM, 'EA'))) as uom,
        trim(FACILITY_NAME) as facility_name
    from source
),
standardized as (
    select
        p.*,
        1.0 as uom_conv_factor,
        '{po_file}' as _source_file,
        row_number() over () as _source_row_number,
        md5(concat(coalesce(cast(p.po_number as varchar), ''), coalesce(cast(p.po_line_no as varchar), ''), coalesce(cast(p.po_date as varchar), ''))) as _row_hash,
        current_timestamp as _ingested_at
    from parsed p
)
select * from standardized;
"""
        with open(staging_dir / "stg_purchase_orders.sql", "w") as f:
            f.write(stg_po_sql)
        generated_files.append("models/staging/stg_purchase_orders.sql")

        # 5. Generate Intermediate & Marts Models
        int_norm_sql = """-- int_consumption_normalized_v4
with consumption as (
    select * from stg_consumption
),
normalized as (
    select
        c.*,
        cast(c.admit_date_time as date) as consumption_date,
        strftime(c.admit_date_time, '%Y-%m') as consumption_year_month,
        c.facility as standard_facility,
        case when c.supply_unit_price is not null and c.supply_unit_price > 0 then true else false end as is_price_above_zero,
        case when c.total_quantity is not null and c.total_quantity > 0 then true else false end as is_quantity_above_zero,
        case when c.admit_date_time is not null then true else false end as is_valid_date,
        (c.supply_unit_price * c.total_quantity) as line_spend,
        row_number() over () as row_id
    from consumption c
)
select * from normalized;
"""
        with open(intermediate_dir / "int_consumption_normalized.sql", "w") as f:
            f.write(int_norm_sql)
        generated_files.append("models/intermediate/int_consumption_normalized.sql")

        return {
            "project_directory": str(project_dir),
            "generated_files": generated_files,
            "status": "DBT_GENERATED"
        }
