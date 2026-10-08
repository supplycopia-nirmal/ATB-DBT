import os
import json
from pathlib import Path
from autonomous_dbt_app.core.memory_store import MemoryStore

def seed_standards():
    store = MemoryStore()
    print("Seeding SupplyCopia DBT standards and EHR/ERP knowledge into ChromaDB...")

    # 1. SupplyCopia Layered DBT Guidelines
    store.add_standard(
        "std_layered_architecture",
        """SupplyCopia DBT Architecture Standard:
        - Staging Layer (stg_*.sql): Views or ephemeral. Every staging table must append the 4 standard audit columns:
          _source_file (filename or dbt var), _source_row_number (ROW_NUMBER() OVER ()),
          _row_hash (MD5 of concatenated business keys), and _ingested_at (CURRENT_TIMESTAMP).
          Staging handles safe type casting (TRY_CAST), string trimming, and unit of measure (UOM) standardization.
        - Intermediate Layer (int_*.sql): Tables in DuckDB. Multi-tier deterministic matching (Tier 1 exact item_id,
          Tier 2 catalog number, Tier 3 vendor part number, Tier 4 description/fuzzy). Contract matching verifies
          consumption_date between contract_start_date and contract_end_date, applying contract_gap_codes:
          ON_CONTRACT, LAPSED_CONTRACT, NO_ITEM_IDENTIFIER, NO_CONTRACT.
          Row validation generates boolean error flags (;NO_PRICE;ZERO_QTY;INVALID_DATE;NO_ITEM_MASTER;NO_CONTRACT)
          and categorizes row_eligibility into ELIGIBLE, INELIGIBLE, REVIEW_REQUIRED.
        - Marts Layer (fct_*.sql): Tables in DuckDB.
          fct_consumption_cost_savings calculates price_variance2 = (supply_unit_price - contract_price_adjusted) * quantity,
          overpayment_amount = MAX(price_variance2, 0), and savings_opportunity (15% benchmark for off-contract).
          fct_gap_analysis aggregates by primary_procedure_group, service_line, facility, and patient_type.""",
        {"category": "architecture", "standard": "SupplyCopia V4"}
    )

    store.add_standard(
        "std_macros_and_seeds",
        """SupplyCopia Standard Macros and Seeds:
        - macros/safe_cast.sql: TRY_CAST with fallback regex replacement for dirty strings.
        - macros/price_variance.sql: Unit-of-measure adjusted price variance formula with ea_conversion_factor.
        - macros/deduplicate.sql: ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ... DESC) to prevent duplicate key explosion.
        - seeds/uom_mappings.csv: Standardizes EACH/EACHES -> EA, CS/CASE -> CA, BOX/BX -> BX, PK/PACKAGE -> PK.
        - seeds/gap_codes.csv: ON_CONTRACT, LAPSED_CONTRACT, NO_ITEM_IDENTIFIER, NO_CONTRACT, PRICE_VARIANCE_EXCEEDED.""",
        {"category": "macros_seeds", "standard": "SupplyCopia V4"}
    )

    # 2. Heterogeneous EHR / EMR / ERP Dialects & Field Signatures
    ehr_mappings = [
        # Epic Systems
        ("epic_tx_id", "TX_ID", "log_id", "Epic Clarity", "Transaction ID primary key in CLARITY_TDL_TRAN"),
        ("epic_proc_cd", "PROC_CODE", "billed_cpt_code", "Epic Clarity", "Procedure code / CPT"),
        ("epic_csn", "PAT_ENC_CSN_ID", "case_id", "Epic Clarity", "Encounter CSN / Case identifier"),
        ("epic_amount", "AMOUNT", "line_spend", "Epic Clarity", "Transaction monetary spend amount"),
        ("epic_unit_cost", "UNIT_COST", "supply_unit_price", "Epic Clarity", "Unit acquisition price"),
        ("epic_qty", "QUANTITY", "total_quantity", "Epic Clarity", "Consumed item quantity"),
        ("epic_implant", "IMPLANT_ID", "item_number", "Epic Clarity", "Implant item identifier"),
        ("epic_dept", "DEPARTMENT_NAME", "facility", "Epic Clarity", "Hospital department / facility name"),
        
        # Cerner Millennium
        ("cerner_charge_id", "CHARGE_ITEM_ID", "log_id", "Cerner Millennium", "Primary charge transaction ID"),
        ("cerner_cpt", "CPT_HCPCS_CD", "billed_cpt_code", "Cerner Millennium", "Billed CPT / HCPCS procedure code"),
        ("cerner_encntr", "ENCNTR_ID", "case_id", "Cerner Millennium", "Patient encounter identifier"),
        ("cerner_price", "ITEM_PRICE", "supply_unit_price", "Cerner Millennium", "Price per unit"),
        ("cerner_qty", "ITEM_QTY", "total_quantity", "Cerner Millennium", "Count of units charged"),
        ("cerner_synonym", "SYNONYM_ID", "manufacturer_catalog_number", "Cerner Millennium", "Item synonym / catalog code"),
        ("cerner_loc", "LOCATION_CD", "facility", "Cerner Millennium", "Service location code"),

        # Lawson / Infor ERP
        ("lawson_po", "PO_CODE", "po_number", "Lawson ERP", "Purchase order document number"),
        ("lawson_line", "LINE_NBR", "po_line_no", "Lawson ERP", "PO line item number"),
        ("lawson_item", "ITEM", "item_number", "Lawson ERP", "Item master reference"),
        ("lawson_ven_item", "VEN_ITEM", "manufacturer_catalog_number", "Lawson ERP", "Vendor / Manufacturer catalog part number"),
        ("lawson_cost", "ENT_UNIT_COST", "unit_price", "Lawson ERP", "PO purchase unit price"),
        ("lawson_qty", "ENT_BUY_QTY", "quantity", "Lawson ERP", "Purchased quantity"),
        ("lawson_uom", "UOM_CODE", "uom", "Lawson ERP", "Unit of measure"),
        ("lawson_vendor", "VENDOR_NAME", "vendor_name", "Lawson ERP", "Supplier or vendor corporate name"),

        # Workday SCM
        ("workday_po_ref", "PO_Line_Reference", "po_line_no", "Workday SCM", "PO line reference identifier"),
        ("workday_mfr_no", "Supplier_Part_Number", "manufacturer_catalog_number", "Workday SCM", "Supplier catalog or part number"),
        ("workday_ext_amt", "Extended_Amount", "total_value", "Workday SCM", "Total line purchase value"),
        ("workday_item_id", "Item_Identifier", "item_id", "Workday SCM", "Internal item identifier"),
        ("workday_loc", "Delivery_Location", "facility", "Workday SCM", "Receiving hospital facility"),

        # Generic / Hospital Spreadsheets
        ("generic_item_no", "Item#", "item_number", "Generic Spreadsheet", "Common header for item identifier"),
        ("generic_mat_no", "Material_Number", "item_number", "SAP / Generic", "Material number identifier"),
        ("generic_rate", "Rate", "supply_unit_price", "Generic Spreadsheet", "Item billing rate / price"),
        ("generic_surg_date", "Surgery_Date", "admit_date_time", "Generic Spreadsheet", "Clinical procedure date")
    ]

    for sig_id, src_col, canon_col, src_sys, desc in ehr_mappings:
        store.add_ehr_signature(sig_id, src_col, canon_col, src_sys, desc)

    # 3. Known Self-Healing Resolution Patterns
    resolutions = [
        (
            "Binder Error: Column not found",
            "Check column aliasing in staging layer. Ensure sources.yml auto-renames or uses COALESCE on synonym columns.",
            "Column name mismatch between raw CSV header and dbt model expectation."
        ),
        (
            "Conversion Error: Could not parse date",
            "Replace strict CAST(x AS TIMESTAMP) with COALESCE(TRY_STRPTIME(x, '%Y-%m-%d %H:%M:%S'), TRY_STRPTIME(x, '%m/%d/%Y'), TRY_CAST(x AS TIMESTAMP)).",
            "Hospital data contains multiple date string formats."
        ),
        (
            "Duplicate key in join / Cartesian explosion",
            "Wrap joining table in CTE with QUALIFY ROW_NUMBER() OVER (PARTITION BY key ORDER BY end_date DESC) = 1.",
            "Multiple overlapping active contracts or item entries."
        )
    ]

    for pat, fix, ctx in resolutions:
        store.add_error_resolution(pat, fix, ctx)

    print("ChromaDB knowledge seeding completed successfully!")

if __name__ == "__main__":
    seed_standards()
