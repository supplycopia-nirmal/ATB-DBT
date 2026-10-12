"""
SupplyCopia Enterprise Schema Contract Validator
Strict JSON Schema & Header Contract Enforcement for Incoming Healthcare Datasets
"""
import json
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

CANONICAL_SCHEMA_CONTRACTS: Dict[str, Dict[str, Any]] = {
    "consumption": {
        "entity_type": "clinical_consumption",
        "primary_keys": ["case_id", "item_number"],
        "required_columns": [
            "case_id",
            "facility",
            "drg_code",
            "supply_unit_price",
            "total_quantity",
            "item_number",
            "item_description"
        ],
        "optional_columns": [
            "admit_date_time",
            "discharge_date_time",
            "patient_mrn",
            "patient_encounter_id",
            "surgeon_npi",
            "primary_surgeon_name",
            "procedure_date",
            "risk_of_mortality",
            "smoking_status",
            "manufacturer_name",
            "mfr_item_number",
            "vendor_name",
            "po_reference_id"
        ],
        "column_types": {
            "supply_unit_price": "float",
            "total_quantity": "float",
            "drg_code": "varchar"
        }
    },
    "purchase_orders": {
        "entity_type": "procurement_orders",
        "primary_keys": ["po_number", "po_line_no"],
        "required_columns": [
            "po_number",
            "po_line_no",
            "item_id",
            "unit_price",
            "quantity",
            "vendor_name"
        ],
        "optional_columns": [
            "po_date",
            "facility_name",
            "total_value",
            "vendor_code",
            "order_status",
            "buyer_id"
        ],
        "column_types": {
            "unit_price": "float",
            "quantity": "float"
        }
    },
    "contracts": {
        "entity_type": "contract_catalog",
        "primary_keys": ["contract_number", "item_id"],
        "required_columns": [
            "contract_number",
            "item_id",
            "contract_price"
        ],
        "optional_columns": [
            "vendor_name",
            "contract_start",
            "contract_end",
            "contract_uom",
            "tier_level"
        ],
        "column_types": {
            "contract_price": "float"
        }
    },
    "item_master": {
        "entity_type": "catalog_crosswalk",
        "primary_keys": ["item_id"],
        "required_columns": [
            "item_id",
            "item_description"
        ],
        "optional_columns": [
            "manufacturer_part_number",
            "manufacture_name",
            "vendor_name",
            "vendor_code",
            "is_active",
            "unspsc_code"
        ],
        "column_types": {}
    },
    "invoices": {
        "entity_type": "ap_invoices",
        "primary_keys": ["invoice_number", "po_number"],
        "required_columns": [
            "invoice_number",
            "po_number",
            "invoice_qty",
            "invoice_unit_price"
        ],
        "optional_columns": [
            "po_line_no",
            "item_id",
            "invoice_total_value",
            "invoice_paid_date"
        ],
        "column_types": {
            "invoice_qty": "float",
            "invoice_unit_price": "float"
        }
    }
}

class SchemaContractValidator:
    """Validates staged CSV files against strict enterprise JSON contracts."""

    @staticmethod
    def validate_csv_contract(file_path: Path, entity: str) -> Dict[str, Any]:
        contract = CANONICAL_SCHEMA_CONTRACTS.get(entity.lower())
        if not contract:
            return {
                "valid": True,
                "entity": entity,
                "message": f"No strict contract configured for entity '{entity}'. Permissive mode applied."
            }

        if not file_path.exists():
            return {
                "valid": False,
                "entity": entity,
                "error": f"File does not exist: {file_path}"
            }

        try:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                header_line = f.readline().strip()
                sample_data_line = f.readline().strip()

            delim = "|" if "|" in header_line else ("," if "," in header_line else "\t")
            columns = [c.strip('"').strip().lower() for c in header_line.split(delim)]

            # Map known aliases
            COLUMN_ALIASES = {
                "case_id": ["case_id", "case_id / encounter fhir id", "encounter_id", "encounter_num"],
                "item_number": ["item_number", "item_id", "item_code"],
                "po_number": ["po_number", "purchase_order_number", "order_id"],
                "po_line_no": ["po_line_no", "po_line", "line_number"],
                "vendor_name": ["vendor_name", "supplier", "manufacturer_name"]
            }

            def column_present(canonical_col: str) -> bool:
                aliases = COLUMN_ALIASES.get(canonical_col.lower(), [canonical_col.lower()])
                return any(a in columns for a in aliases)

            missing_required = []
            for req in contract["required_columns"]:
                if not column_present(req):
                    missing_required.append(req)

            matched_pks = [pk for pk in contract["primary_keys"] if column_present(pk)]
            pk_intact = len(matched_pks) == len(contract["primary_keys"])

            is_valid = len(missing_required) == 0 and pk_intact

            return {
                "valid": is_valid,
                "entity": entity,
                "file_name": file_path.name,
                "delimiter_detected": delim,
                "total_columns": len(columns),
                "matched_required_columns": len(contract["required_columns"]) - len(missing_required),
                "missing_required_columns": missing_required,
                "primary_keys_status": "PASS" if pk_intact else "FAIL",
                "missing_primary_keys": [pk for pk in contract["primary_keys"] if pk.lower() not in columns],
                "contract_lock_status": "LOCKED_VALIDATED" if is_valid else "CONTRACT_VIOLATION"
            }
        except Exception as e:
            return {
                "valid": False,
                "entity": entity,
                "error": f"Schema contract parsing exception: {str(e)}"
            }
