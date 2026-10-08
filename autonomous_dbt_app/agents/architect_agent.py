import json
from typing import Dict, Any, List, Optional
from autonomous_dbt_app.core.memory_store import MemoryStore
from autonomous_dbt_app.core.cortex_client import CortexClient

class ArchitectAgent:
    """
    Architect DaVinci: Ontology & Semantic Mapping Agent
    Synthesizes SupplyCopia canonical column mappings, multi-tier matching topology,
    validation rule matrix, and dbt pipeline configuration blueprint.
    """
    def __init__(self, memory_store: Optional[MemoryStore] = None, cortex_client: Optional[CortexClient] = None):
        self.memory = memory_store or MemoryStore()
        self.cortex = cortex_client or CortexClient()

    def generate_blueprint(self, profile_manifest: Dict[str, Any], client_metadata: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        source_class = profile_manifest.get("source_classification", {})
        files_found = {f["file_name"]: f for f in profile_manifest.get("files_found", [])}

        client_meta = client_metadata or {
            "client_name": "UC Health",
            "client_id": "CL_UCH_001",
            "client_type": "HealthCare System"
        }

        # 1. Identify Joins Required across Datasets
        discovered_joins = [
            {
                "join_id": "JOIN_CONS_ITEM_MASTER",
                "left_dataset": "consumption",
                "right_dataset": "item_master",
                "join_type": "LEFT OUTER",
                "description": "Multi-tier item identifier resolution: maps clinical consumption records to canonical item master catalog.",
                "join_keys": [
                    {"tier": 1, "rule": "Exact Item ID", "left_col": "item_number", "right_col": "item_id", "confidence": "100%"},
                    {"tier": 2, "rule": "Catalog Part Number", "left_col": "manufacturer_catalog_number", "right_col": "mfr_part_number", "confidence": "90%"},
                    {"tier": 3, "rule": "Vendor Part Number", "left_col": "manufacturer_catalog_number", "right_col": "vendor_part_number", "confidence": "80%"}
                ]
            },
            {
                "join_id": "JOIN_CONS_CONTRACTS",
                "left_dataset": "consumption",
                "right_dataset": "contracts",
                "join_type": "LEFT OUTER",
                "description": "Contract pricing linkage: matches effective items and normalized suppliers to active price agreements.",
                "join_keys": [
                    {"tier": 1, "rule": "Item + Vendor + Part# + UOM", "keys": "item_id + vendor_name + manufacturer_part_number + contract_uom"},
                    {"tier": 2, "rule": "Item + Vendor + UOM", "keys": "item_id + vendor_name + contract_uom"},
                    {"tier": 3, "rule": "Item + Vendor + Any UOM", "keys": "item_id + vendor_name (with EA factor conversion)"}
                ],
                "validity_window": "consumption_date BETWEEN contract_start_date AND COALESCE(contract_end_date, '2099-12-31')"
            },
            {
                "join_id": "JOIN_CONS_FACILITY",
                "left_dataset": "consumption",
                "right_dataset": "facility_mapping",
                "join_type": "LEFT OUTER",
                "description": "Hospital entity normalization: maps local departmental mnemonic to standard facility name and region.",
                "join_keys": [{"rule": "Facility Name Match", "left_col": "facility", "right_col": "source_facility"}]
            },
            {
                "join_id": "JOIN_PO_CONTRACTS",
                "left_dataset": "purchase_orders",
                "right_dataset": "contracts",
                "join_type": "LEFT OUTER",
                "description": "PO contract compliance check: links purchase order lines to active negotiated contract pricing.",
                "join_keys": [{"rule": "Item ID Match", "left_col": "item_id", "right_col": "item_id"}],
                "validity_window": "po_date BETWEEN contract_start_date AND contract_end_date"
            },
            {
                "join_id": "JOIN_PO_INVOICES",
                "left_dataset": "purchase_orders",
                "right_dataset": "invoices",
                "join_type": "LEFT OUTER",
                "description": "3-way PO-to-invoice reconciliation: matches invoice line amounts and paid quantities to PO line numbers.",
                "join_keys": [{"rule": "PO Number & Line", "left_col": "po_number + po_line_no", "right_col": "po_number + po_line_no"}]
            }
        ]

        # 2. Applicable Transformations Catalog
        transformations_catalog = [
            {"id": "TR_METADATA", "name": "SupplyCopia Audit Metadata Injection", "applies_to": "All Staging Views", "details": "Appends _source_file, _source_row_number, _row_hash (MD5), and _ingested_at"},
            {"id": "TR_TENANCY", "name": "Multi-Tenant Agnostic Header Injection", "applies_to": "All Marts & Staging", "details": f"Injects client_id='{client_meta['client_id']}', client_name='{client_meta['client_name']}', client_type='{client_meta['client_type']}', and ingestion_batch_id"},
            {"id": "TR_UOM", "name": "Unit of Measure (UOM) Canonicalization", "applies_to": "Consumption & PO", "details": "Standardizes EA, BX, CA, PK and applies conversion multipliers"},
            {"id": "TR_CLEANSE", "name": "Safe Casting & Currency Scrubbing", "applies_to": "All Numeric & Date Columns", "details": "Converts currency strings and mixed date formats using safe DuckDB TRY_CAST"},
            {"id": "TR_DRG", "name": "Procedure & DRG Clinical Grouping", "applies_to": "Consumption Marts", "details": "Categorizes procedures into Orthopedic, Spinal, Cardiac, Neurosurgery, and General Surgery"},
            {"id": "TR_SAVINGS", "name": "Price Variance & Savings Benchmark", "applies_to": "Marts Layer", "details": "Computes exact price variance against contract and applies 15% benchmark savings to off-contract spend"}
        ]

        blueprint = {
            "client_folder": profile_manifest.get("folder_path"),
            "client_metadata": client_meta,
            "pipeline_name": "autonomous_supplycopia_pipeline",
            "sources": {},
            "discovered_joins": discovered_joins,
            "transformations_catalog": transformations_catalog,
            "matching_topology": {

                "item_matching_tiers": [
                    {"tier": 1, "rule": "exact_item_id", "confidence": 1.0, "condition": "c.item_number = im.item_id"},
                    {"tier": 2, "rule": "mfr_part_number", "confidence": 0.90, "condition": "upper(trim(c.manufacturer_catalog_number)) = upper(trim(im.mfr_part_number))"},
                    {"tier": 3, "rule": "vendor_part_number", "confidence": 0.80, "condition": "upper(trim(c.manufacturer_catalog_number)) = upper(trim(im.vendor_part_number))"}
                ],
                "contract_matching_tiers": [
                    {"tier": 1, "rule": "item_vendor_part_uom", "condition": "k.effective_item_id = co.item_id AND k.standard_supplier = co.vendor_name AND k.manufacturer_catalog_number = co.manufacturer_part_number AND k.item_uom = co.contract_uom"},
                    {"tier": 2, "rule": "item_vendor_uom", "condition": "k.effective_item_id = co.item_id AND k.standard_supplier = co.vendor_name AND k.item_uom = co.contract_uom"},
                    {"tier": 3, "rule": "item_vendor_any_uom", "condition": "k.effective_item_id = co.item_id AND k.standard_supplier = co.vendor_name"}
                ]
            },
            "validation_rules": {
                "flags": [
                    {"flag": "NO_PRICE", "expr": "not is_price_above_zero"},
                    {"flag": "ZERO_QTY", "expr": "not is_quantity_above_zero"},
                    {"flag": "INVALID_DATE", "expr": "not is_valid_admit_date"},
                    {"flag": "NO_ITEM_MASTER", "expr": "not is_item_master_matched"},
                    {"flag": "NO_CONTRACT", "expr": "not is_contract_matched"}
                ],
                "eligibility": {
                    "ELIGIBLE": "is_price_above_zero and is_quantity_above_zero and is_valid_admit_date and is_contract_matched",
                    "INELIGIBLE": "not is_price_above_zero or not is_quantity_above_zero or not is_valid_admit_date",
                    "REVIEW_REQUIRED": "ELSE"
                }
            },
            "mitigation_plan": profile_manifest.get("mitigation_plan", {}),
            "status": "BLUEPRINT_GENERATED"
        }

        # Build column mappings for each classified source
        for entity, filename in source_class.items():
            file_meta = files_found.get(filename, {})
            cols = [c.get("column_name") for c in file_meta.get("columns", [])]
            col_map = self._map_columns_for_entity(entity, cols)
            blueprint["sources"][entity] = {
                "file_name": filename,
                "delimiter": file_meta.get("delimiter", ","),
                "format": file_meta.get("format", "csv"),
                "detected_columns": cols,
                "canonical_mapping": col_map
            }

        return blueprint

    def _map_columns_for_entity(self, entity: str, source_columns: List[str]) -> Dict[str, str]:
        """
        Maps source columns to canonical SupplyCopia columns using vector similarity and heuristics.
        """
        canonical_map = {}
        for col in source_columns:
            clean_col = col.strip()
            # 1. Direct exact or lowercase match
            canon = self._heuristic_match(entity, clean_col)
            if not canon:
                # 2. Query ChromaDB semantic signatures
                matches = self.memory.match_column_semantic(clean_col, n_results=1)
                if matches and matches[0]["distance"] < 0.45:
                    canon = matches[0]["metadata"].get("canonical_field")
            if canon:
                canonical_map[clean_col] = canon
            else:
                canonical_map[clean_col] = clean_col.lower().replace(" ", "_").replace("-", "_")
        return canonical_map

    def _heuristic_match(self, entity: str, col: str) -> Optional[str]:
        uc = col.upper()
        if entity == "consumption":
            if uc in ["LOG_ID", "TRANSACTION_ID", "TX_ID", "CHARGE_ID", "ID"]: return "log_id"
            if uc in ["FACILITY", "HOSPITAL", "LOCATION", "DEPT", "DEPARTMENT_NAME"]: return "facility"
            if uc in ["DRG_CODE", "DRG", "SURG_DRG"]: return "drg_code"
            if uc in ["BILLED_CPT_CODE", "CPT_CODE", "CPT", "PROC_CODE"]: return "billed_cpt_code"
            if uc in ["PRIMARY_PROCEDURE", "PROCEDURE_DESC", "SURG_CASE_PROC"]: return "primary_procedure"
            if uc in ["SERVICE_LINE", "SPECIALTY"]: return "service_line"
            if uc in ["PATIENT_TYPE", "PAT_TYPE"]: return "patient_type"
            if uc in ["ADMIT_DATE_TIME", "SURGERY_DATE", "SERVICE_DATE", "TX_DATE"]: return "admit_date_time"
            if uc in ["SUPPLY_UNIT_PRICE", "UNIT_COST", "ITEM_PRICE", "RATE"]: return "supply_unit_price"
            if uc in ["TOTAL_QUANTITY", "QUANTITY", "QTY", "ITEM_QTY"]: return "total_quantity"
            if uc in ["MANUFACTURER_NAME", "MFR_NAME", "VENDOR"]: return "manufacturer_name"
            if uc in ["MANUFACTURER_CATALOG_NUMBER", "CATALOG_NO", "MFR_CAT_NO", "PART_NUM"]: return "manufacturer_catalog_number"
            if uc in ["ITEM_NUMBER", "ITEM_ID", "ITEM#", "IMPLANT_ID", "MATERIAL_NUMBER"]: return "item_number"
            if uc in ["ITEM_DESCRIPTION", "DESCRIPTION", "ITEM_DESC"]: return "item_description"
            if uc in ["ITEM_UOM", "UOM", "UNIT_OF_MEASURE"]: return "raw_item_uom"
            if uc in ["SUPPLIER", "VENDOR_NAME", "DISTRIBUTOR"]: return "supplier"
            if uc in ["UNSPSC_CODE", "UNSPSC"]: return "unspsc_code"

        elif entity == "purchase_order":
            if uc in ["PO_NUMBER", "PO_CODE", "PURCHASE_ORDER_NUMBER", "DOC_NUM"]: return "po_number"
            if uc in ["PO_LINE_NO", "LINE_NBR", "LINE_NUMBER", "PO_LINE"]: return "po_line_no"
            if uc in ["PO_DATE", "ORDER_DATE", "CREATION_DATE"]: return "po_date"
            if uc in ["ITEM_ID", "ITEM_NUMBER", "ITEM", "MATERIAL_ID"]: return "item_id"
            if uc in ["UNIT_PRICE", "PRICE", "ENT_UNIT_COST", "PURCHASE_PRICE"]: return "unit_price"
            if uc in ["QUANTITY", "ORDER_QTY", "ENT_BUY_QTY", "QTY"]: return "quantity"
            if uc in ["TOTAL_VALUE", "EXTENDED_AMOUNT", "LINE_TOTAL"]: return "total_value"
            if uc in ["UOM", "UOM_CODE", "PURCH_UOM"]: return "uom"
            if uc in ["VENDOR_NAME", "SUPPLIER_NAME", "VENDOR"]: return "vendor_name"

        elif entity == "item_master":
            if uc in ["ITEM_ID", "ITEM_NUMBER", "ITEM"]: return "item_id"
            if uc in ["MFR_PART_NUMBER", "MANUFACTURER_PART_NUMBER", "CATALOG_NO"]: return "mfr_part_number"
            if uc in ["VENDOR_PART_NUMBER", "VEN_ITEM", "DIST_PART_NO"]: return "vendor_part_number"
            if uc in ["ITEM_DESCRIPTION", "DESCRIPTION"]: return "item_description"
            if uc in ["UNSPSC_CODE", "UNSPSC"]: return "unspsc_code"

        elif entity == "contracts":
            if uc in ["CONTRACT_NUMBER", "AGREEMENT_NO", "TIER_CODE"]: return "contract_number"
            if uc in ["ITEM_ID", "ITEM_NUMBER", "PRODUCT_ID"]: return "item_id"
            if uc in ["CONTRACT_PRICE", "AGREED_PRICE", "TIER_PRICE"]: return "contract_price"
            if uc in ["CONTRACT_EA_PRICE", "EA_PRICE"]: return "contract_ea_price"
            if uc in ["CONTRACT_UOM", "AGREED_UOM"]: return "contract_uom"
            if uc in ["CONTRACT_START_DATE", "START_DATE", "EFFECTIVE_DATE"]: return "contract_start_date"
            if uc in ["CONTRACT_END_DATE", "END_DATE", "EXPIRATION_DATE"]: return "contract_end_date"
            if uc in ["VENDOR_NAME", "SUPPLIER"]: return "vendor_name"

        return None
