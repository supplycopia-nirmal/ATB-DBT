import os
import csv
import json
from pathlib import Path
from typing import Dict, Any, List, Optional
import pandas as pd
from autonomous_dbt_app.config import REQUIRED_ENTITIES, OPTIONAL_ENTITIES
from autonomous_dbt_app.core.memory_store import MemoryStore

class ProfilerAgent:
    """
    Inspector Astra: Profiler Agent
    Discovers data files in client folder, detects delimiters/encodings,
    profiles schemas (datatypes, null rates, cardinality), classifies source tables,
    and identifies missing data sources with mitigation strategies.
    """
    def __init__(self, memory_store: Optional[MemoryStore] = None):
        self.memory = memory_store or MemoryStore()

    def profile_directory(self, folder_path: Path) -> Dict[str, Any]:
        folder = Path(folder_path)
        if not folder.exists() or not folder.is_dir():
            raise ValueError(f"Directory {folder_path} does not exist.")

        manifest = {
            "folder_path": str(folder),
            "files_found": [],
            "source_classification": {},
            "missing_sources": [],
            "mitigation_plan": {},
            "status": "PROFILED"
        }

        # Scan for supported files
        supported_extensions = [".csv", ".tsv", ".txt", ".parquet", ".xlsx", ".xls"]
        files = [f for f in folder.iterdir() if f.is_file() and f.suffix.lower() in supported_extensions]

        for file_path in files:
            file_profile = self._profile_file(file_path)
            manifest["files_found"].append(file_profile)

        # Classify each file into canonical entities
        # Prioritize canonical UC Health datasets so they become the primary source classification
        canonical_uc_health_files = {
            "UHC_Consumption.csv",
            "UHC_CON_20260930010958.csv",
            "UHC_IM_20260930010918.csv",
            "UHC_PO_20260930010955.csv",
            "UHC_INV_20260930010902.csv"
        }
        sorted_files = sorted(
            manifest["files_found"],
            key=lambda x: (0 if x["file_name"] in canonical_uc_health_files else 1)
        )

        classified_entities = set()
        for file_info in sorted_files:
            entity_type = self._classify_entity(file_info)
            file_info["classified_entity"] = entity_type
            if entity_type != "UNKNOWN":
                # Only assign if not already claimed by a higher-priority canonical file
                if entity_type not in manifest["source_classification"]:
                    manifest["source_classification"][entity_type] = file_info["file_name"]
                classified_entities.add(entity_type)

        # Check required and optional entities
        missing_required = [req for req in REQUIRED_ENTITIES if req not in classified_entities]
        missing_optional = [opt for opt in OPTIONAL_ENTITIES if opt not in classified_entities]

        if missing_required:
            manifest["status"] = "ERROR_MISSING_REQUIRED"
            manifest["missing_required"] = missing_required
            manifest["error_message"] = f"Missing mandatory datasets: {missing_required}. Spend/Consumption and Purchase Orders are required."

        manifest["missing_sources"] = missing_optional

        # Formulate mitigation strategy for missing optional datasets
        for missing in missing_optional:
            if missing == "inventory":
                manifest["mitigation_plan"]["inventory"] = {
                    "action": "SYNTHETIC_PROXY_OR_NULL_STUB",
                    "description": "Inventory data not provided. Generate synthetic inventory holding cost proxy from PO receipt velocity and consumption rate; provide zero-holding risk stub view."
                }
            elif missing == "invoice":
                manifest["mitigation_plan"]["invoice"] = {
                    "action": "BYPASS_INVOICE_VARIANCE",
                    "description": "Invoice reconciliation skipped. PO price variance and contract compliance will be calculated directly without 3-way invoice matching."
                }
            elif missing == "item_master":
                manifest["mitigation_plan"]["item_master"] = {
                    "action": "IMPLICIT_ITEM_MASTER_STUB",
                    "description": "Item master missing. Synthesize item catalog from distinct items across consumption and purchase order lines."
                }
            elif missing == "contracts":
                manifest["mitigation_plan"]["contracts"] = {
                    "action": "OFF_CONTRACT_BENCHMARK",
                    "description": "Contract dataset missing. Treat all spend lines as off-contract and apply standard 15% SupplyCopia benchmark savings opportunity."
                }
            elif missing == "facility_mapping":
                manifest["mitigation_plan"]["facility_mapping"] = {
                    "action": "IDENTITY_FACILITY_STUB",
                    "description": "Facility mapping table missing. Use raw facility names directly with standard identity stub."
                }
            elif missing == "vendor_alias":
                manifest["mitigation_plan"]["vendor_alias"] = {
                    "action": "IDENTITY_VENDOR_STUB",
                    "description": "Vendor alias table missing. Use raw supplier names directly without canonical vendor grouping."
                }

        return manifest

    def _profile_file(self, file_path: Path) -> Dict[str, Any]:
        file_name = file_path.name
        file_size = file_path.stat().st_size
        suffix = file_path.suffix.lower()

        delimiter = self._sniff_delimiter(file_path) if suffix in [".csv", ".tsv", ".txt"] else None

        columns_info = []
        total_sample_rows = 0
        total_rows_est = 0
        try:
            if suffix in [".csv", ".tsv", ".txt"]:
                # Fast row count estimate
                df_sample = pd.read_csv(file_path, sep=delimiter or ',', nrows=500, low_memory=False)
                df = df_sample
            elif suffix == ".parquet":
                df = pd.read_parquet(file_path).head(500)
            elif suffix in [".xlsx", ".xls"]:
                df = pd.read_excel(file_path, nrows=500)
            else:
                df = pd.DataFrame()

            total_sample_rows = len(df)
            for col in df.columns:
                series = df[col]
                dtype = str(series.dtype)
                null_cnt = int(series.isnull().sum())
                null_pct = round((null_cnt / len(series)) * 100, 1) if len(series) > 0 else 0.0
                completeness_pct = round(100.0 - null_pct, 1)
                distinct_cnt = int(series.nunique(dropna=True))
                sample_vals = [str(x) for x in series.dropna().unique()[:4]]

                # Inferred semantic role
                col_upper = str(col).upper()
                inferred_role = "Attribute"
                if any(k in col_upper for k in ["ID", "NUM", "NUMBER", "KEY", "CODE"]):
                    inferred_role = "Identifier / Key"
                elif any(k in col_upper for k in ["PRICE", "COST", "SPEND", "TOTAL", "AMOUNT", "QTY", "QUANTITY", "BAL"]):
                    inferred_role = "Numeric Metric"
                elif any(k in col_upper for k in ["DATE", "TIME"]):
                    inferred_role = "Temporal / Date"
                elif any(k in col_upper for k in ["NAME", "DESC", "CATEGORY", "TYPE", "STATUS"]):
                    inferred_role = "Dimension / Category"

                columns_info.append({
                    "column_name": str(col),
                    "data_type": dtype,
                    "completeness_percentage": completeness_pct,
                    "null_count": null_cnt,
                    "distinct_count": distinct_cnt,
                    "inferred_role": inferred_role,
                    "samples": sample_vals
                })
        except Exception as e:
            columns_info = [{"error": f"Failed to sample parse: {str(e)}"}]

        # Compute exact row count using fast buffered streaming
        exact_lines = self._count_lines_exact(file_path) if suffix in [".csv", ".tsv", ".txt"] else total_sample_rows
        total_row_count = exact_lines if exact_lines > 0 else total_sample_rows

        # Compute spend using DuckDB where possible
        total_spend_value = 0.0
        try:
            import duckdb
            con_quick = duckdb.connect()
            delim_arg = f"delim='{delimiter}'," if delimiter else ""
            read_sql = f"read_csv('{file_path}', {delim_arg} header=True, all_varchar=True, null_padding=True, ignore_errors=True, strict_mode=False, quote='\"', parallel=false)"
            col_names_upper = [str(c.get("column_name", "")).upper() for c in columns_info]
            if "TOTAL_VALUE" in col_names_upper:
                spend_res = con_quick.execute(f"SELECT round(sum(try_cast(TOTAL_VALUE as double)), 2) FROM {read_sql}").fetchone()
                if spend_res and spend_res[0] is not None:
                    total_spend_value = float(spend_res[0])
            elif "SUPPLY_UNIT_PRICE" in col_names_upper and "TOTAL_QUANTITY" in col_names_upper:
                spend_res = con_quick.execute(f"SELECT round(sum(try_cast(SUPPLY_UNIT_PRICE as double) * try_cast(TOTAL_QUANTITY as double)), 2) FROM {read_sql}").fetchone()
                if spend_res and spend_res[0] is not None:
                    total_spend_value = float(spend_res[0])
            con_quick.close()
        except Exception:
            pass

        # Auto-select ONLY the 5 canonical UC Health sources used by the application
        canonical_uc_health_files = {
            "UHC_Consumption.csv",
            "UHC_CON_20260930010958.csv",
            "UHC_IM_20260930010918.csv",
            "UHC_PO_20260930010955.csv",
            "UHC_INV_20260930010902.csv"
        }
        is_canonical = (file_name in canonical_uc_health_files) or (file_name.startswith("UHC_Consumption_") and not file_name.startswith("Filtered_"))

        return {
            "file_name": file_name,
            "file_path": str(file_path),
            "file_size_bytes": file_size,
            "format": suffix.lstrip('.'),
            "delimiter": delimiter,
            "sample_row_count": total_sample_rows,
            "total_row_count": total_row_count,
            "total_spend_value": total_spend_value,
            "selected_for_pipeline": bool(is_canonical),
            "columns": columns_info
        }

    def _count_lines_exact(self, file_path: Path) -> int:
        try:
            with open(file_path, 'rb') as f:
                lines = 0
                buf_size = 1024 * 1024
                while chunk := f.read(buf_size):
                    lines += chunk.count(b'\n')
                return max(0, lines - 1)
        except Exception:
            return 0


    def _sniff_delimiter(self, file_path: Path) -> str:
        candidates = ['|', ',', '\t', ';']
        counts = {c: 0 for c in candidates}
        try:
            with open(file_path, 'r', errors='ignore') as f:
                lines = [f.readline() for _ in range(5)]
            for line in lines:
                for c in candidates:
                    counts[c] += line.count(c)
            best = max(counts, key=counts.get)
            return best if counts[best] > 0 else ','
        except Exception:
            return ','

    def _classify_entity(self, file_info: Dict[str, Any]) -> str:
        fname = file_info["file_name"].upper()
        col_names = [c.get("column_name", "").upper() for c in file_info.get("columns", [])]

        # 1. Filename heuristic - Contracts prioritized before general consumption to prevent UHC_CON_* mistagging
        if any(k in fname for k in ["CONTRACT", "PRICING_AGREEMENT", "TIER_PRICING", "GPO_PRICE", "UHC_CON_", "CON_"]):
            return "contracts"
        if any(k in fname for k in ["CONSUMPTION", "SPEND", "USAGE", "UTILIZATION"]):
            return "consumption"
        if any(k in fname for k in ["PURCHASE_ORDER", "_PO_", "PO_2", "PURCHASE_ORDERS", "PO."]):
            return "purchase_order"
        if any(k in fname for k in ["ITEM_MASTER", "ITEMMASTER", "IM_2", "PRODUCT_CATALOG", "ITEMS"]):
            return "item_master"
        if any(k in fname for k in ["INVOICE", "AP_INV", "INV_2", "ACCOUNTS_PAYABLE"]):
            return "invoice"
        if any(k in fname for k in ["INVENTORY", "STOCK", "ON_HAND", "INV_LEVEL"]):
            return "inventory"
        if any(k in fname for k in ["VENDOR_ALIAS", "SUPPLIER_MAPPING", "ALIAS"]):
            return "vendor_alias"
        if any(k in fname for k in ["FACILITY", "HOSPITAL_MAP", "LOCATION_MAP"]):
            return "facility_mapping"

        # 2. Column-based heuristic
        if any(c in col_names for c in ["CONTRACT_NUMBER", "CONTRACT_PRICE", "CONTRACT_START_DATE"]):
            return "contracts"
        if any(c in col_names for c in ["LOG_ID", "ADMIT_DATE_TIME", "SURGICAL_HIERARCHY", "DRG_CODE", "TX_ID"]):
            return "consumption"
        if any(c in col_names for c in ["PO_NUMBER", "PO_LINE_NO", "ORDER_DATE", "LINE_NBR"]):
            return "purchase_order"
        if any(c in col_names for c in ["ITEM_ID", "MFR_PART_NUMBER", "UNSPSC_CODE", "ITEM_DESCRIPTION"]):
            return "item_master"
        if any(c in col_names for c in ["INVOICE_NUMBER", "INVOICE_QTY", "INVOICE_PAID_DATE"]):
            return "invoice"

        return "UNKNOWN"
