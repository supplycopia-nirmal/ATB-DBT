import os
import sys
import json
import time
import hashlib
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path
from datetime import datetime
import duckdb
import pandas as pd

from autonomous_dbt_app.config import OUTPUT_DIR, ROOT_DIR as PROJECT_ROOT, BASE_DATA_DIR

CATALOGS_DIR = PROJECT_ROOT / "agent_memory" / "client_catalogs"
CATALOGS_DIR.mkdir(parents=True, exist_ok=True)

ENTITY_NATURAL_KEYS = {
    "item_master": ["item_id"],
    "contracts": ["contract_number", "item_id"],
    "purchase_orders": ["po_number", "po_line_no"],
    "invoices": ["invoice_number", "po_line_no"],
    "consumption": ["log_id"]
}

ENTITY_CANONICAL_FILENAMES = {
    "item_master": "UHC_IM_20260930010918.csv",
    "contracts": "UHC_CON_20260930010958.csv",
    "purchase_orders": "UHC_PO_20260930010839.csv",
    "invoices": "UHC_INV_20260930010902.csv",
    "consumption": "UHC_Consumption_20260930010803.csv"
}


def calculate_file_hash(file_path: Path) -> str:
    """Calculates SHA-256 hash of a file."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


class ClientFileTracker:
    """
    Maintains a persistent client-partitioned catalog of all files ingested over time.
    Tracks file provenance, version lineage, row counts, and active/superseded statuses.
    """
    def __init__(self, client_id: str = "CL_UCH_001", catalog_dir: Optional[Path] = None):
        self.client_id = client_id
        self.catalog_dir = Path(catalog_dir) if catalog_dir else CATALOGS_DIR
        self.catalog_dir.mkdir(parents=True, exist_ok=True)
        self.catalog_path = self.catalog_dir / f"{self.client_id}_catalog.json"
        self._ensure_catalog_initialized()

    def _ensure_catalog_initialized(self, client_id: Optional[str] = None):
        """Initializes catalog from disk or scans existing staged data as initial baseline."""
        cid = client_id or self.client_id
        cpath = self.catalog_dir / f"{cid}_catalog.json"
        if not cpath.exists():
            initial_catalog = {
                "client_id": cid,
                "client_name": "UC Health" if cid == "CL_UCH_001" else f"Client {cid}",
                "created_at": datetime.now().isoformat(),
                "last_updated": datetime.now().isoformat(),
                "files": []
            }
            # Auto-seed from existing staged files or BASE_DATA_DIR only for baseline tenant
            if cid == "CL_UCH_001":
                staged_dir = OUTPUT_DIR / "staged_client_data"
                scan_dir = staged_dir if staged_dir.exists() and any(staged_dir.glob("*.csv")) else BASE_DATA_DIR
                
                if scan_dir.exists():
                    for p in sorted(scan_dir.glob("*.csv")):
                        entity_type = self.detect_entity_type(p.name)
                        row_cnt = self._count_csv_rows(p)
                        f_hash = calculate_file_hash(p)
                        initial_catalog["files"].append({
                            "file_id": f"FILE_{cid}_{entity_type[:4].upper()}_BASE",
                            "filename": p.name,
                            "file_name": p.name,
                            "file_path": str(p),
                            "entity_type": entity_type,
                            "version": 1,
                            "version_label": "v1.0 (Baseline)",
                            "row_count": row_cnt,
                            "file_size_bytes": p.stat().st_size,
                            "file_hash": f_hash,
                            "sha256_hash": f_hash,
                            "registered_at": datetime.now().isoformat(),
                            "ingested_at": datetime.now().isoformat(),
                            "status": "ACTIVE",
                            "is_canonical": True,
                            "notes": "Baseline bulk dataset",
                            "merge_history": []
                        })
            self._save_catalog(initial_catalog, cid)

    def _count_csv_rows(self, file_path: Path) -> int:
        try:
            con = duckdb.connect()
            cnt = con.execute(f"SELECT count(*) FROM read_csv_auto('{file_path}', ignore_errors=true);").fetchone()[0]
            con.close()
            return cnt
        except Exception:
            try:
                with open(file_path, "r", errors="ignore") as f:
                    return sum(1 for _ in f) - 1
            except Exception:
                return 0

    def _load_catalog(self, client_id: Optional[str] = None) -> Dict[str, Any]:
        cid = client_id or self.client_id
        cpath = self.catalog_dir / f"{cid}_catalog.json"
        if not cpath.exists():
            self._ensure_catalog_initialized(cid)
        try:
            with open(cpath, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {"client_id": cid, "files": []}

    def _save_catalog(self, data: Dict[str, Any], client_id: Optional[str] = None):
        cid = client_id or data.get("client_id", self.client_id)
        cpath = self.catalog_dir / f"{cid}_catalog.json"
        data["last_updated"] = datetime.now().isoformat()
        with open(cpath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    def detect_entity_type(self, file_name: str) -> str:
        fn = file_name.lower()
        if "item" in fn or "im_" in fn:
            return "item_master"
        elif "contract" in fn or "con_" in fn:
            return "contracts"
        elif "invoice" in fn or "inv_" in fn:
            return "invoices"
        elif "po" in fn or "purchase" in fn:
            return "purchase_orders"
        elif "consump" in fn:
            return "consumption"
        return "other"

    def list_files(self, client_id: Optional[str] = None, entity_type: Optional[str] = None) -> List[Dict[str, Any]]:
        # Handle cases where entity_type was passed as 1st positional argument
        if client_id and client_id in ENTITY_NATURAL_KEYS:
            entity_type = client_id
            client_id = self.client_id
        cat = self._load_catalog(client_id)
        files = cat.get("files", [])
        if entity_type:
            return [f for f in files if f.get("entity_type") == entity_type]
        return files

    def register_file(self, *args, **kwargs) -> Dict[str, Any]:
        """
        Registers a new incoming file in the client catalog.
        Accepts:
          register_file(file_path, entity_type=..., notes=...)
          register_file(client_id, file_path, entity_type, notes=...)
          register_file(client_id=..., file_path=..., entity_type=..., notes=...)
        """
        client_id = kwargs.get("client_id")
        file_path = kwargs.get("file_path")
        entity_type = kwargs.get("entity_type")
        notes = kwargs.get("notes")
        version_label = kwargs.get("version_label")
        custom_metadata = kwargs.get("custom_metadata")

        # Parse positional args
        if len(args) == 1:
            file_path = args[0]
        elif len(args) >= 2:
            first, second = args[0], args[1]
            if isinstance(first, (str, Path)) and (str(first).endswith(".csv") or Path(first).exists()):
                file_path = first
                entity_type = second
            else:
                client_id = str(first)
                file_path = second
            if len(args) >= 3:
                entity_type = args[2]
            if len(args) >= 4:
                notes = args[3]

        cid = client_id or self.client_id
        target_path = Path(file_path)
        if not target_path.exists():
            raise FileNotFoundError(f"File not found: {target_path}")

        detected_entity = entity_type or self.detect_entity_type(target_path.name)
        file_hash = calculate_file_hash(target_path)
        row_count = self._count_csv_rows(target_path)

        cat = self._load_catalog(cid)
        files = cat.setdefault("files", [])

        # Check existing version numbers for this entity
        existing_entity_files = [f for f in files if f.get("entity_type") == detected_entity]
        ver_num = len(existing_entity_files) + 1
        ver_str = version_label or f"v{ver_num}.0 (Update)"

        file_id = f"FILE_{cid}_{detected_entity[:4].upper()}_{int(time.time())}_{ver_num}"
        record = {
            "file_id": file_id,
            "filename": target_path.name,
            "file_name": target_path.name,
            "file_path": str(target_path),
            "entity_type": detected_entity,
            "version": ver_num,
            "version_label": ver_str,
            "row_count": row_count,
            "file_size_bytes": target_path.stat().st_size,
            "file_hash": file_hash,
            "sha256_hash": file_hash,
            "registered_at": datetime.now().isoformat(),
            "ingested_at": datetime.now().isoformat(),
            "status": "ACTIVE",
            "is_canonical": False,
            "notes": notes or "Ingested dataset",
            "custom_metadata": custom_metadata or {},
            "merge_history": []
        }

        files.append(record)
        self._save_catalog(cat, cid)
        return record

    def get_merge_candidates(self, client_id: Optional[str] = None) -> Dict[str, List[Dict[str, Any]]]:
        """Returns entity types having 2 or more active/superseded files eligible for merging."""
        cat = self._load_catalog(client_id)
        candidates = {}
        for f in cat.get("files", []):
            if f.get("status") in ["ACTIVE", "SUPERSEDED"]:
                ent = f.get("entity_type")
                candidates.setdefault(ent, []).append(f)
        return {k: v for k, v in candidates.items() if len(v) >= 2}


class EntityMergeEngine:
    """
    Intelligent Deduplication & Merging Engine.
    Combines primary and supplemental files using customizable deduplication strategies:
    1. 'latest_wins' (SCD Type 1 Upsert): Overwrites attributes on matching primary keys, appends new rows.
    2. 'union_distinct' (Exact Row Deduplication): Purges verbatim duplicates across feeds.
    3. 'additive_append' (Transaction Concatenation): Appends rows while filtering key collisions.
    """
    def __init__(
        self,
        client_id: str = "CL_UCH_001",
        tracker: Optional[ClientFileTracker] = None,
        output_staged_dir: Optional[Path] = None
    ):
        self.client_id = client_id
        self.tracker = tracker or ClientFileTracker(client_id=client_id)
        self.output_staged_dir = Path(output_staged_dir) if output_staged_dir else (OUTPUT_DIR / "staged_client_data")
        self.output_staged_dir.mkdir(parents=True, exist_ok=True)

    def merge_entity(
        self,
        client_id: str = "CL_UCH_001",
        entity_type: str = "item_master",
        merge_strategy: str = "latest_wins",
        primary_keys: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Merges all registered files for a given entity_type in the client catalog.
        Performs de-duplication and writes consolidated file into staged_client_data.
        """
        cid = client_id or self.client_id
        files = self.tracker.list_files(client_id=cid, entity_type=entity_type)
        if not files:
            return {
                "status": "ERROR",
                "message": f"No files registered in catalog for entity: {entity_type}"
            }

        if len(files) == 1:
            # Only single file, already canonical
            f_info = files[0]
            return {
                "status": "SUCCESS",
                "client_id": cid,
                "entity_type": entity_type,
                "merge_strategy": merge_strategy,
                "files_merged_count": 1,
                "total_raw_rows": f_info.get("row_count", 0),
                "final_merged_rows": f_info.get("row_count", 0),
                "duplicates_removed": 0,
                "consolidated_filename": f_info.get("filename"),
                "consolidated_path": f_info.get("file_path"),
                "primary_keys": primary_keys or ENTITY_NATURAL_KEYS.get(entity_type, ["id"])
            }

        # Multiple files: sort by version ascending
        sorted_files = sorted(files, key=lambda x: int(x.get("version", 1)))
        valid_keys = primary_keys or ENTITY_NATURAL_KEYS.get(entity_type, ["item_id"])

        t0 = time.time()
        con = duckdb.connect()

        # Load all versions into DuckDB and tag with version index
        union_sqls = []
        total_raw_rows = 0
        for idx, f_entry in enumerate(sorted_files, 1):
            fpath = Path(f_entry["file_path"])
            if not fpath.exists():
                continue
            table_name = f"t_ver_{idx}"
            con.execute(f"CREATE TEMP TABLE {table_name} AS SELECT * FROM read_csv_auto('{fpath}', ignore_errors=true);")
            row_cnt = con.execute(f"SELECT count(*) FROM {table_name};").fetchone()[0]
            total_raw_rows += row_cnt
            union_sqls.append(f"SELECT *, {idx} as _version_rank FROM {table_name}")

        if not union_sqls:
            con.close()
            return {"status": "ERROR", "message": "Failed to load files into DuckDB for merge."}

        con.execute(f"CREATE TEMP TABLE combined_raw AS {' UNION ALL BY NAME '.join(union_sqls)};")

        # Resolve primary keys against combined columns (case-insensitive)
        all_cols = [c[1].lower() for c in con.execute("PRAGMA table_info('combined_raw');").fetchall()]
        resolved_keys = [k for k in valid_keys if k.lower() in all_cols]
        if not resolved_keys:
            resolved_keys = [all_cols[0]] if all_cols else ["row_id"]

        pk_cols_str = ", ".join(resolved_keys)

        if merge_strategy == "latest_wins":
            # CTE with ROW_NUMBER() over natural keys order by _version_rank DESC
            con.execute(f"""
                CREATE TEMP TABLE merged_output AS
                WITH ranked AS (
                    SELECT *,
                           ROW_NUMBER() OVER(PARTITION BY {pk_cols_str} ORDER BY _version_rank DESC) as _rn
                    FROM combined_raw
                )
                SELECT * EXCLUDE (_version_rank, _rn)
                FROM ranked
                WHERE _rn = 1;
            """)
        elif merge_strategy == "union_distinct":
            con.execute("""
                CREATE TEMP TABLE merged_output AS
                SELECT DISTINCT * EXCLUDE (_version_rank)
                FROM combined_raw;
            """)
        else:  # additive_append
            con.execute("""
                CREATE TEMP TABLE merged_output AS
                SELECT * EXCLUDE (_version_rank)
                FROM combined_raw;
            """)

        final_rows = con.execute("SELECT count(*) FROM merged_output;").fetchone()[0]
        duplicates_removed = total_raw_rows - final_rows

        # Destination filename
        target_name = ENTITY_CANONICAL_FILENAMES.get(entity_type, f"merged_{entity_type}.csv")
        target_path = self.output_staged_dir / target_name

        con.execute(f"COPY merged_output TO '{target_path}' (HEADER, DELIMITER ',');")
        con.close()

        elapsed = round(time.time() - t0, 3)

        # Register merged consolidated file
        merged_rec = self.tracker.register_file(
            file_path=target_path,
            client_id=cid,
            entity_type=entity_type,
            version_label="Consolidated (Latest)",
            notes=f"Consolidated {len(sorted_files)} files using {merge_strategy}",
            custom_metadata={
                "strategy": merge_strategy,
                "merged_source_files": [f["file_id"] for f in sorted_files],
                "duplicates_removed": duplicates_removed,
                "primary_keys": resolved_keys
            }
        )

        # Update source files status in catalog
        cat = self.tracker._load_catalog(cid)
        for f in cat.get("files", []):
            if f["file_id"] in [s["file_id"] for s in sorted_files if s["file_id"] != merged_rec["file_id"]]:
                f["status"] = "SUPERSEDED"
            elif f["file_id"] == merged_rec["file_id"]:
                f["status"] = "MERGED_CONSOLIDATED"
                f["is_canonical"] = True
        self.tracker._save_catalog(cat, cid)

        return {
            "status": "SUCCESS",
            "client_id": cid,
            "entity_type": entity_type,
            "merge_strategy": merge_strategy,
            "files_merged_count": len(sorted_files),
            "total_raw_rows": total_raw_rows,
            "final_merged_rows": final_rows,
            "duplicates_removed": duplicates_removed,
            "consolidated_filename": target_name,
            "consolidated_path": str(target_path),
            "primary_keys": resolved_keys,
            "execution_time_sec": elapsed
        }

    def merge_files(
        self,
        primary_file_id: str,
        supplemental_file_id: str,
        strategy: str = "latest_wins",
        custom_key_columns: Optional[List[str]] = None,
        destination_filename: Optional[str] = None
    ) -> Dict[str, Any]:
        """Executes pairwise merge between two catalog file IDs."""
        cat = self.tracker._load_catalog()
        file_map = {f["file_id"]: f for f in cat.get("files", [])}

        p_info = file_map.get(primary_file_id)
        s_info = file_map.get(supplemental_file_id)

        if not p_info or not s_info:
            raise ValueError(f"Primary ({primary_file_id}) or Supplemental ({supplemental_file_id}) file not found.")

        p_path = Path(p_info["file_path"])
        s_path = Path(s_info["file_path"])

        entity_type = p_info.get("entity_type", "item_master")
        target_name = destination_filename or ENTITY_CANONICAL_FILENAMES.get(entity_type, f"merged_{entity_type}.csv")
        target_path = self.output_staged_dir / target_name

        key_cols = custom_key_columns or ENTITY_NATURAL_KEYS.get(entity_type, ["item_id"])

        con = duckdb.connect()
        con.execute(f"CREATE TEMP TABLE df_primary AS SELECT *, 1 as _v FROM read_csv_auto('{p_path}', ignore_errors=true);")
        con.execute(f"CREATE TEMP TABLE df_supplemental AS SELECT *, 2 as _v FROM read_csv_auto('{s_path}', ignore_errors=true);")

        p_count = con.execute("SELECT count(*) FROM df_primary;").fetchone()[0]
        s_count = con.execute("SELECT count(*) FROM df_supplemental;").fetchone()[0]

        con.execute("CREATE TEMP TABLE combined AS SELECT * FROM df_primary UNION ALL BY NAME SELECT * FROM df_supplemental;")
        all_cols = [c[1].lower() for c in con.execute("PRAGMA table_info('combined');").fetchall()]
        resolved_keys = [k for k in key_cols if k.lower() in all_cols] or [all_cols[0]]
        pk_str = ", ".join(resolved_keys)

        if strategy == "latest_wins":
            con.execute(f"""
                CREATE TEMP TABLE df_merged AS
                WITH ranked AS (
                    SELECT *, ROW_NUMBER() OVER(PARTITION BY {pk_str} ORDER BY _v DESC) as _rn
                    FROM combined
                )
                SELECT * EXCLUDE (_v, _rn) FROM ranked WHERE _rn = 1;
            """)
        elif strategy == "union_distinct":
            con.execute("CREATE TEMP TABLE df_merged AS SELECT DISTINCT * EXCLUDE (_v) FROM combined;")
        else:
            con.execute("CREATE TEMP TABLE df_merged AS SELECT * EXCLUDE (_v) FROM combined;")

        merged_count = con.execute("SELECT count(*) FROM df_merged;").fetchone()[0]
        con.execute(f"COPY df_merged TO '{target_path}' (HEADER, DELIMITER ',');")
        con.close()

        rec = self.tracker.register_file(
            file_path=target_path,
            entity_type=entity_type,
            version_label=f"Merged ({strategy})",
            notes=f"Pairwise merge of {primary_file_id} and {supplemental_file_id}"
        )

        return {
            "status": "SUCCESS",
            "entity_type": entity_type,
            "strategy": strategy,
            "merged_file": target_name,
            "merged_file_path": str(target_path),
            "final_merged_rows": merged_count,
            "duplicates_removed": (p_count + s_count) - merged_count
        }
