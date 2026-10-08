"""
UC Health - Consumption + Contract + Item Master mapping pipeline  (v2.0)
=========================================================================

Enriches the (very large, multi-part) Consumption dataset with
  * standardized vendor names (vendor alias table, exact / core-key / item-overlap / fuzzy matching),
  * UNSPSC from the Item Master,
  * Contract details / price from the Contracts file (Standard Vendor + Item ID + date aware),
  * price validation flags, a full audit trail and a per-row "why could not be mapped" reason,
and does so INCREMENTALLY (state is persisted under ``output/state``).

See README_uc_health_mapping.md for the rules, columns, reason codes and how to run / review.

Business rules (short form):
  * Contract key      : STANDARD vendor name + normalized Item ID (vendor names standardized via
                        ``vendor_alias_table.csv`` across Consumption, Contracts and Item Master).
  * Reference date    : ``consumption_date`` = date part of ADMIT_DATE_TIME.
  * Lookback          : candidates overlap [consumption_date - 6 calendar months, consumption_date].
  * ACTIVE contract   : start <= consumption_date <= end  (null end = open-ended, ASSUMPTION).
  * Selection         : latest contract_start among ACTIVE contracts; ties are resolved by price:
                        one unique price -> MATCHED, several -> AMBIGUOUS_* (no price is ever guessed).
  * Incremental       : unchanged files are skipped; changed contracts / item master / alias table
                        re-map only affected rows (alias-table or logic change = full rebuild).
"""
from __future__ import annotations

import difflib
import hashlib
import json
import logging
import os
import re
import shutil
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

# --------------------------------------------------------------------------------------
# 1. CONFIGURATION
# --------------------------------------------------------------------------------------
PIPELINE_VERSION = "2.1"  # bump when mapping logic changes materially -> invalidates cached mappings

KEY_VENDOR = "vendor_name_standard"      # join key (standardized through the alias table)
KEY_VENDOR_NORM = "vendor_name_normalized"  # trimmed / upper-cased / whitespace-collapsed original
KEY_ITEM = "item_id_normalized"
KEYS = [KEY_VENDOR, KEY_ITEM]

CONSUMPTION_COLUMN_CANDIDATES = {
    "item_id": ["ITEM_NUMBER", "ITEM_ID", "ITEM_NO"],
    "vendor": ["SUPPLIER", "VENDOR_NAME", "VENDOR", "SUPPLIER_NAME"],
    "date": ["ADMIT_DATE_TIME", "ADMIT_DATE", "CONSUMPTION_DATE"],
    "supply_unit_price": ["SUPPLY_UNIT_PRICE", "UNIT_PRICE"],
    "client_contract_price": ["CONTRACT_PRICE", "CLIENT_CONTRACT_PRICE"],
}
CONSUMPTION_REQUIRED = list(CONSUMPTION_COLUMN_CANDIDATES)
CONSUMPTION_CONTEXT_COLUMNS = ["LOG_ID", "FACILITY", "ACCOUNT_NUMBER", "ITEM_DESCRIPTION", "ITEM_UOM",
                               "ITEM_QOE", "TOTAL_QUANTITY", "CONTRACT_FLAG", "MANUFACTURER_NAME"]

CONTRACT_COLUMN_CANDIDATES = {
    "vendor": ["vendor_name", "vendor", "supplier"],
    "item_id": ["item_id", "item_number"],
    "contract_number": ["contract_number"],
    "contract_start": ["contract_start", "contract_start_date"],
    "contract_end": ["contract_end", "contract_end_date"],
    "contract_price": ["contract_price"],
    "contract_ea_price": ["contract_ea_price", "ea_price"],
    "contract_uom": ["contract_uom", "uom"],
    "contract_qoe": ["contract_qoe"],
    "pricing_tier": ["pricing_tier"],
    "contract_category": ["contract_category", "category"],
    "vendor_code": ["vendor_code", "vendor_id"],
}
CONTRACT_REQUIRED = ["vendor", "item_id", "contract_number", "contract_start", "contract_end",
                     "contract_price", "contract_uom"]

ITEM_MASTER_COLUMN_CANDIDATES = {
    "item_id": ["item_id", "item_number"],
    "unspsc": ["unspsc", "unspsc_code"],
    "vendor": ["vendor_name", "vendor", "supplier"],
    "vendor_code": ["vendor_code", "vendor_id"],
}
ITEM_MASTER_REQUIRED = ["item_id", "unspsc"]

# Status vocabularies
ST_MATCHED = "MATCHED"
ST_MISSING_KEY = "MISSING_MAPPING_KEY"
ST_INVALID_DATE = "INVALID_CONSUMPTION_DATE"
ST_NOT_FOUND = "CONTRACT_NOT_FOUND"
ST_NOT_ACTIVE = "CONTRACT_NOT_ACTIVE"
ST_AMBIG_CONTRACTS = "AMBIGUOUS_MULTIPLE_CONTRACTS"
ST_AMBIG_PRICES = "AMBIGUOUS_MULTIPLE_PRICES"
ST_PRICE_MISSING = "CONTRACT_PRICE_MISSING"
CONTRACT_STATUSES = [ST_MATCHED, ST_MISSING_KEY, ST_INVALID_DATE, ST_NOT_FOUND, ST_NOT_ACTIVE,
                     ST_AMBIG_CONTRACTS, ST_AMBIG_PRICES, ST_PRICE_MISSING]
IM_MATCHED, IM_NOT_FOUND, IM_AMBIG = "MATCHED", "ITEM_NOT_FOUND", "AMBIGUOUS_UNSPSC"
IM_MISSING_ID, IM_UNSPSC_MISSING = "MISSING_ITEM_ID", "UNSPSC_MISSING"
FLAG_WITHIN, FLAG_NA = "WITHIN_THRESHOLD", "NOT_COMPARABLE"

# Reason codes -> (which key / data element is the problem, plain-English meaning)
CODE_INFO = {
    "MISSING_ITEM_ID": ("Item ID missing", "Consumption row has a blank Item ID"),
    "MISSING_VENDOR": ("Vendor missing", "Consumption row has a blank Supplier"),
    "MISSING_ITEM_AND_VENDOR": ("Item ID and Vendor missing", "Consumption row has blank Item ID and blank Supplier"),
    "INVALID_CONSUMPTION_DATE": ("Admit date missing/invalid", "Admit date is blank or cannot be parsed"),
    "VENDOR_AND_ITEM_NOT_IN_CONTRACTS": ("Vendor and Item ID both not matched", "Neither the (standard) vendor nor the Item ID exists anywhere in Contracts"),
    "VENDOR_NOT_IN_CONTRACTS": ("Vendor did not match", "Item ID exists in Contracts (under other vendors) but the standard vendor has no contracts - alias/vendor-name issue or different vendor"),
    "ITEM_NOT_IN_CONTRACTS": ("Item ID did not match", "Vendor has contracts but this Item ID is not contracted with any vendor"),
    "VENDOR_ITEM_PAIR_NOT_IN_CONTRACTS": ("Vendor+Item combination not contracted", "Vendor and Item ID both exist in Contracts, but never together (item is contracted under another vendor)"),
    "NOT_ACTIVE_EXPIRED_WITHIN_LOOKBACK": ("Contract dates", "Vendor+Item contract exists but expired before the consumption date (within the 6-month lookback)"),
    "NOT_ACTIVE_EXPIRED_BEFORE_LOOKBACK": ("Contract dates", "Vendor+Item contract exists but expired more than 6 months before the consumption date"),
    "NOT_ACTIVE_NOT_YET_STARTED": ("Contract dates", "Vendor+Item contract exists but starts after the consumption date"),
    "NOT_ACTIVE_INVALID_START_DATE": ("Contract dates", "Vendor+Item contract exists but its start date is missing/invalid"),
    "NOT_ACTIVE_NO_END_DATE": ("Contract dates", "Vendor+Item contract has no end date and open-ended handling is off"),
    "AMBIGUOUS_MULTIPLE_CONTRACTS": ("Multiple contracts, different prices", "Several active contracts tie on latest start date with different prices"),
    "AMBIGUOUS_MULTIPLE_PRICES": ("Multiple prices", "One contract carries several different prices for the latest start date"),
    "CONTRACT_PRICE_MISSING": ("Contract price missing", "Active contract has no price"),
    "ITEM_NOT_IN_ITEM_MASTER": ("Item ID did not match", "Item ID not found in Item Master"),
    "ITEM_NOT_IN_ITEM_MASTER_NONNUMERIC_ID": ("Item ID did not match", "Item ID is non-numeric (internal/non-catalog style) and not found in Item Master"),
    "ITEM_NOT_IN_ITEM_MASTER_LEADING_ZEROS": ("Item ID format", "Item ID not found as-is, but exists in Item Master after removing leading zeros"),
    "UNSPSC_MISSING_IN_ITEM_MASTER": ("UNSPSC blank in Item Master", "Item is in Item Master but has no UNSPSC"),
    "AMBIGUOUS_UNSPSC": ("Multiple UNSPSC", "Item ID has more than one distinct UNSPSC in Item Master"),
}

FAR_PAST = pd.Timestamp("1700-01-01")
FAR_FUTURE = pd.Timestamp("2200-01-01")


@dataclass
class Config:
    """All run parameters. ``fingerprint()`` lists the ones that change mapping RESULTS."""
    base_dir: Path = field(default_factory=Path.cwd)
    input_dir: Optional[Path] = None
    output_dir: Optional[Path] = None
    alias_path: Optional[Path] = None
    consumption_glob: str = "UHC_Consumption_Part_*.csv"
    contracts_file: str = "UHC_Contracts.csv"
    item_master_file: str = "UHC_Item_Master.csv"
    chunk_rows: int = 250_000
    lookback_months: int = 6
    price_threshold: float = 0.20
    # each-level price: SUPPLY_UNIT_PRICE is per each. contract_price / UOM are exposed as well.
    validation_price_field: str = "contract_ea_price"
    null_end_date_open_ended: bool = True   # ASSUMPTION: blank contract end = open-ended
    hash_files: bool = True
    write_final_csv: bool = True
    csv_rows_per_part: int = 1_000_000
    # vendor alias thresholds
    alias_min_item_share: float = 0.60      # share of a vendor's contracted items found under the candidate vendor
    alias_strong_item_share: float = 0.90   # share required when the names share no word ...
    alias_strong_min_items: int = 10        # ... and at least this many contracted Item IDs
    alias_fuzzy_min_ratio: float = 0.94
    alias_hash: str = ""                    # set at run time (hash of the effective alias table)
    as_of_date: Optional[str] = None        # "YYYY-MM-DD"; date used for the CURRENT contract price (default: today)

    def __post_init__(self):
        self.base_dir = Path(self.base_dir)
        self.input_dir = Path(self.input_dir) if self.input_dir else self.base_dir
        self.output_dir = Path(self.output_dir) if self.output_dir else self.base_dir / "output"
        self.alias_path = Path(self.alias_path) if self.alias_path else self.base_dir / "vendor_alias_table.csv"
        assert self.validation_price_field in ("contract_ea_price", "contract_price")

    @property
    def final_dir(self): return self.output_dir / "final"
    @property
    def exc_dir(self): return self.output_dir / "exceptions"
    @property
    def summary_dir(self): return self.output_dir / "summary"
    @property
    def state_dir(self): return self.output_dir / "state"
    @property
    def log_dir(self): return self.output_dir / "logs"
    @property
    def proc_dir(self): return self.state_dir / "consumption_processed"
    @property
    def staging_dir(self): return self.state_dir / "_staging"

    @property
    def as_of(self) -> pd.Timestamp:
        return pd.Timestamp(self.as_of_date) if self.as_of_date else pd.Timestamp.today().normalize()

    @property
    def above_label(self) -> str:
        return f"ABOVE_{int(round(self.price_threshold * 100))}_PERCENT"

    def fingerprint(self) -> dict:
        return {"pipeline_version": PIPELINE_VERSION, "chunk_rows": self.chunk_rows,
                "lookback_months": self.lookback_months, "price_threshold": self.price_threshold,
                "validation_price_field": self.validation_price_field,
                "null_end_date_open_ended": self.null_end_date_open_ended}

    def make_dirs(self):
        for d in (self.final_dir, self.exc_dir, self.summary_dir, self.state_dir, self.log_dir, self.proc_dir):
            d.mkdir(parents=True, exist_ok=True)


# --------------------------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------------------------
LOG = logging.getLogger("uc_mapping")


def setup_logging(cfg: Config) -> logging.Logger:
    """File log (output/logs/pipeline.log) + concise console progress."""
    cfg.log_dir.mkdir(parents=True, exist_ok=True)
    LOG.setLevel(logging.INFO)
    for h in list(LOG.handlers):
        LOG.removeHandler(h)
        h.close()
    fh = logging.FileHandler(cfg.log_dir / "pipeline.log", encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(message)s", "%Y-%m-%d %H:%M:%S"))
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(logging.Formatter("%(asctime)s | %(message)s", "%H:%M:%S"))
    LOG.addHandler(fh)
    LOG.addHandler(sh)
    LOG.propagate = False
    return LOG


# --------------------------------------------------------------------------------------
# Small utilities
# --------------------------------------------------------------------------------------
def _canon(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(s).strip().lower()).strip("_")


def detect_columns(header: List[str], candidates: Dict[str, List[str]], required: List[str],
                   dataset: str) -> Dict[str, Optional[str]]:
    """Map logical fields to the physical column names found in ``header`` (never guesses silently)."""
    canon = {_canon(h): h for h in header}
    found = {logical: next((canon[_canon(c)] for c in cands if _canon(c) in canon), None)
             for logical, cands in candidates.items()}
    missing = [k for k in required if found[k] is None]
    if missing:
        raise ValueError(f"{dataset}: could not detect required columns {missing}; header={header}")
    return found


def _atomic_replace(tmp: Path, final: Path):
    final.parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(30):  # Windows: AV / indexer may briefly lock a freshly written file
        try:
            os.replace(tmp, final)
            return
        except PermissionError:
            if attempt == 29:
                raise
            time.sleep(1.0)


def write_json_atomic(obj, path: Path):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2, default=str), encoding="utf-8")
    _atomic_replace(tmp, path)


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def write_csv_atomic(df: pd.DataFrame, path: Path, **kw):
    tmp = path.with_suffix(".csv.tmp")
    df.to_csv(tmp, index=False, date_format="%Y-%m-%d", **kw)
    _atomic_replace(tmp, path)


def file_sha256(path: Path, block: int = 16 * 1024 * 1024) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while chunk := fh.read(block):
            h.update(chunk)
    return h.hexdigest()


def natural_key(p: Path):
    return [int(t) if t.isdigit() else t.lower() for t in re.split(r"(\d+)", p.name)]


# --------------------------------------------------------------------------------------
# 2. INPUT DISCOVERY + MANIFEST
# --------------------------------------------------------------------------------------
def discover_input_files(cfg: Config) -> dict:
    """Find all Consumption parts (any number), Contracts and Item Master."""
    parts = sorted(cfg.input_dir.glob(cfg.consumption_glob), key=natural_key)
    con = cfg.input_dir / cfg.contracts_file
    im = cfg.input_dir / cfg.item_master_file
    for p in (con, im):
        if not p.exists():
            raise FileNotFoundError(p)
    if not parts:
        raise FileNotFoundError(f"No files match {cfg.consumption_glob} in {cfg.input_dir}")
    return {"consumption": parts, "contracts": con, "item_master": im}


def build_file_manifest(paths: List[Path], prev_files: dict, compute_hash: bool = True) -> dict:
    """Manifest entry per file. sha256 is only recomputed when size/mtime differ from the previous manifest.
    status is NEW / CHANGED / UNCHANGED."""
    out = {}
    for p in paths:
        st = p.stat()
        prev = prev_files.get(p.name)
        entry = {"size_bytes": st.st_size, "mtime_ns": st.st_mtime_ns,
                 "modified_ts": datetime.fromtimestamp(st.st_mtime).isoformat(timespec="seconds")}
        if prev is None:
            entry["sha256"] = file_sha256(p) if compute_hash else None
            entry["status"] = "NEW"
        elif prev.get("size_bytes") == st.st_size and prev.get("mtime_ns") == st.st_mtime_ns:
            entry["sha256"] = prev.get("sha256")
            entry["status"] = "UNCHANGED"
        else:
            entry["sha256"] = file_sha256(p) if compute_hash else None
            same = compute_hash and entry["sha256"] == prev.get("sha256") and prev.get("size_bytes") == st.st_size
            entry["status"] = "UNCHANGED" if same else "CHANGED"
        out[p.name] = entry
    return out


# --------------------------------------------------------------------------------------
# 5. NORMALIZATION
# --------------------------------------------------------------------------------------
def _map_unique(s: pd.Series, fn) -> pd.Series:
    """Apply ``fn`` once per distinct value (fast for high-repeat columns)."""
    mapping = {v: fn(v) for v in pd.unique(s)}
    return s.map(mapping)


_WS = re.compile(r"\s+")


def normalize_vendor(s: pd.Series) -> pd.Series:
    """trim + uppercase + collapse repeated whitespace. Blank stays ''."""
    return _map_unique(s.fillna("").astype(str), lambda v: _WS.sub(" ", v.strip().upper()))


def normalize_item_id(s: pd.Series) -> pd.Series:
    """String only, trimmed. Leading zeros and non-numeric IDs are preserved (never cast to numeric)."""
    return s.fillna("").astype(str).str.strip()


def parse_dates(s: pd.Series) -> Tuple[pd.Series, pd.Series]:
    """Robust date parse -> (normalized datetime64 date, status VALID/MISSING/INVALID). Never raises."""
    txt = s.fillna("").astype(str).str.strip()
    d = pd.to_datetime(txt.str.slice(0, 10), format="%Y-%m-%d", errors="coerce")
    bad = d.isna() & (txt != "")
    if bad.any():
        try:
            d2 = pd.to_datetime(txt[bad], errors="coerce", format="mixed")
            if getattr(d2.dt, "tz", None) is not None:
                d2 = d2.dt.tz_localize(None)
            d = d.copy()
            d[bad] = d2
        except Exception:  # pragma: no cover
            pass
    d = d.dt.normalize()
    status = pd.Series(np.where(txt == "", "MISSING", np.where(d.isna(), "INVALID", "VALID")), index=s.index)
    return d, status


def to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.fillna("").astype(str).str.strip().str.replace(",", "", regex=False), errors="coerce")


def hash_rows(tbl: pa.Table) -> np.ndarray:
    """Deterministic uint64 hash per row over ALL raw columns (used to detect changed rows)."""
    h = np.zeros(tbl.num_rows, dtype=np.uint64)
    mult = np.uint64(1000003)
    for name in tbl.column_names:
        col = tbl.column(name).to_pandas().to_numpy(dtype=object)
        h = (h * mult) ^ pd.util.hash_array(col)
    return h


def normalize_contracts(raw: pd.DataFrame, cols: Dict[str, Optional[str]], cfg: Config,
                        alias: Optional[Dict[str, str]] = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Normalize the Contracts file. Returns (all_records, deduplicated_records_used_for_matching).
    Vendor names are standardized through ``alias``. Unparseable dates become NaT and are flagged."""
    alias = alias or {}
    g = lambda k: raw[cols[k]] if cols.get(k) else pd.Series("", index=raw.index)
    out = pd.DataFrame(index=raw.index)
    out["vendor_name_original"] = g("vendor").fillna("")
    out[KEY_VENDOR_NORM] = normalize_vendor(out["vendor_name_original"])
    out[KEY_VENDOR] = _map_unique(out[KEY_VENDOR_NORM], lambda v: alias.get(v, v))
    out[KEY_ITEM] = normalize_item_id(g("item_id"))
    out["contract_number"] = g("contract_number").fillna("").astype(str).str.strip()
    out["contract_start"], out["start_status"] = parse_dates(g("contract_start"))
    out["contract_end"], out["end_status"] = parse_dates(g("contract_end"))
    out["contract_uom"] = g("contract_uom").fillna("").astype(str).str.strip().str.upper()
    out["contract_price"] = to_num(g("contract_price"))
    out["contract_ea_price"] = to_num(g("contract_ea_price"))
    out["contract_qoe"] = to_num(g("contract_qoe"))
    out["pricing_tier"] = g("pricing_tier").fillna("").astype(str).str.strip().str.upper()
    out["contract_category"] = g("contract_category").fillna("").astype(str).str.strip()
    rec_cols = KEYS + ["contract_number", "contract_start", "contract_end", "contract_uom",
                       "contract_price", "contract_ea_price", "pricing_tier"]
    out["is_duplicate"] = out.duplicated(rec_cols, keep="first")
    usable = (out[KEY_VENDOR] != "") & (out[KEY_ITEM] != "")
    dedup = out.loc[~out["is_duplicate"] & usable].copy().reset_index(drop=True)
    hp = pd.DataFrame({
        "a": dedup[KEY_VENDOR], "b": dedup[KEY_ITEM], "c": dedup["contract_number"], "d": dedup["contract_uom"],
        "e": dedup["contract_start"].astype("int64"), "f": dedup["contract_end"].astype("int64"),
        "g": dedup["contract_price"].round(6), "h": dedup["contract_ea_price"].round(6),
        "i": dedup["pricing_tier"]})
    dedup["rec_hash"] = pd.util.hash_pandas_object(hp, index=False).to_numpy()
    return out, dedup


def normalize_item_master(raw: pd.DataFrame, cols: Dict[str, Optional[str]]) -> pd.DataFrame:
    """One row per Item ID (index) with distinct non-blank UNSPSC values and a status.
    An Item ID with >1 DISTINCT UNSPSC is AMBIGUOUS_UNSPSC (never picks one)."""
    d = pd.DataFrame({KEY_ITEM: normalize_item_id(raw[cols["item_id"]]),
                      "unspsc": raw[cols["unspsc"]].fillna("").astype(str).str.strip()})
    d = d[d[KEY_ITEM] != ""]
    nz = d[d["unspsc"] != ""].drop_duplicates()
    agg = nz.groupby(KEY_ITEM)["unspsc"].agg(lambda x: "|".join(sorted(x)))
    n = nz.groupby(KEY_ITEM)["unspsc"].nunique()
    im = pd.DataFrame(index=pd.Index(d[KEY_ITEM].unique(), name=KEY_ITEM))
    im["unspsc_candidates"] = agg.reindex(im.index)
    im["n_unspsc"] = n.reindex(im.index).fillna(0).astype(int)
    im["unspsc"] = im["unspsc_candidates"].where(im["n_unspsc"] == 1)
    im["item_master_match_status"] = np.select([im["n_unspsc"] == 0, im["n_unspsc"] == 1],
                                               [IM_UNSPSC_MISSING, IM_MATCHED], default=IM_AMBIG)
    im["item_master_rows"] = d.groupby(KEY_ITEM).size().reindex(im.index).astype(int)
    return im


# --------------------------------------------------------------------------------------
# VENDOR STANDARDIZATION (alias table)
# --------------------------------------------------------------------------------------
LEGAL_TOKENS = {"INC", "INCORPORATED", "CORP", "CORPORATION", "LLC", "LP", "LLP", "LTD", "LIMITED", "CO",
                "COMPANY", "PLC", "GMBH", "THE", "USA", "US", "NA", "OF"}
GENERIC_TOKENS = LEGAL_TOKENS | {
    "MEDICAL", "SURGICAL", "HEALTH", "HEALTHCARE", "SYSTEMS", "SYSTEM", "PRODUCTS", "LABORATORIES", "INDUSTRIES",
    "TECHNOLOGIES", "TECHNOLOGY", "SCIENTIFIC", "SUPPLY", "INTERNATIONAL", "GROUP", "HOLDINGS", "DEVICES",
    "SOLUTIONS", "AMERICA", "AND", "SALES", "SERVICES", "DENTAL", "INSTRUMENTS", "SPECIALTIES", "BIOMEDICAL",
    "LIFESCIENCES", "WORLD", "WIDE", "WORLDWIDE", "DIVISION", "CARE", "PHARMA", "PHARMACEUTICALS"}
ALIAS_COLUMNS = ["alias_name", "standard_vendor", "method", "confidence", "evidence", "needs_review",
                 "reviewer_standard_vendor", "consumption_rows", "contract_records", "item_master_rows", "datasets",
                 "vendor_code_check", "vendor_code_evidence", "review_status"]


def vendor_core(name: str) -> str:
    """Punctuation-free name without legal-form words (INC, LLC, CORP ...). Used for the core-key match."""
    s = re.sub(r"[^A-Z0-9]+", " ", str(name).upper().replace("&", " AND "))
    toks = [t for t in s.split() if t not in LEGAL_TOKENS]
    return " ".join(toks) if toks else s.strip()


def vendor_sig_tokens(name: str) -> set:
    return {t for t in vendor_core(name).split() if t not in GENERIC_TOKENS and len(t) > 2}


def cluster_contract_vendors(contract_records: pd.Series, seed: Dict[str, str]):
    """Stage 1: Contracts vendor spellings -> standard vendor. Legal-form variants (INC/LLC/CORP ...) collapse to the
    most frequent spelling. Vendors already in the alias table (``seed``) keep their standard name."""
    std: Dict[str, str] = dict(seed)
    meta: Dict[str, tuple] = {}
    core_std = {vendor_core(s): s for s in set(seed.values())}
    by_core: Dict[str, List[str]] = {}
    for n in contract_records.index:
        if n not in std:
            by_core.setdefault(vendor_core(n), []).append(n)
    for core, mem in by_core.items():
        top = core_std.get(core) or sorted(mem, key=lambda x: (-int(contract_records.get(x, 0)), x))[0]
        core_std.setdefault(core, top)
        for n in mem:
            std[n] = top
            meta[n] = ("EXACT" if (n == top and len(mem) == 1) else "CONTRACT_VENDOR_ANCHOR" if n == top else "CORE_KEY",
                       1.0, "" if n == top else f"same name as '{top}' after removing punctuation / legal words")
    return std, meta, core_std


def vendor_code_evidence(vendor_items: pd.DataFrame, im_codes: pd.DataFrame, con_codes: pd.DataFrame,
                         code_to_std: Dict[str, str]) -> Dict[str, dict]:
    """Vendor-code evidence per Consumption spelling. Consumption has no vendor code column, so the code is derived
    from the Item IDs the spelling consumed: Item Master ``vendor_code`` first (the item's primary vendor), Contracts
    ``vendor_code`` as fallback. Result: dominant code (by consumption rows), the Contracts vendor that owns that code,
    its share of coded rows and the number of supporting Item IDs."""
    out: Dict[str, dict] = {}
    for src, codes in (("IM", im_codes), ("CON", con_codes)):
        m = vendor_items.merge(codes, left_on="i", right_on="item_id")
        if m.empty:
            continue
        g = m.groupby(["v", "vendor_code"]).agg(r=("rows", "sum"), ni=("item_id", "nunique")).reset_index()
        tot = g.groupby("v")["r"].transform("sum")
        g["share"] = g["r"] / tot
        top = g.sort_values(["v", "r"], ascending=[True, False]).drop_duplicates("v")
        n_items = vendor_items.groupby("v")["i"].nunique()
        for r in top.itertuples():
            if r.v in out:
                continue
            out[r.v] = {"src": src, "code": r.vendor_code, "vendor": code_to_std.get(r.vendor_code),
                        "share": float(r.share), "items": int(r.ni), "n_items": int(n_items.get(r.v, 0))}
    return out


def code_accepts(ev: Optional[dict], name: str, min_share: float = 0.8) -> bool:
    """Is the vendor-code evidence strong enough to decide the vendor on its own?
    Item Master code: dominant code >= 80% of coded rows. Contracts-only fallback additionally needs >= 3 Item IDs
    or a shared distinctive word in the names."""
    if not ev or not ev.get("vendor") or ev["share"] < min_share:
        return False
    if ev["src"] == "IM":
        return True
    return ev["items"] >= 3 or bool(vendor_sig_tokens(name) & vendor_sig_tokens(ev["vendor"]))


def resolve_vendor_aliases(unknown: List[str], std: Dict[str, str], meta: Dict[str, tuple], core_std: Dict[str, str],
                           item_sets: Dict[str, set], con_items: Dict[str, set], code_ev: Dict[str, dict],
                           im_name_vendor: Dict[str, str], cfg: Config) -> pd.DataFrame:
    """Resolve every name in ``unknown`` to a standard vendor name (always a Contracts vendor name).

    Order of evidence (first hit wins), recorded in ``method``:
      EXACT / CONTRACT_VENDOR_ANCHOR  spelling exists in Contracts (legal-form variants collapse to the most frequent spelling)
      CORE_KEY      same name after dropping punctuation and legal words (INC / LLC / CORP ...)
      VENDOR_CODE   Item Master vendor name -> its vendor code -> Contracts vendor with that code; or (Consumption) the
                    dominant Item-Master/Contracts vendor code of the Item IDs the spelling consumed
      ITEM_OVERLAP  its Item IDs are (mostly) contracted under one Contracts vendor AND names share a distinctive word
                    (or the overlap is very strong)
      FUZZY_NAME    string similarity >= cfg.alias_fuzzy_min_ratio with a clear margin
      NO_MATCH      kept as its own name (a vendor that genuinely has no contracts)
    Contracts vendors that exist verbatim are never re-mapped to a different vendor."""
    std = dict(std)
    meta = dict(meta)
    core_of: Dict[str, str] = {}
    getc = lambda n: core_of.setdefault(n, vendor_core(n))
    item_std: Dict[str, set] = {i: {std.get(v, v) for v in vs} for i, vs in con_items.items()}
    std_vendors = sorted(set(std.values()))
    std_tokens = {s: vendor_sig_tokens(s) for s in std_vendors}
    for n in unknown:
        if n in meta:
            continue
        c = getc(n)
        if c in core_std:
            std[n] = core_std[c]
            meta[n] = ("CORE_KEY", 1.0, f"same name as '{core_std[c]}' after removing punctuation / legal words")
            continue
        if n in im_name_vendor:
            std[n] = im_name_vendor[n]
            meta[n] = ("VENDOR_CODE", 1.0, f"Item Master vendor code of this vendor belongs to Contracts vendor '{im_name_vendor[n]}'")
            continue
        ev = code_ev.get(n)
        if code_accepts(ev, n):
            std[n] = ev["vendor"]
            meta[n] = ("VENDOR_CODE", round(ev["share"], 3),
                       f"vendor code {ev['code']} ({ev['src']}) = '{ev['vendor']}' for {ev['share']:.0%} of consumed rows ({ev['items']} of {ev['n_items']} Item IDs)")
            continue
        cnt, n_in = Counter(), 0
        for i in item_sets.get(n, ()):
            vs = item_std.get(i)
            if vs:
                n_in += 1
                cnt.update(vs)
        ntok = vendor_sig_tokens(n)
        best = cnt.most_common(2)
        if n_in >= 2 and best:
            v1, c1 = best[0]
            c2 = best[1][1] if len(best) > 1 else 0
            share = c1 / n_in
            token_hit = bool(ntok & std_tokens.get(v1, set()))
            strong = ((share >= cfg.alias_strong_item_share and n_in >= cfg.alias_strong_min_items) or
                      (share >= 0.80 and n_in >= 50))
            if share >= cfg.alias_min_item_share and c1 >= 1.5 * c2 and (token_hit or strong):
                std[n] = v1
                meta[n] = ("ITEM_OVERLAP", round(share, 3),
                           f"{c1} of {n_in} contracted Item IDs sit under '{v1}'" + ("" if token_hit else " (names share no word)"))
                continue
        cands = []
        for s in std_vendors:
            sm = difflib.SequenceMatcher(None, c, getc(s))
            if sm.real_quick_ratio() >= 0.8 and sm.quick_ratio() >= 0.8:
                cands.append((sm.ratio(), s))
        cands.sort(reverse=True)
        if cands and cands[0][0] >= cfg.alias_fuzzy_min_ratio and (len(cands) == 1 or cands[0][0] - cands[1][0] >= 0.03):
            std[n] = cands[0][1]
            meta[n] = ("FUZZY_NAME", round(cands[0][0], 3), f"name similarity {cands[0][0]:.2f} to '{cands[0][1]}'")
            continue
        std[n] = n
        why = (f"no contracts vendor with matching name; {cnt.most_common(1)[0][1]} of {n_in} contracted Item IDs sit under '{cnt.most_common(1)[0][0]}' (below threshold)"
               if cnt else "no contracts vendor with matching name and none of its Item IDs are in Contracts")
        meta[n] = ("NO_MATCH", 0.0, why)
    rows = [{"alias_name": n, "standard_vendor": std[n], "method": meta[n][0], "confidence": meta[n][1],
             "evidence": meta[n][2]} for n in dict.fromkeys(unknown) if n in meta]
    return pd.DataFrame(rows, columns=["alias_name", "standard_vendor", "method", "confidence", "evidence"])


_APPROVE_RE = re.compile(r"\b(ok|okay|approved?|accept(ed)?|agree[ds]?|confirmed?)\b", re.I)
_CHECKCODE_RE = re.compile(r"vendor\s*(code|id)", re.I)


def interpret_reviewer(text, valid_names: set) -> Tuple[str, Optional[str]]:
    """Column ``reviewer_standard_vendor`` accepts a vendor name (override) OR a free-text comment.
    Returns (kind, vendor): OVERRIDE(name) / APPROVED / CHECK_CODE / COMMENT / NONE. A comment is never used as a name."""
    t = str(text or "").strip()
    if not t:
        return "NONE", None
    u = _WS.sub(" ", t.upper())
    if u in valid_names:
        return "OVERRIDE", u
    if _APPROVE_RE.search(t):
        return "APPROVED", None
    if _CHECKCODE_RE.search(t):
        return "CHECK_CODE", None
    return "COMMENT", None


def effective_alias(tbl: pd.DataFrame, valid_names: set) -> Dict[str, str]:
    """alias_name -> standard vendor; a reviewer OVERRIDE (a valid vendor name in reviewer_standard_vendor) always wins.
    Free-text comments in that column are ignored here."""
    out = {}
    for a, s, r in zip(tbl["alias_name"], tbl["standard_vendor"], tbl["reviewer_standard_vendor"]):
        kind, val = interpret_reviewer(r, valid_names)
        out[a] = val if kind == "OVERRIDE" else s
    return out


def apply_reviewer_decisions(tbl: pd.DataFrame, valid_names: set, code_ev: Dict[str, dict]) -> pd.DataFrame:
    """Act on the reviewer's column-G entries and cross-check every judgement-based alias against vendor codes.
      APPROVED    keep the proposed standard vendor; record whether the vendor code CONFIRMS or CONFLICTS with it
      CHECK_CODE  decide by vendor code: accepted -> standard vendor = code vendor (method VENDOR_CODE), else stays as is
      OVERRIDE    reviewer typed a vendor name -> used as is
      COMMENT     free text without a decision keyword -> ignored (listed for follow-up)"""
    tbl = tbl.copy()
    for col in ("vendor_code_check", "vendor_code_evidence", "review_status"):
        tbl[col] = tbl[col].fillna("").astype(str) if col in tbl.columns else ""
    judged = {"ITEM_OVERLAP", "FUZZY_NAME", "NO_MATCH", "VENDOR_CODE"}
    for idx, r in tbl.iterrows():
        kind, val = interpret_reviewer(r["reviewer_standard_vendor"], valid_names)
        ev = code_ev.get(r["alias_name"])
        status, check, evtxt = {"NONE": "", "COMMENT": "COMMENT_ONLY", "OVERRIDE": "OVERRIDE", "APPROVED": "APPROVED",
                                "CHECK_CODE": "CODE_CHECK_PENDING"}[kind], "", ""
        if kind == "CHECK_CODE":
            if code_accepts(ev, r["alias_name"]):
                tbl.at[idx, "standard_vendor"] = ev["vendor"]
                tbl.at[idx, "method"] = "VENDOR_CODE"
                tbl.at[idx, "confidence"] = round(ev["share"], 3)
                tbl.at[idx, "evidence"] = (f"vendor code {ev['code']} ({ev['src']}) = '{ev['vendor']}' for {ev['share']:.0%} of consumed rows "
                                           f"({ev['items']} of {ev['n_items']} Item IDs)")
                status = "DECIDED_BY_VENDOR_CODE"
            else:
                status = "NO_VENDOR_CODE_EVIDENCE" if not ev else "VENDOR_CODE_EVIDENCE_TOO_WEAK"
        if r["method"] in judged or tbl.at[idx, "method"] in judged:
            std_now = val if kind == "OVERRIDE" else tbl.at[idx, "standard_vendor"]
            if not ev or not ev.get("vendor"):
                check, evtxt = "NO_CODE_DATA", "no Item Master / Contracts vendor code found for the Item IDs of this spelling"
            else:
                evtxt = (f"code {ev['code']} ({ev['src']}) -> '{ev['vendor']}': {ev['share']:.0%} of consumed rows, "
                         f"{ev['items']} of {ev['n_items']} Item IDs")
                if ev["vendor"] == std_now and ev["share"] >= 0.5:
                    check = "CONFIRMED"
                elif ev["share"] >= 0.5:
                    check = "CONFLICT"
                else:
                    check = "INCONCLUSIVE"
            if kind == "APPROVED" and check == "CONFLICT":
                status = "APPROVED_BUT_CODE_CONFLICT"
        tbl.at[idx, "vendor_code_check"], tbl.at[idx, "vendor_code_evidence"], tbl.at[idx, "review_status"] = check, evtxt, status
    return tbl


def alias_hash(mapping: Dict[str, str]) -> str:
    return hashlib.sha256(json.dumps(sorted(mapping.items())).encode()).hexdigest()[:16]


def scan_consumption_vendor_items(path: Path, header: List[str], cols: Dict[str, str]) -> pd.DataFrame:
    """Distinct (normalized vendor, item id) with row counts from just the two key columns of a CSV."""
    conv = pacsv.ConvertOptions(include_columns=[cols["vendor"], cols["item_id"]],
                                column_types={cols["vendor"]: pa.string(), cols["item_id"]: pa.string()})
    rd = pacsv.open_csv(path, read_options=pacsv.ReadOptions(block_size=64 << 20),
                        parse_options=pacsv.ParseOptions(newlines_in_values=True, delimiter="|"), convert_options=conv)
    parts = []
    for b in rd:
        df = b.to_pandas()
        df.columns = ["v", "i"]
        parts.append(df.groupby(["v", "i"], dropna=False).size().rename("rows").reset_index())
    d = pd.concat(parts).groupby(["v", "i"], dropna=False)["rows"].sum().reset_index()
    d["v"] = normalize_vendor(d["v"])
    d["i"] = normalize_item_id(d["i"])
    return d.groupby(["v", "i"])["rows"].sum().reset_index()


# --------------------------------------------------------------------------------------
# 6. CHANGE DETECTION (Contracts / Item Master)
# --------------------------------------------------------------------------------------
def detect_contract_changes(prev: Optional[pd.DataFrame], cur: pd.DataFrame, cfg: Config) -> Optional[pd.DataFrame]:
    """Compare normalized contract snapshots. One row per affected Vendor+Item key with the Consumption-date
    window [lo, hi] that can be influenced (a contract valid [start, end] is a candidate for dates in
    [start, end + lookback]). A key that appears/disappears entirely gets an unbounded window.
    Returns None when there is no previous snapshot."""
    if prev is None:
        return None
    added = cur[~cur["rec_hash"].isin(prev["rec_hash"])]
    removed = prev[~prev["rec_hash"].isin(cur["rec_hash"])]
    ch = pd.concat([added, removed], ignore_index=True)
    if ch.empty:
        return pd.DataFrame(columns=KEYS + ["lo", "hi"])
    ends = pd.DatetimeIndex(ch["contract_end"].dropna().unique())
    if len(ends):
        hi_map = pd.Series(ends + pd.DateOffset(months=cfg.lookback_months), index=ends)
        ch["hi"] = ch["contract_end"].map(hi_map).fillna(FAR_FUTURE)
    else:
        ch["hi"] = FAR_FUTURE
    ch["lo"] = ch["contract_start"].fillna(FAR_PAST)
    w = ch.groupby(KEYS, as_index=False).agg(lo=("lo", "min"), hi=("hi", "max"))
    pk = prev[KEYS].drop_duplicates().assign(_p=True)
    ck = cur[KEYS].drop_duplicates().assign(_c=True)
    flip = pk.merge(ck, on=KEYS, how="outer")
    flip = flip[flip["_p"].isna() | flip["_c"].isna()][KEYS]
    if len(flip):
        w = w.merge(flip.assign(_flip=True), on=KEYS, how="left")
        w.loc[w["_flip"].notna(), ["lo", "hi"]] = [FAR_PAST, FAR_FUTURE]
        w = w.drop(columns="_flip")
    w["lo"], w["hi"] = pd.to_datetime(w["lo"]), pd.to_datetime(w["hi"])
    return w


def detect_item_master_changes(prev: Optional[pd.DataFrame], cur: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Item IDs whose UNSPSC / status was added, changed or removed. None when no previous snapshot."""
    if prev is None:
        return None
    a = prev[["unspsc_candidates", "item_master_match_status"]].add_prefix("prev_")
    b = cur[["unspsc_candidates", "item_master_match_status"]].add_prefix("cur_")
    m = a.join(b, how="outer")
    in_prev, in_cur = m["prev_item_master_match_status"].notna(), m["cur_item_master_match_status"].notna()
    same = (m["prev_unspsc_candidates"].fillna("") == m["cur_unspsc_candidates"].fillna("")) & \
           (m["prev_item_master_match_status"] == m["cur_item_master_match_status"])
    m["change"] = np.select([~in_prev, ~in_cur, ~same], ["ADDED", "REMOVED", "CHANGED"], default="")
    return m[m["change"] != ""].reset_index()[[KEY_ITEM, "change", "prev_unspsc_candidates", "cur_unspsc_candidates"]]


# --------------------------------------------------------------------------------------
# 7-9. MAPPING (Item Master, Contracts, Price validation)
# --------------------------------------------------------------------------------------
BASE_SCHEMA = [
    ("source_file", pa.string()), ("source_row_number", pa.int64()), ("row_id", pa.string()),
    ("row_hash", pa.uint64()), (KEY_ITEM, pa.string()), ("vendor_name_original", pa.string()),
    (KEY_VENDOR_NORM, pa.string()), (KEY_VENDOR, pa.string()), ("consumption_date", pa.timestamp("ns")),
    ("consumption_date_status", pa.string()), ("supply_unit_price", pa.float64()),
    ("client_contract_price", pa.float64()), ("row_eligibility", pa.string())]
UNSPSC_SCHEMA = [("mapped_unspsc", pa.string()), ("item_master_match_status", pa.string()),
                 ("item_master_gap_code", pa.string()), ("item_master_gap_detail", pa.string())]
CONTRACT_SCHEMA = [
    ("contract_match_status", pa.string()), ("contract_selection_reason", pa.string()),
    ("lookback_start_date", pa.timestamp("ns")), ("mapped_contract_number", pa.string()),
    ("mapped_contract_start_date", pa.timestamp("ns")), ("mapped_contract_end_date", pa.timestamp("ns")),
    ("mapped_contract_price", pa.float64()), ("mapped_contract_ea_price", pa.float64()),
    ("mapped_contract_uom", pa.string()), ("contract_candidate_count", pa.int32()),
    ("active_contract_count", pa.int32()), ("tied_contract_count", pa.int32()),
    ("unique_contract_price_count", pa.int32()), ("closest_contract_number", pa.string()),
    ("closest_contract_start_date", pa.timestamp("ns")), ("closest_contract_end_date", pa.timestamp("ns")),
    ("candidate_contract_detail", pa.string()), ("mapped_pricing_tier", pa.string()), ("ambiguity_driver", pa.string()),
    ("contract_gap_code", pa.string()), ("contract_gap_detail", pa.string())]
PRICE_SCHEMA = [
    ("mapped_validation_price", pa.float64()), ("client_vs_mapped_price_diff", pa.float64()),
    ("client_vs_mapped_price_diff_pct", pa.float64()), ("client_contract_price_validation_flag", pa.string()),
    ("supply_vs_mapped_price_diff", pa.float64()), ("supply_vs_mapped_price_diff_pct", pa.float64()),
    ("supply_price_validation_flag", pa.string())]
FINAL_SCHEMA = [("unmapped_reason_code", pa.string()), ("unmapped_reason", pa.string())]  # always the LAST columns
BASE_COLS = [c for c, _ in BASE_SCHEMA]
UNSPSC_COLS = [c for c, _ in UNSPSC_SCHEMA]
CONTRACT_COLS = [c for c, _ in CONTRACT_SCHEMA]
PRICE_COLS = [c for c, _ in PRICE_SCHEMA]
FINAL_COLS = [c for c, _ in FINAL_SCHEMA]
DERIVED_SCHEMA = BASE_SCHEMA + UNSPSC_SCHEMA + CONTRACT_SCHEMA + PRICE_SCHEMA + FINAL_SCHEMA
DERIVED_COLS = [c for c, _ in DERIVED_SCHEMA]
_INT_COLS = [c for c, t in CONTRACT_SCHEMA if t == pa.int32()]


def compute_base(tbl: pa.Table, source_file: str, row_offset: int, cols: Dict[str, str],
                 alias: Dict[str, str]) -> pd.DataFrame:
    """Normalized keys, standard vendor, standardized consumption_date, numeric prices, row id/hash, eligibility."""
    n = tbl.num_rows
    col = lambda k: tbl.column(cols[k]).to_pandas()
    item = normalize_item_id(col("item_id"))
    vend_orig = col("vendor").fillna("")
    vend = normalize_vendor(vend_orig)
    vend_std = _map_unique(vend, lambda v: alias.get(v, v))
    cdate, dstat = parse_dates(col("date"))
    rownum = np.arange(row_offset + 1, row_offset + n + 1, dtype=np.int64)
    elig = np.where((item == "") | (vend == ""), ST_MISSING_KEY, np.where(cdate.isna(), ST_INVALID_DATE, "ELIGIBLE"))
    return pd.DataFrame({
        "source_file": source_file, "source_row_number": rownum,
        "row_id": pd.Series(rownum).astype(str).radd(f"{source_file}:").to_numpy(),
        "row_hash": hash_rows(tbl), KEY_ITEM: item.to_numpy(), "vendor_name_original": vend_orig.to_numpy(),
        KEY_VENDOR_NORM: vend.to_numpy(), KEY_VENDOR: vend_std.to_numpy(), "consumption_date": cdate.to_numpy(),
        "consumption_date_status": dstat.to_numpy(),
        "supply_unit_price": to_num(col("supply_unit_price")).to_numpy(),
        "client_contract_price": to_num(col("client_contract_price")).to_numpy(), "row_eligibility": elig})


def map_unspsc(base: pd.DataFrame, im: pd.DataFrame) -> pd.DataFrame:
    """Item Master enrichment: UNSPSC by Item ID + a reason when it could not be mapped.
    Ambiguous UNSPSC is flagged, never resolved."""
    assert im.index.is_unique
    items = base[KEY_ITEM].fillna("")
    in_im = items.isin(im.index)
    status = items.map(im["item_master_match_status"]).where(in_im, IM_NOT_FOUND).where(items != "", IM_MISSING_ID)
    mapped = items.map(im["unspsc"]).where(status == IM_MATCHED)
    nonnum = ~items.str.fullmatch(r"\d+")
    stripped_hit = (items.str.lstrip("0") != items) & items.str.lstrip("0").isin(im.index)
    code = np.select(
        [status == IM_MISSING_ID, (status == IM_NOT_FOUND) & stripped_hit, (status == IM_NOT_FOUND) & nonnum,
         status == IM_NOT_FOUND, status == IM_UNSPSC_MISSING, status == IM_AMBIG],
        ["MISSING_ITEM_ID", "ITEM_NOT_IN_ITEM_MASTER_LEADING_ZEROS", "ITEM_NOT_IN_ITEM_MASTER_NONNUMERIC_ID",
         "ITEM_NOT_IN_ITEM_MASTER", "UNSPSC_MISSING_IN_ITEM_MASTER", "AMBIGUOUS_UNSPSC"], default="")
    q = "Item ID '" + items + "'"
    cand = items.map(im["unspsc_candidates"]).fillna("")
    detail = np.select(
        [code == "MISSING_ITEM_ID", code == "ITEM_NOT_IN_ITEM_MASTER_LEADING_ZEROS",
         code == "ITEM_NOT_IN_ITEM_MASTER_NONNUMERIC_ID", code == "ITEM_NOT_IN_ITEM_MASTER",
         code == "UNSPSC_MISSING_IN_ITEM_MASTER", code == "AMBIGUOUS_UNSPSC"],
        ["Consumption Item ID is blank",
         (q + " not found in Item Master as-is, but exists after removing leading zeros").to_numpy(),
         (q + " is non-numeric and not found in Item Master").to_numpy(),
         (q + " not found in Item Master").to_numpy(),
         (q + " is in Item Master but UNSPSC is blank").to_numpy(),
         (q + " has multiple UNSPSC in Item Master: " + cand).to_numpy()], default="")
    out = pd.DataFrame({"mapped_unspsc": mapped.to_numpy(), "item_master_match_status": status.to_numpy(),
                        "item_master_gap_code": pd.Series(code, index=base.index).replace("", None).to_numpy(),
                        "item_master_gap_detail": pd.Series(detail, index=base.index).replace("", None).to_numpy()},
                       index=base.index)
    assert len(out) == len(base)
    return out


@dataclass
class ContractIndex:
    """Lookup structures over the Contracts used to explain WHY a key did not match."""
    vendors: set
    items: set
    item_vendors: pd.Series  # item id -> "VENDOR A; VENDOR B" (up to 3)


def build_contract_index(con: pd.DataFrame) -> ContractIndex:
    iv = con.drop_duplicates([KEY_ITEM, KEY_VENDOR]).groupby(KEY_ITEM)[KEY_VENDOR].agg(
        lambda s: "; ".join(sorted(s)[:3]) + (" ..." if len(s) > 3 else ""))
    return ContractIndex(set(con[KEY_VENDOR]), set(con[KEY_ITEM]), iv)


def _empty_contract_block(index) -> pd.DataFrame:
    return pd.DataFrame(index=index, columns=CONTRACT_COLS)


def map_contracts(base: pd.DataFrame, con: pd.DataFrame, cfg: Config, cindex: Optional[ContractIndex] = None) -> pd.DataFrame:
    """Vectorised contract selection for the rows in ``base`` (no Python row loops).

    Works on UNIQUE (standard vendor, item, consumption_date) combinations, joins them to the Contracts of the same
    vendor+item, evaluates lookback/active flags with array operations and picks the contract by group-wise
    aggregation. Also produces ``contract_gap_code/detail`` explaining every non-MATCHED outcome.
    Result has the same index/length as ``base`` (asserted)."""
    cindex = cindex or build_contract_index(con)
    elig = base["row_eligibility"].eq("ELIGIBLE")
    idx = base.index
    K3 = KEYS + ["consumption_date"]
    # ---- ineligible rows: which key is missing -------------------------------------------------------------
    mi = base[KEY_ITEM].fillna("").eq("")
    mv = base[KEY_VENDOR].fillna("").eq("")
    ne_code = np.select([mi & mv, mi, mv], ["MISSING_ITEM_AND_VENDOR", "MISSING_ITEM_ID", "MISSING_VENDOR"],
                        default="INVALID_CONSUMPTION_DATE")
    ne_detail = np.select(
        [mi & mv, mi, mv, base["consumption_date_status"].eq("MISSING")],
        ["Item ID and Supplier are both blank", "Item ID is blank", "Supplier is blank", "Admit date is blank"],
        default="Admit date could not be parsed")
    if not elig.any():
        out = _empty_contract_block(idx)
        out["contract_match_status"] = base["row_eligibility"].to_numpy()
        out["contract_selection_reason"] = "ROW_NOT_ELIGIBLE_FOR_CONTRACT_MAPPING"
        out["contract_gap_code"], out["contract_gap_detail"] = ne_code, ne_detail
        return _finalize_contract_block(out)

    combos = base.loc[elig, K3].drop_duplicates().reset_index(drop=True)
    combos["cid"] = np.arange(len(combos))
    keyset = con[KEYS].drop_duplicates().assign(_ex=True)
    combos = combos.merge(keyset, on=KEYS, how="left", validate="many_to_one")
    combos["key_exists"] = combos["_ex"].notna()
    ud = pd.DatetimeIndex(combos["consumption_date"].unique())
    lb = pd.Series(ud - pd.DateOffset(months=cfg.lookback_months), index=ud)
    combos["lookback_start_date"] = combos["consumption_date"].map(lb)
    cids = combos["cid"].to_numpy()
    n = len(combos)

    conm = con[KEYS + ["contract_number", "contract_start", "contract_end", "contract_uom",
                       "contract_price", "contract_ea_price", "pricing_tier"]]
    pairs = combos.loc[combos["key_exists"], ["cid"] + K3 + ["lookback_start_date"]].merge(conm, on=KEYS, how="inner")
    s, e, cd, lbs = pairs["contract_start"], pairs["contract_end"], pairs["consumption_date"], pairs["lookback_start_date"]
    openend = cfg.null_end_date_open_ended
    start_ok = s.notna() & (s <= cd)
    pairs["in_lb"] = start_ok & ((e >= lbs) | (e.isna() & openend))
    pairs["active"] = start_ok & ((e >= cd) | (e.isna() & openend))
    cand = pairs[pairs["in_lb"]]
    act = cand[cand["active"]]

    cand_cnt = cand.groupby("cid").size().reindex(cids, fill_value=0).to_numpy()
    act_cnt = act.groupby("cid").size().reindex(cids, fill_value=0).to_numpy()
    mx = act.groupby("cid")["contract_start"].transform("max")
    tie = act[act["contract_start"] == mx]                       # latest-start contracts among the active ones
    tie_cnt = tie.groupby("cid").size().reindex(cids, fill_value=0).to_numpy()
    tie_p = tie[tie["contract_price"].notna()].copy()             # rows able to provide a price
    if len(tie_p):
        # Price uniqueness is judged on the price used for validation. With the each-level (EA) price, candidates that
        # differ only in pack size / UOM but have the SAME each-level price are NOT ambiguous (the pack-level
        # price/UOM reported is then taken from the EA-UOM record first, then lowest contract number).
        if cfg.validation_price_field == "contract_ea_price":
            key_price = tie_p["contract_ea_price"].round(6).where(tie_p["contract_ea_price"].notna(), -tie_p["contract_price"].round(6))
            tie_p["pk"] = key_price.factorize()[0]
        else:
            tie_p["pk"] = pd.MultiIndex.from_arrays([tie_p["contract_price"].round(6), tie_p["contract_uom"].fillna("")]).factorize()[0]
        tie_p["pk_tuple"] = pd.MultiIndex.from_arrays([tie_p["contract_price"].round(6), tie_p["contract_ea_price"].round(6).fillna(-1.0),
                                                       tie_p["contract_uom"].fillna("")]).factorize()[0]
        g_tp = tie_p.groupby("cid")
        n_price = g_tp["pk"].nunique().reindex(cids, fill_value=0).to_numpy()
        n_tuple = g_tp["pk_tuple"].nunique().reindex(cids, fill_value=0).to_numpy()
        n_cnum = g_tp["contract_number"].nunique().reindex(cids, fill_value=0).to_numpy()
        n_tier = g_tp["pricing_tier"].nunique().reindex(cids, fill_value=0).to_numpy()
        n_end = tie_p.assign(_e=tie_p["contract_end"].fillna(FAR_FUTURE)).groupby("cid")["_e"].nunique().reindex(cids, fill_value=0).to_numpy()
        rep = (tie_p.assign(_notea=tie_p["contract_uom"] != "EA")
               .sort_values(["cid", "_notea", "contract_number", "contract_uom", "contract_price"])
               .drop_duplicates("cid").set_index("cid"))
    else:
        n_price = n_cnum = n_tuple = n_tier = n_end = np.zeros(n, dtype=int)
        rep = pd.DataFrame(columns=tie_p.columns).set_index("cid")

    key_exists = combos["key_exists"].to_numpy()
    status = np.select(
        [~key_exists, act_cnt == 0, n_price == 0, (n_price > 1) & (n_cnum > 1), n_price > 1],
        [ST_NOT_FOUND, ST_NOT_ACTIVE, ST_PRICE_MISSING, ST_AMBIG_CONTRACTS, ST_AMBIG_PRICES], default=ST_MATCHED)
    reason = np.select(
        [status == ST_NOT_FOUND, (status == ST_NOT_ACTIVE) & (cand_cnt > 0), status == ST_NOT_ACTIVE,
         status == ST_PRICE_MISSING, status == ST_AMBIG_CONTRACTS, status == ST_AMBIG_PRICES,
         (status == ST_MATCHED) & (n_tuple > 1),
         (status == ST_MATCHED) & (act_cnt == 1), (status == ST_MATCHED) & (tie_cnt == 1), (status == ST_MATCHED)],
        ["NO_CONTRACT_FOR_VENDOR_AND_ITEM",
         "CONTRACT_IN_LOOKBACK_BUT_NOT_ACTIVE_ON_CONSUMPTION_DATE",
         "VENDOR_ITEM_IN_CONTRACTS_BUT_NONE_IN_LOOKBACK_WINDOW_OR_START_DATE_INVALID",
         "ACTIVE_CONTRACT_HAS_NO_PRICE",
         "TIE_ON_LATEST_START_DATE_ACROSS_MULTIPLE_CONTRACTS_WITH_DIFFERENT_PRICES",
         "MULTIPLE_DIFFERENT_PRICES_ON_LATEST_ACTIVE_CONTRACT",
         "SAME_EACH_PRICE_ACROSS_TIED_RECORDS_PACK_UOM_DIFFERS",
         "SINGLE_ACTIVE_CONTRACT", "LATEST_START_DATE_AMONG_ACTIVE_CONTRACTS",
         "TIE_ON_LATEST_START_DATE_SAME_PRICE_LOWEST_CONTRACT_NUMBER_REPORTED"], default="")

    res = pd.DataFrame({"cid": cids, "contract_match_status": status, "contract_selection_reason": reason,
                        "contract_candidate_count": cand_cnt, "active_contract_count": act_cnt,
                        "tied_contract_count": tie_cnt, "unique_contract_price_count": n_price})
    res = pd.concat([combos[K3 + ["lookback_start_date"]], res], axis=1)
    res.index = pd.Index(cids)  # label = cid; unnamed so 'cid' is not both an index level and a column
    m_ok = res["contract_match_status"].eq(ST_MATCHED)
    for src, dst in [("contract_number", "mapped_contract_number"), ("contract_start", "mapped_contract_start_date"),
                     ("contract_end", "mapped_contract_end_date"), ("contract_price", "mapped_contract_price"),
                     ("contract_ea_price", "mapped_contract_ea_price"), ("contract_uom", "mapped_contract_uom"),
                     ("pricing_tier", "mapped_pricing_tier")]:
        res[dst] = rep[src].reindex(res.index).where(m_ok) if len(rep) else np.nan
    # audit: closest in-lookback contract for NOT_ACTIVE rows (never used as a mapped price)
    na_ids = res.index[res["contract_match_status"].eq(ST_NOT_ACTIVE)]
    cl = (cand[cand["cid"].isin(na_ids)].sort_values(["cid", "contract_end", "contract_start", "contract_number"],
                                                      ascending=[True, False, False, True])
          .drop_duplicates("cid").set_index("cid"))
    for src, dst in [("contract_number", "closest_contract_number"), ("contract_start", "closest_contract_start_date"),
                     ("contract_end", "closest_contract_end_date")]:
        res[dst] = cl[src].reindex(res.index) if len(cl) else np.nan
    # audit: full candidate list for ambiguous outcomes
    amb_mask = res["contract_match_status"].isin([ST_AMBIG_CONTRACTS, ST_AMBIG_PRICES])
    amb_ids = res.index[amb_mask | (m_ok & (n_tuple > 1))]   # ambiguous rows + matched rows resolved by the each-level price
    res["candidate_contract_detail"] = np.nan
    if len(amb_ids):
        t = tie_p[tie_p["cid"].isin(amb_ids)]
        det = (t["contract_number"] + "|start=" + t["contract_start"].dt.strftime("%Y-%m-%d") +
               "|end=" + t["contract_end"].dt.strftime("%Y-%m-%d").fillna("OPEN") +
               "|price=" + t["contract_price"].astype(str) + "|ea_price=" + t["contract_ea_price"].astype(str) +
               "|uom=" + t["contract_uom"] + "|tier=" + t["pricing_tier"].replace("", "(blank)"))
        res["candidate_contract_detail"] = det.groupby(t["cid"]).agg("; ".join).reindex(res.index)
    # what makes the tied records differ (only for unresolved ambiguity)
    res["ambiguity_driver"] = np.where(~amb_mask.to_numpy(), None, np.select(
        [n_cnum > 1, n_tier > 1, n_end > 1], ["DIFFERENT_CONTRACT_NUMBERS", "DIFFERENT_PRICING_TIER",
                                              "SAME_CONTRACT_AND_TIER_DIFFERENT_END_DATE"],
        default="SAME_CONTRACT_TIER_AND_END_DATE"))

    # ---- WHY NOT MAPPED: code + plain-English detail ----------------------------------------------------------------
    kv, ki = combos[KEY_VENDOR], combos[KEY_ITEM]
    v_ex = kv.isin(cindex.vendors).to_numpy()
    i_ex = ki.isin(cindex.items).to_numpy()
    iv_txt = ki.map(cindex.item_vendors).fillna("").to_numpy()
    qv, qi = "'" + kv + "'", "'" + ki + "'"
    dstr = combos["consumption_date"].dt.strftime("%Y-%m-%d")
    code = np.full(n, "", dtype=object)
    detail = np.full(n, "", dtype=object)
    nf = status == ST_NOT_FOUND
    scen = [nf & ~v_ex & ~i_ex, nf & ~v_ex & i_ex, nf & v_ex & ~i_ex, nf & v_ex & i_ex]
    scen_code = ["VENDOR_AND_ITEM_NOT_IN_CONTRACTS", "VENDOR_NOT_IN_CONTRACTS", "ITEM_NOT_IN_CONTRACTS",
                 "VENDOR_ITEM_PAIR_NOT_IN_CONTRACTS"]
    scen_txt = [("Vendor " + qv + " and Item ID " + qi + " not found in Contracts").to_numpy(),
                ("Vendor " + qv + " not found in Contracts (check vendor alias); Item ID " + qi +
                 " is contracted under: " + iv_txt).to_numpy(),
                ("Item ID " + qi + " not found in Contracts (vendor " + qv + " has other contracts)").to_numpy(),
                ("Vendor " + qv + " and Item ID " + qi + " both exist in Contracts but not together; item is contracted under: "
                 + iv_txt).to_numpy()]
    for m_, c_, t_ in zip(scen, scen_code, scen_txt):
        code[m_] = c_
        detail[m_] = t_[m_]
    na = status == ST_NOT_ACTIVE
    if na.any():
        pn = pairs[pairs["cid"].isin(cids[na])].copy()
        s_ok = pn["contract_start"].notna()
        exp_gap = (pn["consumption_date"] - pn["contract_end"]).dt.days.where(
            s_ok & (pn["contract_start"] <= pn["consumption_date"]) & (pn["contract_end"] < pn["consumption_date"]))
        fut_gap = (pn["contract_start"] - pn["consumption_date"]).dt.days.where(s_ok & (pn["contract_start"] > pn["consumption_date"]))
        pn["gap"] = exp_gap.fillna(fut_gap)
        pn["kind"] = np.select(
            [~s_ok, fut_gap.notna(), exp_gap.notna() & (pn["contract_end"] < pn["lookback_start_date"]), exp_gap.notna()],
            ["NOT_ACTIVE_INVALID_START_DATE", "NOT_ACTIVE_NOT_YET_STARTED", "NOT_ACTIVE_EXPIRED_BEFORE_LOOKBACK",
             "NOT_ACTIVE_EXPIRED_WITHIN_LOOKBACK"], default="NOT_ACTIVE_NO_END_DATE")
        n_file = pn.groupby("cid").size()
        best = pn.sort_values(["cid", "gap", "contract_number"]).drop_duplicates("cid").set_index("cid")
        sd = best["contract_start"].dt.strftime("%Y-%m-%d").fillna("invalid")
        ed = best["contract_end"].dt.strftime("%Y-%m-%d").fillna("OPEN")
        base_txt = ("Vendor+Item is in Contracts but no contract is active on " +
                    best["consumption_date"].dt.strftime("%Y-%m-%d") + ": nearest is " + best["contract_number"] +
                    " (" + sd + " to " + ed + ")")
        tail = " [" + n_file.reindex(best.index).astype(str) + " contract record(s) on file]"
        gap_s = best["gap"].fillna(0).astype(int).astype(str)
        txt = np.select(
            [best["kind"] == "NOT_ACTIVE_EXPIRED_WITHIN_LOOKBACK", best["kind"] == "NOT_ACTIVE_EXPIRED_BEFORE_LOOKBACK",
             best["kind"] == "NOT_ACTIVE_NOT_YET_STARTED", best["kind"] == "NOT_ACTIVE_INVALID_START_DATE"],
            [(base_txt + ", ended " + gap_s + " days before consumption (inside the 6-month lookback)" + tail).to_numpy(),
             (base_txt + ", ended " + gap_s + " days before consumption (outside the 6-month lookback)" + tail).to_numpy(),
             (base_txt + ", starts " + gap_s + " days after consumption" + tail).to_numpy(),
             (base_txt + "; its start date is missing/invalid" + tail).to_numpy()],
            default=(base_txt + "; no end date" + tail).to_numpy())
        kmap = pd.Series(best["kind"].to_numpy(), index=best.index)
        tmap = pd.Series(txt, index=best.index)
        code[na] = kmap.reindex(cids[na]).to_numpy()
        detail[na] = tmap.reindex(cids[na]).to_numpy()
    lstart = act.groupby("cid")["contract_start"].max().reindex(cids).dt.strftime("%Y-%m-%d").fillna("").to_numpy()
    det_c = res["candidate_contract_detail"].fillna("").to_numpy()
    for st_, c_, pre in ((ST_AMBIG_CONTRACTS, "AMBIGUOUS_MULTIPLE_CONTRACTS", " active contracts share latest start date "),
                         (ST_AMBIG_PRICES, "AMBIGUOUS_MULTIPLE_PRICES", " different prices on one contract for latest start date ")):
        m_ = status == st_
        if m_.any():
            code[m_] = c_
            detail[m_] = (pd.Series(tie_cnt.astype(str)) if st_ == ST_AMBIG_CONTRACTS else pd.Series(n_price.astype(str))
                          ).to_numpy()[m_].astype(object) + pre + lstart[m_] + " -> " + det_c[m_]
    m_ = status == ST_PRICE_MISSING
    code[m_] = "CONTRACT_PRICE_MISSING"
    detail[m_] = "Active contract found but it has no price"
    res["contract_gap_code"] = pd.Series(code, index=res.index).replace("", None)
    res["contract_gap_detail"] = pd.Series(detail, index=res.index).replace("", None)

    rows = base.loc[elig, K3].merge(res.drop(columns="cid", errors="ignore"), on=K3, how="left", validate="many_to_one")
    assert len(rows) == int(elig.sum()), "contract join multiplied/dropped rows"
    rows.index = base.index[elig]
    out = rows.reindex(idx)
    ne = ~elig
    out.loc[ne, "contract_match_status"] = base.loc[ne, "row_eligibility"]
    out.loc[ne, "contract_selection_reason"] = "ROW_NOT_ELIGIBLE_FOR_CONTRACT_MAPPING"
    out.loc[ne, "contract_gap_code"] = pd.Series(ne_code, index=idx)[ne]
    out.loc[ne, "contract_gap_detail"] = pd.Series(ne_detail, index=idx)[ne]
    return _finalize_contract_block(out)


def _finalize_contract_block(out: pd.DataFrame) -> pd.DataFrame:
    for c in CONTRACT_COLS:
        if c not in out.columns:
            out[c] = np.nan
    out = out[CONTRACT_COLS].copy()
    for c in _INT_COLS:
        out[c] = pd.to_numeric(out[c], errors="coerce").astype("Int32")
    for c in ("lookback_start_date", "mapped_contract_start_date", "mapped_contract_end_date",
              "closest_contract_start_date", "closest_contract_end_date"):
        out[c] = pd.to_datetime(out[c])
    for c in ("mapped_contract_price", "mapped_contract_ea_price"):
        out[c] = pd.to_numeric(out[c], errors="coerce").astype("float64")
    return out


def validate_prices(base: pd.DataFrame, cblk: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    """Two comparisons against the mapped contract price (denominator = mapped price):
       (1) client-provided Contract Price vs mapped, (2) Supply Unit Price vs mapped.
       Review flags only - a >threshold difference is NOT treated as a data error."""
    matched = cblk["contract_match_status"].eq(ST_MATCHED)
    mapped = cblk[f"mapped_{cfg.validation_price_field}"].where(matched).astype("float64")
    den_ok = mapped.notna() & (mapped != 0)
    out = pd.DataFrame(index=base.index)
    out["mapped_validation_price"] = mapped
    for label, price in (("client", base["client_contract_price"]), ("supply", base["supply_unit_price"])):
        comparable = matched & den_ok & price.notna()
        diff = (price - mapped).where(comparable)
        pct = (diff / mapped.where(den_ok).abs()).where(comparable)
        flag = np.where(~comparable, FLAG_NA, np.where(pct.abs() > cfg.price_threshold, cfg.above_label, FLAG_WITHIN))
        if label == "client":
            out["client_vs_mapped_price_diff"], out["client_vs_mapped_price_diff_pct"] = diff, pct
            out["client_contract_price_validation_flag"] = flag
        else:
            out["supply_vs_mapped_price_diff"], out["supply_vs_mapped_price_diff_pct"] = diff, pct
            out["supply_price_validation_flag"] = flag
    return out[PRICE_COLS]


def _join_nonempty(a: pd.Series, b: pd.Series, sep: str) -> pd.Series:
    a, b = a.fillna(""), b.fillna("")
    r = np.where((a != "") & (b != ""), a + sep + b, np.where(a != "", a, b))
    return pd.Series(r, index=a.index).replace("", None)


def build_unmapped_reason(u_blk: pd.DataFrame, c_blk: pd.DataFrame) -> pd.DataFrame:
    """LAST two columns of the dataset: machine-readable code(s) and a plain-English explanation. Blank when the
    row is fully mapped (contract MATCHED and UNSPSC MATCHED)."""
    cg, cdx = c_blk["contract_gap_code"], c_blk["contract_gap_detail"]
    ig, idx_ = u_blk["item_master_gap_code"], u_blk["item_master_gap_detail"]
    code = _join_nonempty(("CONTRACT:" + cg.fillna("")).where(cg.notna(), None),
                          ("ITEM_MASTER:" + ig.fillna("")).where(ig.notna(), None), "; ")
    text = _join_nonempty(("CONTRACT: " + cdx.fillna("")).where(cdx.notna(), None),
                          ("ITEM MASTER: " + idx_.fillna("")).where(idx_.notna(), None), " || ")
    return pd.DataFrame({"unmapped_reason_code": code, "unmapped_reason": text})


def contract_affected_mask(base: pd.DataFrame, windows: Optional[pd.DataFrame]) -> np.ndarray:
    """Rows whose Vendor+Item is in the changed-contract windows and whose date falls inside [lo, hi]."""
    if windows is None or windows.empty:
        return np.zeros(len(base), dtype=bool)
    assert not windows.duplicated(KEYS).any()
    m = base[KEYS + ["consumption_date"]].merge(windows, on=KEYS, how="left")
    return ((m["consumption_date"] >= m["lo"]) & (m["consumption_date"] <= m["hi"])).to_numpy()


def reason_affected_mask(base: pd.DataFrame, prev_status: np.ndarray, windows: Optional[pd.DataFrame],
                         vendors: set, items: set) -> np.ndarray:
    """Rows whose *explanation text* (not the mapping) can change: NOT_ACTIVE rows of a changed key (any date - the
    nearest contract may be far away) and NOT_FOUND rows whose vendor/item has changed contracts."""
    if windows is None or windows.empty:
        return np.zeros(len(base), dtype=bool)
    hit = base[KEYS].merge(windows[KEYS].assign(_h=True), on=KEYS, how="left")["_h"].notna().to_numpy()
    nf = (prev_status == ST_NOT_FOUND) & (base[KEY_VENDOR].isin(vendors).to_numpy() | base[KEY_ITEM].isin(items).to_numpy())
    return (hit & (prev_status == ST_NOT_ACTIVE)) | nf


def item_affected_mask(base: pd.DataFrame, items: Optional[set]) -> np.ndarray:
    if not items:
        return np.zeros(len(base), dtype=bool)
    it = base[KEY_ITEM].fillna("")
    return (it.isin(items) | it.str.lstrip("0").isin(items)).to_numpy()


def determine_affected_consumption_rows(base: pd.DataFrame, windows, items) -> Tuple[np.ndarray, np.ndarray]:
    """(contract_affected, item_master_affected) boolean masks for a chunk of Consumption rows."""
    return contract_affected_mask(base, windows), item_affected_mask(base, items)


# --------------------------------------------------------------------------------------
# CURRENT CONTRACT PRICE + CONTRACT CATEGORY (item level, as of the run date)
# --------------------------------------------------------------------------------------
# Independent of the historical (consumption-date) mapping above. Computed when the final dataset is written, so it
# never invalidates the cached mapping state.
CURRENT_SCHEMA = [
    ("current_contract_status", pa.string()), ("current_contract_price", pa.float64()),
    ("current_contract_ea_price", pa.float64()), ("current_contract_uom", pa.string()),
    ("current_contract_number", pa.string()), ("current_contract_vendor", pa.string()),
    ("current_contract_start_date", pa.timestamp("ns")), ("current_contract_end_date", pa.timestamp("ns")),
    ("current_pricing_tier", pa.string()), ("current_contract_scope", pa.string()),
    ("current_contract_price_count", pa.int32()), ("non_contract_reason_code", pa.string()),
    ("non_contract_reason", pa.string()), ("item_contract_category", pa.string()), ("item_contract_category_code", pa.string()),
    ("item_contract_category_description", pa.string()), ("current_contract_as_of_date", pa.timestamp("ns"))]
CURRENT_COLS = [c for c, _ in CURRENT_SCHEMA]
CURRENT_DATE_COLS = ["current_contract_start_date", "current_contract_end_date", "current_contract_as_of_date"]
ST_CONTRACTED, ST_CONTRACTED_AMBIG = "CONTRACTED", "CONTRACTED_PRICE_AMBIGUOUS"
ST_CONTRACTED_NOPRICE, ST_NONCONTRACTED, ST_NOT_ASSESSED = "CONTRACTED_NO_PRICE", "NON-CONTRACTED", "NOT_ASSESSED"
NONCONTRACT_INFO = {
    "NO_CONTRACT_RECORD_FOR_ITEM": "Item ID has no record in Contracts (never contracted)",
    "ALL_CONTRACTS_EXPIRED": "Item has contract records but all have expired as of the run date",
    "ONLY_FUTURE_CONTRACTS": "Item's contracts all start after the run date",
    "NO_VALID_CONTRACT_DATES": "Item's contract records have no usable start date",
    "MISSING_ITEM_ID": "Consumption row has no Item ID, so it cannot be assessed"}


@dataclass
class CurrentLookups:
    as_of: pd.Timestamp
    by_vendor: pd.DataFrame   # one row per (standard vendor, item) with an active contract
    by_item: pd.DataFrame     # one row per item with an active contract (any vendor)
    hist: pd.DataFrame        # per item: latest expired / first future contract (explains NON-CONTRACTED)
    category: pd.DataFrame    # per item: contract_category (+ code, description)


def _pick_current(act: pd.DataFrame, keys: List[str], cfg: Config, prefix: str) -> pd.DataFrame:
    """Per key: number of active records, and - on the latest start date - the price when it is unique.
    Uniqueness uses the validation price (each-level price by default), exactly like the historical mapping."""
    n_active = act.groupby(keys).size().rename("n_active")
    tie = act[act["contract_start"] == act.groupby(keys)["contract_start"].transform("max")]
    pr = tie[tie["contract_price"].notna()].copy()
    if cfg.validation_price_field == "contract_ea_price":
        pr["_kp"] = pr["contract_ea_price"].round(6).where(pr["contract_ea_price"].notna(), -pr["contract_price"].round(6))
    else:
        pr["_kp"] = pr["contract_price"].round(6)
    price_count = pr.groupby(keys)["_kp"].nunique().rename("price_count")
    rep = (pr.assign(_notea=pr["contract_uom"] != "EA")
           .sort_values(keys + ["_notea", KEY_VENDOR, "contract_number", "contract_uom", "contract_price"])
           .drop_duplicates(keys).set_index(keys))
    cols = ["contract_price", "contract_ea_price", "contract_uom", "contract_number", "contract_start", "contract_end", "pricing_tier"]
    out = n_active.to_frame().join(price_count)
    out["price_count"] = out["price_count"].fillna(0).astype(int)
    r = rep[cols + ([KEY_VENDOR] if KEY_VENDOR not in keys else [])].rename(columns={KEY_VENDOR: "rep_vendor"})
    out = out.join(r)
    if KEY_VENDOR in keys:
        out["rep_vendor"] = out.index.get_level_values(KEY_VENDOR)
    unique = out["price_count"] == 1
    for c in cols + ["rep_vendor"]:   # a price/contract is only reported when the price is unique
        out[c] = out[c].where(unique)
    out = out.rename(columns={c: f"{prefix}{c}" for c in out.columns}).reset_index()
    return out


def build_current_lookups(con: pd.DataFrame, as_of: pd.Timestamp, cfg: Config) -> CurrentLookups:
    """Item-level lookups over the (de-duplicated, vendor-standardized) Contracts as of ``as_of``.
    Active = start <= as_of <= end (blank end = open-ended when cfg.null_end_date_open_ended)."""
    start_ok = con["contract_start"].notna() & (con["contract_start"] <= as_of)
    active = start_ok & ((con["contract_end"] >= as_of) | (con["contract_end"].isna() & cfg.null_end_date_open_ended))
    act = con[active]
    by_vendor = _pick_current(act, KEYS, cfg, "v_")
    by_item = _pick_current(act, [KEY_ITEM], cfg, "i_")
    # why an item is NOT currently contracted
    n_rec = con.groupby(KEY_ITEM).size().rename("n_rec")
    past = con[start_ok & (con["contract_end"] < as_of)].sort_values(["contract_end", "contract_start"]).groupby(KEY_ITEM).last()
    fut = con[con["contract_start"].notna() & (con["contract_start"] > as_of)].sort_values("contract_start").groupby(KEY_ITEM).first()
    hist = n_rec.to_frame().join(past[[KEY_VENDOR, "contract_number", "contract_end"]].add_prefix("past_"), how="left") \
        .join(fut[[KEY_VENDOR, "contract_number", "contract_start"]].add_prefix("fut_"), how="left").reset_index()
    cat = con[con["contract_category"] != ""].assign(_s=con["contract_start"].fillna(FAR_PAST)).sort_values("_s") \
        .groupby(KEY_ITEM)["contract_category"].last().to_frame().reset_index()
    cat["contract_category_code"] = cat["contract_category"].str.extract(r"^(\d{6,8})")[0]
    cat["contract_category_description"] = cat["contract_category"].str.replace(r"^\d+\s*[-:]\s*", "", regex=True)
    return CurrentLookups(as_of, by_vendor, by_item, hist, cat)


def current_contract_block(keys: pd.DataFrame, lk: CurrentLookups) -> pd.DataFrame:
    """Current contract price / category / NON-CONTRACTED tag for each row (same order and length as ``keys``).
    Preference: an active contract of the row's own standard vendor, otherwise an active contract of any vendor."""
    k = pd.DataFrame({KEY_VENDOR: keys[KEY_VENDOR].fillna("").to_numpy(), KEY_ITEM: keys[KEY_ITEM].fillna("").to_numpy()})
    n = len(k)
    mv = k.merge(lk.by_vendor, on=KEYS, how="left", validate="many_to_one")
    mi = k[[KEY_ITEM]].merge(lk.by_item, on=KEY_ITEM, how="left", validate="many_to_one")
    mh = k[[KEY_ITEM]].merge(lk.hist, on=KEY_ITEM, how="left", validate="many_to_one")
    mc = k[[KEY_ITEM]].merge(lk.category, on=KEY_ITEM, how="left", validate="many_to_one")
    assert len(mv) == len(mi) == len(mh) == len(mc) == n, "current-contract join changed the row count"
    blank = k[KEY_ITEM].eq("").to_numpy()
    has_v, has_i = mv["v_n_active"].notna().to_numpy(), mi["i_n_active"].notna().to_numpy()
    has = has_v | has_i
    pcount = np.where(has_v, mv["v_price_count"], mi["i_price_count"])
    pick = lambda vc, ic: np.where(has_v, mv[vc], mi[ic])
    status = np.select([blank, has & (pcount == 1), has & (pcount > 1), has], [ST_NOT_ASSESSED, ST_CONTRACTED, ST_CONTRACTED_AMBIG, ST_CONTRACTED_NOPRICE],
                       default=ST_NONCONTRACTED)
    nc = status == ST_NONCONTRACTED
    q = "'" + k[KEY_ITEM] + "'"
    past_ok, fut_ok = mh["past_contract_end"].notna().to_numpy(), mh["fut_contract_start"].notna().to_numpy()
    no_rec = mh["n_rec"].isna().to_numpy()
    rcode = np.select([blank, nc & no_rec, nc & past_ok, nc & fut_ok, nc],
                      ["MISSING_ITEM_ID", "NO_CONTRACT_RECORD_FOR_ITEM", "ALL_CONTRACTS_EXPIRED", "ONLY_FUTURE_CONTRACTS", "NO_VALID_CONTRACT_DATES"],
                      default="")
    past_txt = ("Latest contract " + mh["past_contract_number"].fillna("") + " (" + mh["past_" + KEY_VENDOR].fillna("") + ") ended " +
                mh["past_contract_end"].dt.strftime("%Y-%m-%d").fillna("") + np.where(fut_ok, "; a later contract starts " +
                                                                                         mh["fut_contract_start"].dt.strftime("%Y-%m-%d").fillna(""), ""))
    fut_txt = ("First contract " + mh["fut_contract_number"].fillna("") + " (" + mh["fut_" + KEY_VENDOR].fillna("") + ") starts " +
               mh["fut_contract_start"].dt.strftime("%Y-%m-%d").fillna(""))
    rtext = np.select([rcode == "MISSING_ITEM_ID", rcode == "NO_CONTRACT_RECORD_FOR_ITEM", rcode == "ALL_CONTRACTS_EXPIRED",
                       rcode == "ONLY_FUTURE_CONTRACTS", rcode == "NO_VALID_CONTRACT_DATES"],
                      ["Consumption row has no Item ID", (q + " has no record in Contracts").to_numpy(), past_txt.to_numpy(),
                       fut_txt.to_numpy(), (q + " contract records have no usable start date").to_numpy()], default="")
    out = pd.DataFrame({
        "current_contract_status": status,
        "current_contract_price": pick("v_contract_price", "i_contract_price").astype("float64"),
        "current_contract_ea_price": pick("v_contract_ea_price", "i_contract_ea_price").astype("float64"),
        "current_contract_uom": pick("v_contract_uom", "i_contract_uom"),
        "current_contract_number": pick("v_contract_number", "i_contract_number"),
        "current_contract_vendor": pick("v_rep_vendor", "i_rep_vendor"),
        "current_contract_start_date": pd.to_datetime(pd.Series(pick("v_contract_start", "i_contract_start"))),
        "current_contract_end_date": pd.to_datetime(pd.Series(pick("v_contract_end", "i_contract_end"))),
        "current_pricing_tier": pick("v_pricing_tier", "i_pricing_tier"),
        "current_contract_scope": np.where(has_v, "SAME_VENDOR", np.where(has_i, "OTHER_VENDOR", None)),
        "current_contract_price_count": pd.array(np.where(has, pcount, np.nan), dtype="Int32"),
        "non_contract_reason_code": pd.Series(rcode).replace("", None).to_numpy(),
        "non_contract_reason": pd.Series(rtext).replace("", None).to_numpy(),
        "item_contract_category": mc["contract_category"].to_numpy(),
        "item_contract_category_code": mc["contract_category_code"].to_numpy(),
        "item_contract_category_description": mc["contract_category_description"].to_numpy(),
        "current_contract_as_of_date": lk.as_of})
    for c in ("current_contract_uom", "current_contract_number", "current_contract_vendor", "current_pricing_tier"):
        out[c] = pd.Series(out[c]).where(pd.Series(out[c]).notna(), None)
    return out[CURRENT_COLS]


# --------------------------------------------------------------------------------------
# DATA DICTIONARY of the final dataset (single source of truth for the Excel sheet and the HTML documentation)
# --------------------------------------------------------------------------------------
G_SRC, G_AUDIT, G_KEYS, G_IM, G_CON, G_PRICE, G_CUR, G_WHY = (
    "1. Consumption (source columns)", "2. Row identity & audit", "3. Keys & normalization", "4. Item Master mapping",
    "5. Contract mapping (at consumption date)", "6. Price validation", "7. Current contract & category (as of run date)",
    "8. Why not mapped")
SRC_CONS, SRC_CON, SRC_IM, SRC_ALIAS, SRC_PIPE = "Consumption", "Contracts", "Item Master", "Vendor alias table", "Pipeline"
_D = []


def _d(name, group, source, desc, values=""):
    _D.append((name, group, source, desc, values))


for _n, _t in [
    ("LOG_ID", "Procedure log identifier from the source system; repeats on every supply line of the same log."),
    ("FACILITY", "Facility / unit where the case took place."),
    ("MEDICAL_RECORD_NUMBER", "Patient medical record number (sensitive; unchanged)."),
    ("CASE_ID / ENCOUNTER FHIR ID", "Case / encounter identifier."),
    ("DRG_CODE", "Diagnosis-related group code(s) of the encounter (may hold several comma-separated codes)."),
    ("SURGICAL_HIERARCHY", "Surgical hierarchy / major diagnostic category grouping."),
    ("BILLED_CPT_CODE", "Billed CPT code(s) for the case."),
    ("PRIMARY_ICD10_PX_CODE", "Primary ICD-10 procedure code."),
    ("PRIMARY_PROCEDURE", "Primary procedure description."),
    ("SERVICE_LINE", "Clinical service line."),
    ("PATIENT_TYPE", "Patient type / setting (e.g. outpatient surgery)."),
    ("LEAD_SURGEON", "Lead surgeon name."),
    ("PAYOR_GROUP", "Payor group of the encounter."),
    ("ADMIT_DATE_TIME", "Admit date and time. Its date part becomes consumption_date, the reference date for contract selection."),
    ("DISCHARGE_DATE_TIME", "Discharge date and time."),
    ("LOS", "Length of stay."),
    ("GMLOS", "Geometric mean length of stay benchmark for the DRG."),
    ("ACCOUNT_NUMBER", "Patient account number."),
    ("CONTRACT_PRICE", "Client-provided contract price for the supply line. Compared with the mapped contract price (client_vs_mapped_*)."),
    ("TOTAL_ACQUISITION_COST", "Total acquisition cost of the supply line."),
    ("SUPPLY_UNIT_PRICE", "Unit price paid for the supply item. Compared with the mapped contract price (supply_vs_mapped_*)."),
    ("TOTAL_QUANTITY", "Quantity consumed."),
    ("IMPLANT_VAR_DIRECT_COST", "Variable direct cost - implants."),
    ("MED_SUPPLY_VAR_DIRECT_COST", "Variable direct cost - medical supplies."),
    ("TOTAL_CHARGES", "Total charges of the account."),
    ("TOTAL_ACCT_BAL", "Total account balance."),
    ("TOTAL_ADJ", "Total adjustments."),
    ("TOTAL_PMTS", "Total payments."),
    ("SSI (0/1)", "Surgical site infection flag."),
    ("BLOOD_TRANSFUSION_FLAG (0/1)", "Blood transfusion flag."),
    ("READMISSION_INDEX_CASE (0/1)", "Readmission index case flag."),
    ("MORTALITY (0/1)", "Mortality flag."),
    ("RISK OF MORTALITY", "Risk-of-mortality class."),
    ("MANUFACTURER_NAME", "Manufacturer name of the item (not used as the contract vendor)."),
    ("MANUFACTURER_CATALOG_NUMBER", "Manufacturer catalog number."),
    ("ITEM_NUMBER", "Item ID. Mapping key to Item Master (item_id) and Contracts (item_id); normalized into item_id_normalized."),
    ("ITEM_DESCRIPTION", "Item description."),
    ("ITEM_UOM", "Unit of measure of the consumed item (e.g. EACH, PAIR)."),
    ("ITEM_QOE", "Quantity of each per unit of measure as provided."),
    ("SUPPLIER", "Supplier name. Source of the vendor key; normalized and standardized into vendor_name_standard."),
    ("CONTRACT_CATEGORY", "Client-provided contract category label (e.g. Pack, Tubing). NOT the Contracts-file category - see item_contract_category."),
    ("SPEND_CATEGORY", "Spend category."),
    ("UNSPSC_CODE", "Client free-text UNSPSC / category label. Not used; the UNSPSC comes from Item Master (mapped_unspsc)."),
    ("CONTRACT_FLAG", "Client flag ON CONTRACT / OFF CONTRACT. Not used for mapping."),
    ("ASA_RATING", "ASA physical status rating."),
    ("BMI_BUCKET", "BMI bucket."),
    ("ROBOTICS (0/1)", "Robotic procedure flag."),
    ("SMOKING_STATUS", "Smoking status."),
    ("DIABETIC_STATUS (0/1)", "Diabetic flag."),
    ("PATIENT_AGE_BUCKET", "Patient age bucket."),
    ("PATIENT_GENDER", "Patient gender."),
    ("ETHNICITY", "Patient ethnicity."),
]:
    _d(_n, G_SRC, SRC_CONS, _t + " Unchanged from the source file (text).")

_d("source_file", G_AUDIT, SRC_PIPE, "Name of the Consumption CSV part the row came from.")
_d("source_row_number", G_AUDIT, SRC_PIPE, "1-based data row number inside source_file (header excluded).")
_d("row_id", G_AUDIT, SRC_PIPE, "Stable unique row identifier = source_file:source_row_number. Appears exactly once in the dataset.")
_d("row_hash", G_AUDIT, SRC_PIPE, "64-bit hash of the whole raw row; used to detect changed rows on re-runs.")

_d("item_id_normalized", G_KEYS, SRC_CONS, "ITEM_NUMBER as text, trimmed. Leading zeros and non-numeric IDs preserved. Join key for Item Master and Contracts.")
_d("vendor_name_original", G_KEYS, SRC_CONS, "SUPPLIER exactly as in the source.")
_d("vendor_name_normalized", G_KEYS, SRC_CONS, "SUPPLIER trimmed, upper-cased, repeated spaces collapsed. This is the vendor alias table's alias_name.")
_d("vendor_name_standard", G_KEYS, SRC_ALIAS, "Standard vendor name after the vendor alias table (exact / legal-suffix core key / vendor code / item overlap / fuzzy / reviewer override). Join key to Contracts.")
_d("consumption_date", G_KEYS, SRC_CONS, "Date part of ADMIT_DATE_TIME. Reference date for the 6-month lookback and contract validity. Empty if missing/invalid.")
_d("consumption_date_status", G_KEYS, SRC_PIPE, "Result of parsing ADMIT_DATE_TIME.", "VALID, MISSING, INVALID")
_d("supply_unit_price", G_KEYS, SRC_CONS, "SUPPLY_UNIT_PRICE parsed to a number (blank/invalid = empty).")
_d("client_contract_price", G_KEYS, SRC_CONS, "CONTRACT_PRICE parsed to a number (blank/invalid = empty).")
_d("row_eligibility", G_KEYS, SRC_PIPE, "Whether the row can be keyed for contract mapping: Item ID, Supplier and valid admit date all present. Only ELIGIBLE rows count in the match-rate denominator.",
   "ELIGIBLE, MISSING_MAPPING_KEY, INVALID_CONSUMPTION_DATE")

_d("mapped_unspsc", G_IM, SRC_IM, "UNSPSC from Item Master (unspsc) joined on item_id = item_id_normalized. Empty unless item_master_match_status = MATCHED.")
_d("item_master_match_status", G_IM, SRC_IM, "Result of the Item Master join. An Item ID with more than one distinct UNSPSC is never resolved arbitrarily.",
   "MATCHED, ITEM_NOT_FOUND, AMBIGUOUS_UNSPSC, MISSING_ITEM_ID, UNSPSC_MISSING")
_d("item_master_gap_code", G_IM, SRC_PIPE, "Machine-readable reason when the Item Master mapping failed (empty when MATCHED).",
   "MISSING_ITEM_ID, ITEM_NOT_IN_ITEM_MASTER, ITEM_NOT_IN_ITEM_MASTER_NONNUMERIC_ID, ITEM_NOT_IN_ITEM_MASTER_LEADING_ZEROS, UNSPSC_MISSING_IN_ITEM_MASTER, AMBIGUOUS_UNSPSC")
_d("item_master_gap_detail", G_IM, SRC_PIPE, "Plain-English explanation for item_master_gap_code.")

_d("contract_match_status", G_CON, SRC_CON, "Outcome of mapping the row to a contract on its consumption date. Only MATCHED rows receive a mapped price.",
   "MATCHED, CONTRACT_NOT_FOUND, CONTRACT_NOT_ACTIVE, AMBIGUOUS_MULTIPLE_CONTRACTS, AMBIGUOUS_MULTIPLE_PRICES, CONTRACT_PRICE_MISSING, MISSING_MAPPING_KEY, INVALID_CONSUMPTION_DATE")
_d("contract_selection_reason", G_CON, SRC_PIPE, "Why this contract was selected (or which rule ended the search).",
   "SINGLE_ACTIVE_CONTRACT, LATEST_START_DATE_AMONG_ACTIVE_CONTRACTS, TIE_ON_LATEST_START_DATE_SAME_PRICE_LOWEST_CONTRACT_NUMBER_REPORTED, SAME_EACH_PRICE_ACROSS_TIED_RECORDS_PACK_UOM_DIFFERS, NO_CONTRACT_FOR_VENDOR_AND_ITEM, CONTRACT_IN_LOOKBACK_BUT_NOT_ACTIVE_ON_CONSUMPTION_DATE, ... (see README)")
_d("lookback_start_date", G_CON, SRC_PIPE, "consumption_date minus 6 calendar months: start of the lookback window.")
_d("mapped_contract_number", G_CON, SRC_CON, "contract_number of the selected contract (MATCHED rows only). If several tied records have the same each-level price, the lowest number is shown.")
_d("mapped_contract_start_date", G_CON, SRC_CON, "contract_start of the selected contract.")
_d("mapped_contract_end_date", G_CON, SRC_CON, "contract_end of the selected contract (empty = open-ended).")
_d("mapped_contract_price", G_CON, SRC_CON, "contract_price of the selected contract, per contract UOM (pack price).")
_d("mapped_contract_ea_price", G_CON, SRC_CON, "contract_ea_price of the selected contract: each-level price. Used for price validation.")
_d("mapped_contract_uom", G_CON, SRC_CON, "contract_uom of the selected contract (e.g. EA, BX, CA).")
_d("contract_candidate_count", G_CON, SRC_CON, "Contract records for the same standard vendor + Item ID that overlap the 6-month lookback window.")
_d("active_contract_count", G_CON, SRC_CON, "Candidate records active on the consumption date (start <= date <= end; blank end = open-ended).")
_d("tied_contract_count", G_CON, SRC_CON, "Active records sharing the latest contract_start date (the tie set).")
_d("unique_contract_price_count", G_CON, SRC_CON, "Distinct each-level (EA) prices within the tie set. 1 = deterministic price; >1 = ambiguous.")
_d("closest_contract_number", G_CON, SRC_CON, "Audit only (CONTRACT_NOT_ACTIVE rows): contract number of the closest contract inside the lookback window. Never used as a price.")
_d("closest_contract_start_date", G_CON, SRC_CON, "Audit only: start date of that closest contract.")
_d("closest_contract_end_date", G_CON, SRC_CON, "Audit only: end date of that closest contract.")
_d("candidate_contract_detail", G_CON, SRC_CON, "For ambiguous rows (and rows resolved by the each-level price): every tied record as number|start|end|price|ea_price|uom|tier.")
_d("mapped_pricing_tier", G_CON, SRC_CON, "pricing_tier of the selected contract (informational; not used to choose a price).")
_d("ambiguity_driver", G_CON, SRC_PIPE, "For ambiguous rows: what differs between the tied records.",
   "DIFFERENT_CONTRACT_NUMBERS, DIFFERENT_PRICING_TIER, SAME_CONTRACT_AND_TIER_DIFFERENT_END_DATE, SAME_CONTRACT_TIER_AND_END_DATE")
_d("contract_gap_code", G_CON, SRC_PIPE, "Machine-readable reason the contract mapping failed (empty when MATCHED).",
   "VENDOR_NOT_IN_CONTRACTS, ITEM_NOT_IN_CONTRACTS, VENDOR_ITEM_PAIR_NOT_IN_CONTRACTS, VENDOR_AND_ITEM_NOT_IN_CONTRACTS, NOT_ACTIVE_EXPIRED_WITHIN_LOOKBACK, NOT_ACTIVE_EXPIRED_BEFORE_LOOKBACK, NOT_ACTIVE_NOT_YET_STARTED, AMBIGUOUS_MULTIPLE_CONTRACTS, AMBIGUOUS_MULTIPLE_PRICES, MISSING_ITEM_ID, MISSING_VENDOR, MISSING_ITEM_AND_VENDOR, INVALID_CONSUMPTION_DATE, ...")
_d("contract_gap_detail", G_CON, SRC_PIPE, "Plain-English explanation for contract_gap_code (names the vendor, Item ID, nearest contract and dates).")

_d("mapped_validation_price", G_PRICE, SRC_CON, "Mapped price used for validation (the each-level price) - MATCHED rows only. Denominator of the % differences.")
_d("client_vs_mapped_price_diff", G_PRICE, SRC_PIPE, "client_contract_price - mapped_validation_price.")
_d("client_vs_mapped_price_diff_pct", G_PRICE, SRC_PIPE, "(client_contract_price - mapped price) / mapped price (fraction, 0.2 = 20%).")
_d("client_contract_price_validation_flag", G_PRICE, SRC_PIPE, "Review flag (not an error) for the client price vs mapped price. ABOVE_20_PERCENT when |difference| > 20%.",
   "WITHIN_THRESHOLD, ABOVE_20_PERCENT, NOT_COMPARABLE")
_d("supply_vs_mapped_price_diff", G_PRICE, SRC_PIPE, "supply_unit_price - mapped_validation_price.")
_d("supply_vs_mapped_price_diff_pct", G_PRICE, SRC_PIPE, "(supply_unit_price - mapped price) / mapped price (fraction).")
_d("supply_price_validation_flag", G_PRICE, SRC_PIPE, "Review flag for supply unit price vs mapped price.", "WITHIN_THRESHOLD, ABOVE_20_PERCENT, NOT_COMPARABLE")

_d("current_contract_status", G_CUR, SRC_CON, "Is the item contracted TODAY (run date)? NON-CONTRACTED = no active contract, hence no current contract price.",
   "CONTRACTED, CONTRACTED_PRICE_AMBIGUOUS, CONTRACTED_NO_PRICE, NON-CONTRACTED, NOT_ASSESSED")
_d("current_contract_price", G_CUR, SRC_CON, "contract_price (pack price) of the item's active contract on the run date; latest start date, unique price only.")
_d("current_contract_ea_price", G_CUR, SRC_CON, "Each-level price of that current contract.")
_d("current_contract_uom", G_CUR, SRC_CON, "UOM of the current contract price.")
_d("current_contract_number", G_CUR, SRC_CON, "Contract number of the current contract.")
_d("current_contract_vendor", G_CUR, SRC_CON, "Standard vendor that holds the current contract.")
_d("current_contract_start_date", G_CUR, SRC_CON, "Start date of the current contract.")
_d("current_contract_end_date", G_CUR, SRC_CON, "End date of the current contract.")
_d("current_pricing_tier", G_CUR, SRC_CON, "pricing_tier of the current contract (informational).")
_d("current_contract_scope", G_CUR, SRC_PIPE, "SAME_VENDOR = the row's own standard vendor holds the current contract (preferred); OTHER_VENDOR = another vendor's.", "SAME_VENDOR, OTHER_VENDOR")
_d("current_contract_price_count", G_CUR, SRC_PIPE, "Distinct each-level prices on the latest-start active contract (1 = unique).")
_d("non_contract_reason_code", G_CUR, SRC_PIPE, "Why the item is NON-CONTRACTED / NOT_ASSESSED.",
   "NO_CONTRACT_RECORD_FOR_ITEM, ALL_CONTRACTS_EXPIRED, ONLY_FUTURE_CONTRACTS, NO_VALID_CONTRACT_DATES, MISSING_ITEM_ID")
_d("non_contract_reason", G_CUR, SRC_PIPE, "Plain-English explanation (latest contract and end date, or first future start).")
_d("item_contract_category", G_CUR, SRC_CON, "contract_category of the item from the Contracts file (8-digit code + description). One value per item; empty if the item is not in Contracts. Named to differ from the consumption CONTRACT_CATEGORY.")
_d("item_contract_category_code", G_CUR, SRC_CON, "Leading 8-digit code of item_contract_category.")
_d("item_contract_category_description", G_CUR, SRC_CON, "Description part of item_contract_category.")
_d("current_contract_as_of_date", G_CUR, SRC_PIPE, "Date the current contract columns refer to (the run date unless Config.as_of_date is set).")

_d("unmapped_reason_code", G_WHY, SRC_PIPE, "LAST COLUMNS. Code(s) of everything that could not be mapped, prefixed CONTRACT: / ITEM_MASTER: and joined with '; '. Empty = fully mapped (contract MATCHED and UNSPSC MATCHED).")
_d("unmapped_reason", G_WHY, SRC_PIPE, "LAST COLUMN. Plain-English explanation of why the row could not be mapped (which key was missing or did not match). Empty = fully mapped.")
DATA_DICTIONARY = {d[0]: d for d in _D}


def expected_final_columns(raw_columns: List[str]) -> List[str]:
    """Column order of the final dataset: raw consumption, derived, current contract, unmapped reason (last)."""
    derived = [c for c in DERIVED_COLS if c not in FINAL_COLS]
    return list(raw_columns) + derived + CURRENT_COLS + FINAL_COLS


def build_data_dictionary(names: List[str], types: Optional[Dict[str, str]] = None, examples: Optional[Dict[str, str]] = None) -> pd.DataFrame:
    """One row per final-dataset column in final order. Fails if a column has no dictionary entry."""
    missing = [n for n in names if n not in DATA_DICTIONARY]
    assert not missing, f"columns without data-dictionary entry: {missing}"
    rows = []
    for i, n in enumerate(names, start=1):
        _, group, source, desc, values = DATA_DICTIONARY[n]
        rows.append({"position": i, "column": n, "group": group, "source_dataset": source, "data_type": (types or {}).get(n, ""),
                     "description": desc, "allowed_values": values, "example": (examples or {}).get(n, "")})
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------------------
# CSV chunk reader
# --------------------------------------------------------------------------------------
def read_csv_header(path: Path) -> List[str]:
    import csv
    with open(path, "r", encoding="utf-8-sig", errors="replace", newline="") as fh:
        return next(csv.reader(fh, delimiter="|"))


def iter_csv_chunks(path: Path, header: List[str], chunk_rows: int):
    """Stream a CSV as fixed-size Arrow tables (all columns kept as raw strings; blanks stay '')."""
    conv = pacsv.ConvertOptions(column_types={h: pa.string() for h in header}, strings_can_be_null=False)
    rd = pacsv.open_csv(path, read_options=pacsv.ReadOptions(block_size=32 << 20),
                        parse_options=pacsv.ParseOptions(newlines_in_values=True, delimiter="|"), convert_options=conv)
    buf, nbuf = [], 0
    for batch in rd:
        buf.append(batch)
        nbuf += batch.num_rows
        while nbuf >= chunk_rows:
            t = pa.Table.from_batches(buf).combine_chunks()
            yield t.slice(0, chunk_rows)
            rest = t.slice(chunk_rows)
            buf, nbuf = ([b for b in rest.to_batches()] if rest.num_rows else []), rest.num_rows
    if nbuf:
        yield pa.Table.from_batches(buf).combine_chunks()


# --------------------------------------------------------------------------------------
# The pipeline run object (stages are methods so the notebook can call them one by one)
# --------------------------------------------------------------------------------------
class PipelineRun:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.t0 = time.perf_counter()
        self.started = datetime.now()
        cfg.make_dirs()
        setup_logging(cfg)
        if cfg.staging_dir.exists():
            LOG.warning("Found leftover staging directory from an unfinished run - discarding it.")
            shutil.rmtree(cfg.staging_dir, ignore_errors=True)
        cfg.staging_dir.mkdir(parents=True, exist_ok=True)
        self.files: dict = {}
        self.stats = {"files_processed": [], "files_skipped": [], "files_removed": [],
                      "rows_new": 0, "rows_changed": 0, "rows_deleted": 0, "rows_remapped_contract": 0,
                      "rows_remapped_unspsc": 0, "rows_reused": 0}
        self.metrics: dict = {}
        self.outputs: List[str] = []
        self.timings: Dict[str, float] = {}
        self.warnings: List[str] = []
        self.alias: Dict[str, str] = {}
        self.alias_table: Optional[pd.DataFrame] = None

    # ---- 2. discovery -------------------------------------------------------------
    def discover(self):
        cfg = self.cfg
        self.files = discover_input_files(cfg)
        self.prev_manifest = read_json(cfg.state_dir / "input_manifest.json") or {}
        self.prev_state = read_json(cfg.state_dir / "pipeline_state.json") or {}
        prev_files = self.prev_manifest.get("files", {})
        all_paths = self.files["consumption"] + [self.files["contracts"], self.files["item_master"]]
        t = time.perf_counter()
        self.manifest_files = build_file_manifest(all_paths, prev_files, cfg.hash_files)
        self.timings["manifest_hashing"] = time.perf_counter() - t
        LOG.info("Run start | pipeline_version=%s | input_dir=%s", PIPELINE_VERSION, cfg.input_dir)
        LOG.info("Consumption files discovered: %s", [p.name for p in self.files["consumption"]])
        self.removed_files = [n for n, e in prev_files.items()
                              if e.get("role") == "consumption" and n not in self.manifest_files]
        return {p.name: self.manifest_files[p.name]["status"] for p in self.files["consumption"]}

    # ---- 3. loading -----------------------------------------------------------------
    def load_reference(self):
        t = time.perf_counter()
        self.contracts_raw = pd.read_csv(self.files["contracts"], sep='|', dtype=str, keep_default_na=False, encoding="utf-8-sig", encoding_errors="replace")
        self.im_raw = pd.read_csv(self.files["item_master"], sep='|', dtype=str, keep_default_na=False, encoding="utf-8-sig", encoding_errors="replace")
        self.con_cols = detect_columns(list(self.contracts_raw.columns), CONTRACT_COLUMN_CANDIDATES, CONTRACT_REQUIRED, "Contracts")
        self.im_cols = detect_columns(list(self.im_raw.columns), ITEM_MASTER_COLUMN_CANDIDATES, ITEM_MASTER_REQUIRED, "Item Master")
        self.headers = {p.name: read_csv_header(p) for p in self.files["consumption"]}
        ref = self.headers[self.files["consumption"][0].name]
        for n, h in self.headers.items():
            if h != ref:
                raise ValueError(f"{n}: header differs from {self.files['consumption'][0].name}; parts must share one layout")
        self.cons_cols = detect_columns(ref, CONSUMPTION_COLUMN_CANDIDATES, CONSUMPTION_REQUIRED, "Consumption")
        self.raw_columns = ref
        clash = set(ref) & set(DERIVED_COLS)
        if clash:
            raise ValueError(f"Consumption columns clash with derived column names: {clash}")
        LOG.info("Detected Consumption columns: %s", self.cons_cols)
        LOG.info("Detected Contract columns   : %s", self.con_cols)
        LOG.info("Detected Item Master columns: %s", self.im_cols)
        self.timings["load_reference"] = time.perf_counter() - t

    # ---- 5a. vendor standardization ---------------------------------------------------------
    def _consumption_vendor_items(self) -> pd.DataFrame:
        """Distinct (normalized vendor, item id) with row counts over ALL Consumption files: taken from the processed
        state for unchanged files (columnar, fast) and from the raw CSV (two columns only) otherwise."""
        parts = []
        for p in self.files["consumption"]:
            sp = self.state_path(p.name)
            if self.manifest_files[p.name]["status"] == "UNCHANGED" and sp.exists():
                try:
                    d = pq.read_table(sp, columns=[KEY_VENDOR_NORM, KEY_ITEM]).to_pandas().fillna("")
                    d.columns = ["v", "i"]
                    parts.append(d.groupby(["v", "i"]).size().rename("rows").reset_index())
                    continue
                except Exception:  # pragma: no cover - fall back to the CSV
                    pass
            t1 = time.perf_counter()
            parts.append(scan_consumption_vendor_items(p, self.headers[p.name], self.cons_cols))
            LOG.info("  vendor scan %s: %.1fs", p.name, time.perf_counter() - t1)
        return pd.concat(parts).groupby(["v", "i"])["rows"].sum().reset_index()

    def build_vendor_alias(self):
        """Create / extend ``vendor_alias_table.csv`` (one row per distinct vendor spelling in Consumption, Contracts
        and Item Master) and load the effective alias dictionary.
        * New spellings are resolved automatically (exact, core key, VENDOR CODE, item overlap, fuzzy).
        * Existing rows keep their decision; column G (reviewer_standard_vendor) is interpreted: a vendor name = override,
          'ok ...' = approved (cross-checked against vendor codes), 'check vendor code ...' = decided by vendor code.
        * Vendor codes: Contracts and Item Master carry ``vendor_code``; Consumption does not, so its code is derived
          from the Item IDs it consumed (Item Master code first). Delete the file to rebuild from scratch."""
        cfg = self.cfg
        t = time.perf_counter()
        existing = pd.read_csv(cfg.alias_path, sep=',', dtype=str, keep_default_na=False) if cfg.alias_path.exists() else None
        if existing is not None:
            for c in ALIAS_COLUMNS:
                if c not in existing.columns:
                    existing[c] = ""
        known = set(existing["alias_name"]) if existing is not None else set()
        con_v = normalize_vendor(self.contracts_raw[self.con_cols["vendor"]])
        con_rec = con_v[con_v != ""].value_counts()
        has_im_vendor = bool(self.im_cols.get("vendor"))
        im_v = normalize_vendor(self.im_raw[self.im_cols["vendor"]]) if has_im_vendor else pd.Series([], sep='|', dtype=str)
        im_rec = im_v[im_v != ""].value_counts()
        vi = self._consumption_vendor_items()
        cons_rows = vi.groupby("v")["rows"].sum()
        # ---- vendor codes ------------------------------------------------------------------------------------------
        con_item = normalize_item_id(self.contracts_raw[self.con_cols["item_id"]])
        con_code_col = self.con_cols.get("vendor_code")
        im_code_col = self.im_cols.get("vendor_code")
        con_code = self.contracts_raw[con_code_col].fillna("").astype(str).str.strip() if con_code_col else pd.Series("", index=self.contracts_raw.index)
        code_to_base = pd.DataFrame({"v": con_v, "c": con_code})
        code_to_base = code_to_base[(code_to_base.v != "") & (code_to_base.c != "")].groupby("c")["v"].agg(lambda s: s.value_counts().index[0])
        seed = {}
        if existing is not None:
            seed = {a: s for a, s, cr in zip(existing["alias_name"], existing["standard_vendor"], existing["contract_records"])
                    if int(float(cr or 0)) > 0}
        std0, meta0, core_std = cluster_contract_vendors(con_rec, seed)
        code_to_std = {c: std0.get(v, v) for c, v in code_to_base.items()}
        con_codes = pd.DataFrame({"item_id": con_item, "vendor_code": con_code}).query("item_id != '' and vendor_code != ''").drop_duplicates()
        im_codes = pd.DataFrame(columns=["item_id", "vendor_code"])
        im_name_vendor: Dict[str, str] = {}
        if im_code_col:
            imc = self.im_raw[im_code_col].fillna("").astype(str).str.strip()
            im_codes = pd.DataFrame({"item_id": normalize_item_id(self.im_raw[self.im_cols["item_id"]]), "vendor_code": imc}
                                    ).query("item_id != '' and vendor_code != ''").drop_duplicates()
            if has_im_vendor:
                d = pd.DataFrame({"v": im_v, "c": imc})
                d = d[(d.v != "") & (d.c != "")]
                dom = d.groupby("v")["c"].agg(lambda s: s.value_counts().index[0])
                im_name_vendor = {v: code_to_std[c] for v, c in dom.items() if c in code_to_std}
        code_ev = vendor_code_evidence(vi, im_codes, con_codes, code_to_std)
        self.code_ev = code_ev
        # ---- new spellings -----------------------------------------------------------------------------------------------
        unknown = [n for n in dict.fromkeys(list(con_rec.index) + list(cons_rows.index) + list(im_rec.index)) if n and n not in known]
        if unknown or existing is None:
            item_sets: Dict[str, set] = {}
            for v, g in vi.groupby("v"):
                item_sets.setdefault(v, set()).update(i for i in g["i"] if i)
            if has_im_vendor:
                d = pd.DataFrame({"v": im_v, "i": normalize_item_id(self.im_raw[self.im_cols["item_id"]])})
                for v, g in d[(d.v != "") & (d.i != "")].groupby("v"):
                    item_sets.setdefault(v, set()).update(g["i"])
            cv = pd.DataFrame({"v": con_v, "i": con_item})
            cv = cv[(cv.v != "") & (cv.i != "")].drop_duplicates()
            con_items = {i: set(g) for i, g in cv.groupby("i")["v"]}
            LOG.info("Vendor alias: resolving %d new vendor spellings", len(unknown))
            new = resolve_vendor_aliases(unknown, std0, meta0, core_std, item_sets, con_items, code_ev, im_name_vendor, cfg)
            new["needs_review"] = np.where(new["method"].isin(["EXACT", "CONTRACT_VENDOR_ANCHOR", "CORE_KEY"]), "N", "Y")
            new["reviewer_standard_vendor"] = ""
            new["consumption_rows"] = new["alias_name"].map(cons_rows).fillna(0).astype(int)
            new["contract_records"] = new["alias_name"].map(con_rec).fillna(0).astype(int)
            new["item_master_rows"] = new["alias_name"].map(im_rec).fillna(0).astype(int)
            new["datasets"] = ["|".join(x for x, c in (("CONSUMPTION", r.consumption_rows), ("CONTRACTS", r.contract_records),
                                                      ("ITEM_MASTER", r.item_master_rows)) if c > 0) for r in new.itertuples()]
            for c in ("vendor_code_check", "vendor_code_evidence", "review_status"):
                new[c] = ""
            tbl = pd.concat([existing, new], ignore_index=True) if existing is not None else new
        else:
            tbl = existing
        valid_names = set(std0.values()) | set(con_rec.index) | set(tbl["standard_vendor"])
        tbl = apply_reviewer_decisions(tbl[ALIAS_COLUMNS], valid_names, code_ev)
        self.alias_table = tbl[ALIAS_COLUMNS]
        self.alias = effective_alias(self.alias_table, valid_names)
        cfg.alias_hash = alias_hash(self.alias)
        self._write_alias_table()
        n_changed = sum(1 for a, s in self.alias.items() if a != s)
        LOG.info("Vendor alias table: %d spellings -> %d standard vendors (%d re-mapped) | hash %s",
                 len(self.alias_table), len(set(self.alias.values())), n_changed, cfg.alias_hash)
        LOG.info("  reviewer status: %s | vendor-code check: %s",
                 self.alias_table["review_status"].replace("", "(none)").value_counts().to_dict(),
                 self.alias_table.loc[self.alias_table["vendor_code_check"] != "", "vendor_code_check"].value_counts().to_dict())
        self.timings["vendor_alias"] = time.perf_counter() - t
        return self.alias_table

    def _write_alias_table(self):
        tmp = self.cfg.alias_path.with_suffix(".csv.tmp")
        self.alias_table.sort_values(["consumption_rows", "contract_records"], ascending=False, key=lambda s: pd.to_numeric(s, errors="coerce")
                                     ).to_csv(tmp, index=False)
        _atomic_replace(tmp, self.cfg.alias_path)

    # ---- 5b. normalization ---------------------------------------------------------------------
    def normalize_reference(self):
        cfg = self.cfg
        self.contracts_all, self.contracts = normalize_contracts(self.contracts_raw, self.con_cols, cfg, self.alias)
        self.im = normalize_item_master(self.im_raw, self.im_cols)
        self.cindex = build_contract_index(self.contracts)
        LOG.info("Contracts: %d raw rows -> %d unique usable records (%d exact duplicates dropped); %d standard vendors",
                 len(self.contracts_all), len(self.contracts), int(self.contracts_all["is_duplicate"].sum()),
                 self.contracts[KEY_VENDOR].nunique())
        LOG.info("Item Master: %d raw rows -> %d Item IDs (%d ambiguous UNSPSC)", len(self.im_raw), len(self.im),
                 int((self.im["item_master_match_status"] == IM_AMBIG).sum()))

    def plan_incremental(self):
        cfg = self.cfg
        self.force_all, reason = False, None
        self.changed_spellings = set()
        if not self.prev_manifest:
            self.force_all, reason = True, "no previous manifest (first run)"
        elif {k: v for k, v in self.prev_state.get("fingerprint", {}).items() if k != "alias_hash"} != cfg.fingerprint():
            self.force_all, reason = True, "pipeline version / mapping logic / config changed"
        elif not (cfg.state_dir / "contracts_snapshot.parquet").exists() or not (cfg.state_dir / "item_master_snapshot.parquet").exists():
            self.force_all, reason = True, "reference snapshots missing"
        self.force_reason = reason
        if not self.force_all:
            # Vendor alias changes: a CHANGED standard vendor for a spelling already in the processed data re-maps just
            # the rows with that spelling (new spellings from new contract / item-master vendors or new files cost nothing).
            prior = []
            for p_ in self.files["consumption"]:
                sp = self.state_path(p_.name)
                if sp.exists():
                    prior.append(pq.read_table(sp, columns=[KEY_VENDOR_NORM, KEY_VENDOR]).to_pandas().drop_duplicates())
            if prior:
                pm = pd.concat(prior).drop_duplicates().dropna()
                chg = pm[pm[KEY_VENDOR_NORM].map(lambda v: self.alias.get(v, v)) != pm[KEY_VENDOR]]
                if len(chg):
                    self.changed_spellings = set(chg[KEY_VENDOR_NORM])
                    LOG.info("Vendor alias changed for %d already-processed spellings -> only their rows are re-mapped: %s",
                             len(chg), sorted(self.changed_spellings)[:10])
        if self.force_all:
            LOG.info("FULL REBUILD required: %s", reason)
        self.file_actions = {}
        for p in self.files["consumption"]:
            st = self.manifest_files[p.name]["status"]
            has_state = (cfg.proc_dir / f"{p.stem}.parquet").exists()
            self.file_actions[p.name] = "NEW" if (self.force_all or st == "NEW" or not has_state) else st
            LOG.info("  %-32s %-9s (%s)", p.name, self.file_actions[p.name], st)
        if self.removed_files:
            LOG.warning("Consumption files removed since last run: %s (their rows are dropped)", self.removed_files)
        prev_con = prev_im = None
        if not self.force_all:
            prev_con = pd.read_parquet(cfg.state_dir / "contracts_snapshot.parquet")
            prev_im = pd.read_parquet(cfg.state_dir / "item_master_snapshot.parquet").set_index(KEY_ITEM)
        self.contract_windows = detect_contract_changes(prev_con, self.contracts, cfg)
        self.im_changes = detect_item_master_changes(prev_im, self.im)
        self.affected_items = set(self.im_changes[KEY_ITEM]) if self.im_changes is not None else None
        if self.contract_windows is not None:
            w = self.contract_windows
            self.changed_vendors, self.changed_items = set(w[KEY_VENDOR]), set(w[KEY_ITEM])
            LOG.info("Contract changes: %d affected Vendor+Item keys", len(w))
            for r in w.head(20).itertuples(index=False):
                LOG.info("   affected key: %s | %s | window %s -> %s", r[0], r[1], str(r.lo)[:10], str(r.hi)[:10])
        else:
            self.changed_vendors, self.changed_items = set(), set()
        if self.im_changes is not None:
            LOG.info("Item Master changes: %d affected Item IDs %s", len(self.im_changes),
                     self.im_changes[KEY_ITEM].head(20).tolist())
        return self.contract_windows, self.im_changes

    # ---- state helpers ------------------------------------------------------------------
    def state_path(self, name: str) -> Path:
        return self.cfg.proc_dir / f"{Path(name).stem}.parquet"

    def stage_path(self, name: str) -> Path:
        return self.cfg.staging_dir / f"{Path(name).stem}.parquet"

    def effective_path(self, name: str) -> Path:
        s = self.stage_path(name)
        return s if s.exists() else self.state_path(name)

    def _file_needs_update(self, path: Path) -> bool:
        if self.contract_windows is None and self.affected_items is None and not self.changed_spellings:
            return False
        t = pq.read_table(path, columns=KEYS + [KEY_VENDOR_NORM, "consumption_date", "contract_match_status"]).to_pandas()
        if self.changed_spellings and t[KEY_VENDOR_NORM].isin(self.changed_spellings).any():
            return True
        c, i = determine_affected_consumption_rows(t, self.contract_windows, self.affected_items)
        r = reason_affected_mask(t, t["contract_match_status"].to_numpy(), self.contract_windows, self.changed_vendors, self.changed_items)
        return bool(c.any() or i.any() or r.any())

    # ---- 6/7/8/9. incremental consumption processing ----------------------------------------
    def process_consumption(self):
        t0 = time.perf_counter()
        self.row_counts = {}
        for p in self.files["consumption"]:
            act = self.file_actions[p.name]
            prev_path = self.state_path(p.name)
            if act == "UNCHANGED":
                pf = pq.ParquetFile(prev_path)
                if not self._file_needs_update(prev_path):
                    LOG.info("SKIP %s (unchanged, %d rows reused)", p.name, pf.metadata.num_rows)
                    self.stats["files_skipped"].append(p.name)
                    self.stats["rows_reused"] += pf.metadata.num_rows
                    self.row_counts[p.name] = pf.metadata.num_rows
                    continue
            LOG.info("PROCESS %s (%s)", p.name, act)
            t = time.perf_counter()
            n = self._process_file(p, act)
            self.row_counts[p.name] = n
            self.stats["files_processed"].append(p.name)
            LOG.info("  done %s: %d rows in %.1fs", p.name, n, time.perf_counter() - t)
        self.timings["process_consumption"] = time.perf_counter() - t0
        LOG.info("Consumption processing: new=%d changed=%d deleted=%d remapped(contract)=%d remapped(unspsc)=%d reused=%d",
                 self.stats["rows_new"], self.stats["rows_changed"], self.stats["rows_deleted"],
                 self.stats["rows_remapped_contract"], self.stats["rows_remapped_unspsc"], self.stats["rows_reused"])

    def _process_file(self, path: Path, action: str) -> int:
        cfg = self.cfg
        stg = self.stage_path(path.name)
        tmp = stg.with_suffix(".parquet.tmp")
        prev_path = self.state_path(path.name)
        use_prev = action in ("CHANGED", "UNCHANGED") and prev_path.exists() and not self.force_all
        pf = pq.ParquetFile(prev_path) if use_prev else None
        from_csv = action != "UNCHANGED"
        chunks = iter_csv_chunks(path, self.headers[path.name], cfg.chunk_rows) if from_csv else None
        n_groups_prev = pf.num_row_groups if pf else 0
        windows = None if self.force_all else self.contract_windows
        items = None if self.force_all else self.affected_items
        writer, k, total = None, 0, 0
        raw_names = self.raw_columns
        while True:
            if from_csv:
                raw = next(chunks, None)
                if raw is None:
                    break
            else:
                if k >= n_groups_prev:
                    break
                raw = None
            prev_tbl = pf.read_row_group(k) if (pf and k < n_groups_prev) else None
            if raw is None:
                raw = prev_tbl.select(raw_names)
                base = prev_tbl.select(BASE_COLS).to_pandas()
            else:
                base = compute_base(raw, path.name, k * cfg.chunk_rows, self.cons_cols, self.alias)
            n = len(base)
            alias_aff = np.zeros(n, dtype=bool)
            if self.changed_spellings and not self.force_all:   # standard vendor changed for these spellings -> re-key and re-map
                mm = base[KEY_VENDOR_NORM].isin(self.changed_spellings).to_numpy()
                if mm.any():
                    base.loc[mm, KEY_VENDOR] = base.loc[mm, KEY_VENDOR_NORM].map(lambda v: self.alias.get(v, v)).to_numpy()
                    alias_aff = mm
            prev_df = prev_tbl.select(DERIVED_COLS).to_pandas() if prev_tbl is not None else None
            reuse = np.zeros(n, dtype=bool)
            if prev_df is not None:
                m = min(n, len(prev_df))
                if from_csv:
                    reuse[:m] = base["row_hash"].to_numpy()[:m] == prev_df["row_hash"].to_numpy()[:m]
                    self.stats["rows_deleted"] += max(0, len(prev_df) - n)
                    self.stats["rows_changed"] += int((~reuse[:m]).sum())
                    self.stats["rows_new"] += n - m
                else:
                    reuse[:m] = True
            elif from_csv:
                self.stats["rows_new"] += n
            need_src = ~reuse
            c_aff, i_aff = determine_affected_consumption_rows(base, windows, items)
            if prev_df is not None and windows is not None:
                ps = np.full(n, "", dtype=object)
                ps[:len(prev_df)] = prev_df["contract_match_status"].to_numpy()[:n]
                c_aff = c_aff | reason_affected_mask(base, ps, windows, self.changed_vendors, self.changed_items)
            need_c = need_src | ((c_aff | alias_aff) & reuse)
            need_u = need_src | (i_aff & reuse)
            self.stats["rows_remapped_contract"] += int((need_c & ~need_src).sum())
            self.stats["rows_remapped_unspsc"] += int((need_u & ~need_src).sum())
            self.stats["rows_reused"] += int((~(need_c | need_u)).sum())

            def assemble(cols, need, fn):
                """Reused rows come from the previous parquet, rows that need work are recomputed by ``fn``."""
                parts = []
                keep = np.flatnonzero(~need)
                if len(keep):
                    parts.append(prev_df.iloc[keep][cols])
                if need.any():
                    parts.append(fn(np.flatnonzero(need)))
                res = pd.concat(parts) if len(parts) > 1 else parts[0]
                return res.sort_index()

            base_i = base.copy()
            base_i.index = np.arange(n)
            unspsc_fn = lambda pos: map_unspsc(base_i.iloc[pos], self.im)
            cont_fn = lambda pos: self._contract_and_price(base_i.iloc[pos])
            if prev_df is not None:
                u_blk = assemble(UNSPSC_COLS, need_u, unspsc_fn)
                cp_blk = assemble(CONTRACT_COLS + PRICE_COLS, need_c, cont_fn)
            else:
                u_blk, cp_blk = unspsc_fn(np.arange(n)), cont_fn(np.arange(n))
            u_blk, cp_blk = u_blk.reindex(base_i.index), cp_blk.reindex(base_i.index)
            for c in _INT_COLS:  # parquet round trip turns nullable ints into floats
                cp_blk[c] = pd.to_numeric(cp_blk[c], errors="coerce").astype("Int32")
            fin = build_unmapped_reason(u_blk, cp_blk)
            der = pd.concat([base_i, u_blk, cp_blk, fin], axis=1)[DERIVED_COLS]
            der = der.replace({KEY_ITEM: {"": None}, KEY_VENDOR: {"": None}, KEY_VENDOR_NORM: {"": None}})
            dtbl = pa.Table.from_pandas(der, schema=pa.schema(DERIVED_SCHEMA), preserve_index=False)
            out = raw
            for name in dtbl.column_names:
                out = out.append_column(name, dtbl.column(name))
            if writer is None:
                writer = pq.ParquetWriter(tmp, out.schema, compression="zstd")
            writer.write_table(out)
            total += n
            k += 1
        if from_csv and pf is not None and k < n_groups_prev:
            self.stats["rows_deleted"] += sum(pf.metadata.row_group(i).num_rows for i in range(k, n_groups_prev))
        if writer is None:
            raise RuntimeError(f"{path.name}: produced no rows")
        writer.close()
        writer = None
        os.replace(tmp, stg)
        return total

    def _contract_and_price(self, base_sub: pd.DataFrame) -> pd.DataFrame:
        cblk = map_contracts(base_sub, self.contracts, self.cfg, self.cindex)
        pblk = validate_prices(base_sub, cblk, self.cfg)
        return pd.concat([cblk, pblk], axis=1)

    def build_current_lookups(self):
        """Item-level lookups for the CURRENT contract price / category (as of cfg.as_of, default today)."""
        self.as_of = self.cfg.as_of
        self.current_lk = build_current_lookups(self.contracts, self.as_of, self.cfg)
        lk = self.current_lk
        LOG.info("Current contracts as of %s: %d items with an active contract (%d with an ambiguous price); %d items have a category",
                 self.as_of.date(), len(lk.by_item), int((lk.by_item["i_price_count"] > 1).sum()), len(lk.category))
        return lk

    # ---- validation + aggregation pass ------------------------------------------------------------
    def collect_metrics(self):
        """One columnar pass over all processed parquet files: validation assertions, profiling and all
        aggregations needed for KPIs / summaries. Raises AssertionError if a validation check fails."""
        cfg = self.cfg
        t0 = time.perf_counter()
        cols = ["source_file", "source_row_number", KEY_ITEM, KEY_VENDOR, KEY_VENDOR_NORM, "vendor_name_original",
                "consumption_date", "consumption_date_status", "supply_unit_price", "client_contract_price",
                "row_eligibility", "item_master_match_status", "contract_match_status", "contract_gap_code",
                "item_master_gap_code", "client_contract_price_validation_flag", "supply_price_validation_flag",
                "mapped_contract_price", "mapped_contract_ea_price", "unmapped_reason", "contract_gap_detail"]
        aggs, reason_parts, item_parts, vi_parts, im_parts = [], [], [], [], []
        if not hasattr(self, "current_lk"):
            self.build_current_lookups()
        cur_parts, cat_parts, cv_parts, nc_parts, cur_items = [], [], [], [], {}
        items, combos, errors = set(), set(), []
        norm_rows = Counter()
        blanks = {c: 0 for c in ("item_id", "vendor", "date", "supply_unit_price", "client_contract_price")}
        dq = Counter()
        total = 0
        per_file = {}
        for p in self.files["consumption"]:
            pf = pq.ParquetFile(self.effective_path(p.name))
            names = pf.schema_arrow.names
            need = cols + [c for c in ("ITEM_DESCRIPTION",) if c in names]
            expect_next, nfile = 1, 0
            for k in range(pf.num_row_groups):
                df = pf.read_row_group(k, columns=need).to_pandas()
                n = len(df)
                if not (df["source_row_number"].to_numpy() == np.arange(expect_next, expect_next + n)).all():
                    errors.append(f"{p.name}: source_row_number not contiguous in row group {k}")
                expect_next += n
                nfile += n
                if not (df["source_file"] == p.name).all():
                    errors.append(f"{p.name}: foreign source_file value")
                mm = df["contract_match_status"] == ST_MATCHED
                if df.loc[mm, "mapped_contract_price"].isna().any():
                    errors.append(f"{p.name}: MATCHED rows without mapped_contract_price")
                ap = df["contract_match_status"].isin([ST_AMBIG_PRICES, ST_AMBIG_CONTRACTS, ST_NOT_ACTIVE, ST_NOT_FOUND])
                if df.loc[ap, ["mapped_contract_price", "mapped_contract_ea_price"]].notna().any().any():
                    errors.append(f"{p.name}: price present on unresolved/ambiguous rows")
                if not df["contract_match_status"].isin(CONTRACT_STATUSES).all():
                    errors.append(f"{p.name}: unknown contract_match_status")
                if (mm & df["contract_gap_code"].notna()).any() or (~mm & df["contract_gap_code"].isna()).any():
                    errors.append(f"{p.name}: contract_gap_code inconsistent with contract_match_status")
                imm = df["item_master_match_status"] == IM_MATCHED
                if (imm & df["item_master_gap_code"].notna()).any() or (~imm & df["item_master_gap_code"].isna()).any():
                    errors.append(f"{p.name}: item_master_gap_code inconsistent with item_master_match_status")
                blanks["item_id"] += int(df[KEY_ITEM].isna().sum())
                blanks["vendor"] += int(df[KEY_VENDOR].isna().sum())
                blanks["date"] += int(df["consumption_date"].isna().sum())
                blanks["supply_unit_price"] += int(df["supply_unit_price"].isna().sum())
                blanks["client_contract_price"] += int(df["client_contract_price"].isna().sum())
                dq["date_invalid"] += int((df["consumption_date_status"] == "INVALID").sum())
                dq["supply_price_le_0"] += int((df["supply_unit_price"] <= 0).sum())
                dq["nonnumeric_item_rows"] += int((df[KEY_ITEM].notna() & ~df[KEY_ITEM].fillna("").str.fullmatch(r"\d+")).sum())
                dq["leading_zero_item_rows"] += int(df[KEY_ITEM].fillna("").str.match(r"^0\d").sum())
                items.update(df[KEY_ITEM].dropna().unique())
                combos.update((df[KEY_VENDOR].fillna("") + "\x1f" + df[KEY_ITEM].fillna("")).unique())
                norm_rows.update(df[KEY_VENDOR_NORM].fillna("").value_counts().to_dict())
                month = df["consumption_date"].dt.strftime("%Y-%m").fillna("INVALID/MISSING DATE")
                vend = df[KEY_VENDOR].fillna("(MISSING)")
                cb = current_contract_block(df[[KEY_VENDOR, KEY_ITEM]], self.current_lk)
                cb.index = df.index
                cdf = df[["contract_match_status"]].join(cb[["current_contract_status", "non_contract_reason_code", "non_contract_reason",
                                                             "item_contract_category"]].rename(columns={"item_contract_category": "contract_category"})
                                                         ).assign(vendor=vend, item=df[KEY_ITEM])
                cur_parts.append(cdf.groupby(["current_contract_status", "non_contract_reason_code", "contract_match_status"], dropna=False)
                                 .size().rename("rows").reset_index())
                cat_parts.append(cdf.groupby(["contract_category", "current_contract_status"], dropna=False).size().rename("rows").reset_index())
                cv_parts.append(cdf.groupby(["vendor", "current_contract_status"], dropna=False).size().rename("rows").reset_index())
                for st_, g_ in cdf.groupby("current_contract_status")["item"]:
                    cur_items.setdefault(st_, set()).update(g_.dropna().unique())
                ncs = cdf[cdf["current_contract_status"] == ST_NONCONTRACTED]
                if len(ncs):
                    ncs = ncs.assign(item_description=df.loc[ncs.index, "ITEM_DESCRIPTION"] if "ITEM_DESCRIPTION" in df.columns else None)
                    nc_parts.append(ncs.groupby(["vendor", "item", "non_contract_reason_code"], dropna=False).agg(
                        rows=("item", "size"), reason=("non_contract_reason", "first"), item_description=("item_description", "first")).reset_index())
                gk = ["vendor", "month", "row_eligibility", "contract_match_status", "item_master_match_status",
                      "client_contract_price_validation_flag", "supply_price_validation_flag"]
                aggs.append(df.assign(month=month, vendor=vend).groupby(gk, dropna=False, observed=True).size().rename("rows").reset_index())
                unm = df[df["contract_gap_code"].notna()]
                if len(unm):
                    reason_parts.append(unm.assign(vendor=vend[unm.index]).groupby(["vendor", "contract_gap_code"], dropna=False).size().rename("rows").reset_index())
                    desc = "ITEM_DESCRIPTION" if "ITEM_DESCRIPTION" in df.columns else None
                    vi_parts.append(unm.assign(vendor=vend[unm.index]).groupby(
                        ["vendor", KEY_VENDOR_NORM, KEY_ITEM, "contract_gap_code"], dropna=False).agg(
                        rows=("row_eligibility", "size"), detail=("contract_gap_detail", "first"),
                        **({"item_description": (desc, "first")} if desc else {})).reset_index())
                bad_im = df[df["item_master_gap_code"].notna()]
                if len(bad_im):
                    desc = "ITEM_DESCRIPTION" if "ITEM_DESCRIPTION" in df.columns else None
                    im_parts.append(bad_im.groupby([KEY_ITEM, "item_master_gap_code"], dropna=False).agg(
                        consumption_rows=("row_eligibility", "size"), sample_vendor=(KEY_VENDOR_NORM, "first"),
                        **({"item_description": (desc, "first")} if desc else {})).reset_index())
            per_file[p.name] = nfile
            src_rows = self.row_counts.get(p.name)
            if src_rows is not None and src_rows != nfile:
                errors.append(f"{p.name}: processed rows {nfile} != source rows {src_rows}")
            total += nfile
        assert not errors, "VALIDATION FAILED:\n  " + "\n  ".join(errors[:20])
        gk = ["vendor", "month", "row_eligibility", "contract_match_status", "item_master_match_status",
              "client_contract_price_validation_flag", "supply_price_validation_flag"]
        agg = pd.concat(aggs, ignore_index=True).groupby(gk, dropna=False)["rows"].sum().reset_index()
        assert int(agg["rows"].sum()) == total, "aggregation does not reconcile with row count"
        self.agg, self.total_rows, self.rows_per_file = agg, total, per_file
        self.norm_vendor_rows = norm_rows
        self.contract_reason_by_vendor = (pd.concat(reason_parts).groupby(["vendor", "contract_gap_code"])["rows"].sum().reset_index()
                                          if reason_parts else pd.DataFrame(columns=["vendor", "contract_gap_code", "rows"]))
        self.top_vendor_items = (pd.concat(vi_parts).groupby(["vendor", KEY_VENDOR_NORM, KEY_ITEM, "contract_gap_code"], dropna=False).agg(
            rows=("rows", "sum"), detail=("detail", "first"), **({"item_description": ("item_description", "first")} if "item_description" in vi_parts[0] else {})).reset_index()
            if vi_parts else pd.DataFrame())
        self.item_master_gaps = (pd.concat(im_parts).groupby([KEY_ITEM, "item_master_gap_code"], dropna=False).agg(
            consumption_rows=("consumption_rows", "sum"), sample_vendor=("sample_vendor", "first"),
            **({"item_description": ("item_description", "first")} if "item_description" in im_parts[0] else {})).reset_index()
            if im_parts else pd.DataFrame())
        self.profile = {"total_rows": total, "unique_item_ids": len(items), "unique_vendor_item_combinations": len(combos),
                        "blank_counts": blanks, "dq": dict(dq)}
        self.metrics = calculate_mapping_metrics(agg, cfg, self.profile)
        # current contract status (item level, as of cfg.as_of)
        self.current_status = pd.concat(cur_parts).groupby(["current_contract_status", "non_contract_reason_code", "contract_match_status"],
                                                           dropna=False)["rows"].sum().reset_index()
        self.current_cat = pd.concat(cat_parts).groupby(["contract_category", "current_contract_status"], dropna=False)["rows"].sum().reset_index()
        self.current_vendor = pd.concat(cv_parts).groupby(["vendor", "current_contract_status"], dropna=False)["rows"].sum().reset_index()
        self.current_items = {k_: len(v_) for k_, v_ in cur_items.items()}
        self.noncontracted_items = (pd.concat(nc_parts).groupby(["vendor", "item", "non_contract_reason_code"], dropna=False).agg(
            rows=("rows", "sum"), reason=("reason", "first"), item_description=("item_description", "first")).reset_index()
            if nc_parts else pd.DataFrame(columns=["vendor", "item", "non_contract_reason_code", "rows", "reason", "item_description"]))
        cs = self.current_status.groupby("current_contract_status")["rows"].sum()
        assert int(cs.sum()) == total, "current-contract status counts do not add up to total rows"
        for st_ in (ST_CONTRACTED, ST_CONTRACTED_AMBIG, ST_CONTRACTED_NOPRICE, ST_NONCONTRACTED, ST_NOT_ASSESSED):
            self.metrics[f"Current Contract - {st_} Rows"] = int(cs.get(st_, 0))
            self.metrics[f"Current Contract - {st_} Distinct Items"] = int(self.current_items.get(st_, 0))
            self.metrics[f"% of Total Rows - Current Contract {st_}"] = round(100.0 * int(cs.get(st_, 0)) / total, 4) if total else 0.0
        self.timings["collect_metrics"] = time.perf_counter() - t0
        LOG.info("Validation passed. Total rows=%d | eligible=%d | matched=%d | match rate=%.2f%%",
                 total, self.metrics["Total Eligible Rows"], self.metrics["Successfully Matched Rows"],
                 self.metrics["Contract Match Rate %"])
        return self.metrics

    # ---- Contract data quality ------------------------------------------------------------------------------
    def contract_data_quality(self):
        """Profile Contracts before mapping. Returns a long DQ table; details kept for the workbook."""
        cfg = self.cfg
        a, d = self.contracts_all, self.contracts
        rows = []
        add = lambda m, v, note="": rows.append({"dataset": "Contracts", "metric": m, "value": v, "note": note})
        add("Contract raw records", len(a))
        add("Exact duplicate records", int(a["is_duplicate"].sum()), "same vendor, item, contract number, dates, UOM, prices - dropped")
        add("Records missing vendor or item id", int(((a[KEY_VENDOR] == "") | (a[KEY_ITEM] == "")).sum()), "excluded from matching")
        add("Missing Contract Start Date (blank)", int((a["start_status"] == "MISSING").sum()))
        add("Invalid / unparseable Contract Start Date", int((a["start_status"] == "INVALID").sum()), "e.g. year 0201; record can never be ACTIVE")
        add("Missing Contract End Date (blank)", int((a["end_status"] == "MISSING").sum()),
            "treated as open-ended" if cfg.null_end_date_open_ended else "treated as invalid")
        add("Invalid / unparseable Contract End Date", int((a["end_status"] == "INVALID").sum()))
        add("Start date year < 2000 (suspicious)", int((a["contract_start"].dt.year < 2000).sum()))
        add("End date before start date", int((d["contract_end"] < d["contract_start"]).sum()))
        add("Missing Contract Price", int(a["contract_price"].isna().sum()))
        add("Missing Contract EA Price", int(a["contract_ea_price"].isna().sum()))
        add("Missing contract category (blank)", int((a["contract_category"] == "").sum()))
        add("Items with more than one contract category", int((d[d["contract_category"] != ""].groupby(KEY_ITEM)["contract_category"].nunique() > 1).sum()))
        add("Missing pricing tier (blank)", int((a["pricing_tier"] == "").sum()), f"{100 * (a['pricing_tier'] == '').mean():.1f}% of records")
        add("Distinct pricing-tier labels", a["pricing_tier"].nunique(), "includes free-text labels such as LOCAL / NON CONTRACT / OFF CONTRACT; tier_requirements column is empty")
        if self.con_cols.get("vendor_code"):
            vc = self.contracts_raw[self.con_cols["vendor_code"]].fillna("").astype(str).str.strip()
            vcd = pd.DataFrame({"v": a[KEY_VENDOR_NORM], "c": vc}).query("c != ''")
            add("Missing vendor code", int((vc == "").sum()))
            add("Vendor names with more than one vendor code", int((vcd.groupby("v")["c"].nunique() > 1).sum()))
            add("Vendor codes shared by more than one vendor name", int((vcd.groupby("c")["v"].nunique() > 1).sum()))
        add("Distinct vendor spellings (raw)", a[KEY_VENDOR_NORM].nunique())
        add("Distinct standard vendors", d[KEY_VENDOR].nunique(), "after vendor alias standardization")
        add("Distinct Vendor+Item keys", len(d[KEYS].drop_duplicates()))
        add("Distinct Item IDs", d[KEY_ITEM].nunique())
        g_item = d.groupby(KEY_ITEM)["contract_number"].nunique()
        add("Item IDs linked to multiple contract numbers", int((g_item > 1).sum()))
        g_ki = d.groupby(KEYS)
        n_con = g_ki["contract_number"].nunique()
        add("Vendor+Item keys linked to multiple contract numbers", int((n_con > 1).sum()))
        n_pr = g_ki["contract_price"].nunique()
        add("Vendor+Item keys with multiple distinct contract prices", int((n_pr > 1).sum()))
        n_ea = g_ki["contract_ea_price"].nunique()
        add("Vendor+Item keys with multiple distinct EA prices", int((n_ea > 1).sum()))
        n_uom = g_ki["contract_uom"].nunique()
        add("Vendor+Item keys with multiple UOMs", int((n_uom > 1).sum()))
        same_uom = d.groupby(KEYS + ["contract_uom"])["contract_price"].nunique()
        add("Vendor+Item+UOM combinations with multiple prices", int((same_uom > 1).sum()))
        v = d[d["contract_start"].notna()].sort_values(KEYS + ["contract_start", "contract_end"]).copy()
        v["_end"] = v["contract_end"].fillna(FAR_FUTURE)
        v["_prev_max_end"] = v.groupby(KEYS)["_end"].cummax().groupby([v[KEY_VENDOR], v[KEY_ITEM]]).shift()
        v["overlaps_previous"] = v["contract_start"] <= v["_prev_max_end"]
        ov_keys = v[v["overlaps_previous"]][KEYS].drop_duplicates()
        add("Records overlapping an earlier record of the same Vendor+Item", int(v["overlaps_previous"].sum()))
        add("Vendor+Item keys with overlapping validity periods", len(ov_keys))
        self.contract_dq = pd.DataFrame(rows)
        multi = pd.DataFrame({"contract_numbers": n_con, "distinct_contract_prices": n_pr, "distinct_ea_prices": n_ea,
                              "uoms": n_uom, "records": g_ki.size()})
        self.dq_multi = (multi[(multi.distinct_contract_prices > 1) | (multi.distinct_ea_prices > 1) | (multi.uoms > 1) |
                               (multi.contract_numbers > 1)].reset_index().sort_values("records", ascending=False))
        bad = a[(a["start_status"] != "VALID") | (a["end_status"] == "INVALID") | a["contract_price"].isna() | a["is_duplicate"]]
        self.dq_invalid = bad[["vendor_name_original", KEY_VENDOR, KEY_ITEM, "contract_number", "contract_start", "contract_end",
                               "start_status", "end_status", "contract_price", "contract_ea_price", "is_duplicate"]]
        return self.contract_data_quality_frame()

    def contract_data_quality_frame(self):
        return self.contract_dq

    def consumption_and_item_master_dq(self) -> pd.DataFrame:
        """DQ rows for Consumption and Item Master (needs collect_metrics first)."""
        m, pr, dq = self.metrics, self.profile, self.profile["dq"]
        tot = max(pr["total_rows"], 1)
        rows = []
        add = lambda ds, metric, v, note="": rows.append({"dataset": ds, "metric": metric, "value": v, "note": note})
        add("Consumption", "Total rows", pr["total_rows"])
        for k, v in pr["blank_counts"].items():
            add("Consumption", f"Blank / invalid {k}", v, f"{100 * v / tot:.2f}% of rows")
        add("Consumption", "Unparseable admit dates (non-blank)", dq.get("date_invalid", 0))
        add("Consumption", "Supply unit price <= 0", dq.get("supply_price_le_0", 0))
        add("Consumption", "Non-numeric Item IDs (rows)", dq.get("nonnumeric_item_rows", 0), "internal / non-catalog style IDs are not in Item Master")
        add("Consumption", "Item IDs with leading zeros (rows)", dq.get("leading_zero_item_rows", 0), "preserved as-is")
        add("Consumption", "Unique Item IDs", pr["unique_item_ids"])
        add("Consumption", "Unique standard Vendor + Item combinations", pr["unique_vendor_item_combinations"])
        add("Consumption", "Distinct vendor spellings (normalized)", len(self.norm_vendor_rows))
        add("Consumption", "Distinct standard vendors", self.agg["vendor"].nunique())
        add("Item Master", "Raw rows", len(self.im_raw))
        add("Item Master", "Distinct Item IDs", len(self.im))
        add("Item Master", "Item IDs with multiple UNSPSC", int((self.im["item_master_match_status"] == IM_AMBIG).sum()))
        add("Item Master", "Item IDs with blank UNSPSC", int((self.im["item_master_match_status"] == IM_UNSPSC_MISSING).sum()))
        add("Item Master", "Duplicate Item ID rows", int(len(self.im_raw) - len(self.im)))
        add("Vendor alias", "Vendor spellings in alias table", len(self.alias_table))
        add("Vendor alias", "Spellings re-mapped to a different standard name", sum(1 for a, s in self.alias.items() if a != s))
        add("Vendor alias", "Re-mapped by item-overlap / fuzzy (needs review)", int(self.alias_table["needs_review"].eq("Y").sum()))
        add("Vendor alias", "Spellings with no contracts vendor (NO_MATCH)", int(self.alias_table["method"].eq("NO_MATCH").sum()))
        return pd.DataFrame(rows)

    # ---- Ambiguity investigation: does UOM / pricing tier help? -------------------------------------------------------------
    def analyze_ambiguity(self):
        """Quantify how far UOM and pricing tier (and simple tie-break rules) could reduce contract/price ambiguity.

        Candidate rows = rows that were ambiguous under the strict test (price, EA price, UOM all equal) - i.e. the rows now
        AMBIGUOUS_* plus those resolved by the each-level-price rule. For each (vendor, item, date) combination the tied
        contract records are rebuilt and every rule is scored. Rules that *choose* a price are tested against the
        client-provided Contract Price (informational only - it is never used to assign a price)."""
        t0 = time.perf_counter()
        cols = [KEY_VENDOR, KEY_ITEM, "consumption_date", "contract_match_status", "contract_selection_reason",
                "ambiguity_driver", "client_contract_price"]
        first = pq.ParquetFile(self.effective_path(self.files["consumption"][0].name)).schema_arrow.names
        if "ITEM_UOM" in first:
            cols.append("ITEM_UOM")
        same_ea = "SAME_EACH_PRICE_ACROSS_TIED_RECORDS_PACK_UOM_DIFFERS"
        parts = []
        for p in self.files["consumption"]:
            pf = pq.ParquetFile(self.effective_path(p.name))
            for k in range(pf.num_row_groups):
                d = pf.read_row_group(k, columns=cols).to_pandas()
                d = d[d["contract_match_status"].isin([ST_AMBIG_CONTRACTS, ST_AMBIG_PRICES]) | (d["contract_selection_reason"] == same_ea)]
                if len(d):
                    parts.append(d)
        empty = pd.DataFrame(columns=["scenario", "rows", "pct_of_candidate_rows", "combos", "applied", "comment"])
        if not parts:
            self.ambiguity_scenarios, self.ambiguity_drivers, self.ambiguity_rules = empty, empty.iloc[:, :0], empty.iloc[:, :0]
            return
        amb = pd.concat(parts, ignore_index=True)
        if "ITEM_UOM" not in amb:
            amb["ITEM_UOM"] = ""
        g = amb.groupby([KEY_VENDOR, KEY_ITEM, "consumption_date"]).agg(
            rows=("contract_match_status", "size"), status=("contract_match_status", "first"),
            driver=("ambiguity_driver", "first"), client=("client_contract_price", "median"),
            uom=("ITEM_UOM", "first")).reset_index()
        g["cid"] = np.arange(len(g))
        con = self.contracts[KEYS + ["contract_number", "contract_start", "contract_end", "contract_uom", "contract_price",
                                     "contract_ea_price", "pricing_tier"]]
        pr = g.merge(con, on=KEYS)
        cd = pr["consumption_date"]
        pr = pr[pr["contract_start"].notna() & (pr["contract_start"] <= cd) &
                ((pr["contract_end"] >= cd) | (pr["contract_end"].isna() & self.cfg.null_end_date_open_ended))]
        pr = pr[pr["contract_start"] == pr.groupby("cid")["contract_start"].transform("max")]
        pr = pr[pr["contract_price"].notna()].copy()
        pr["ea"] = pr["contract_ea_price"].round(6)
        pr["tuple"] = pr["contract_price"].round(6).astype(str) + "|" + pr["ea"].astype(str) + "|" + pr["contract_uom"]
        umap = {"EACH": "EA", "CASE": "CA", "ROLL": "RL", "PACKAGE": "PK", "BOX": "BX", "BAG": "BG", "PAIR": "PR", "BOTTLE": "BO", "TRAY": "TY"}
        pr["uom_ok"] = pr["uom"].map(umap).fillna("?") == pr["contract_uom"]
        nun = lambda df, c: df.groupby("cid")[c].nunique()
        b = g.set_index("cid")[["rows", "status", "driver"]].copy()
        b["n_tuple"], b["n_ea"] = nun(pr, "tuple"), nun(pr, "ea")
        pu = pr[pr["uom_ok"]]
        b["has_uom"] = pu.groupby("cid").size().reindex(b.index).fillna(0) > 0
        b["n_tuple_uom"] = nun(pu, "tuple").reindex(b.index).fillna(0)
        b["n_ea_uom"] = nun(pu, "ea").reindex(b.index).fillna(0)
        tot, combos = int(b["rows"].sum()), len(b)
        S = lambda m_: (int(b.loc[m_, "rows"].sum()), int(m_.sum()))
        resolved_ea = b["n_ea"] == 1
        uom_only = (~resolved_ea) & b["has_uom"] & (b["n_tuple_uom"] == 1)
        # per-tier price uniqueness among the residual combos
        resid = b.index[~resolved_ea]
        rp = pr[pr["cid"].isin(resid)]
        per_tier = rp.groupby(["cid", "pricing_tier"])["ea"].nunique().groupby("cid").max().reindex(b.index)
        multi_tier = nun(rp, "pricing_tier").reindex(b.index).fillna(0) > 1
        tier_only = (~resolved_ea) & multi_tier & (per_tier == 1)
        same_tier_conflict = (~resolved_ea) & (per_tier > 1)
        rows = []
        add = lambda name, m_, applied, comment: rows.append({
            "scenario": name, "rows": S(m_)[0], "pct_of_candidate_rows": round(100 * S(m_)[0] / max(tot, 1), 2),
            "combos": S(m_)[1], "applied": applied, "comment": comment})
        allm = pd.Series(True, index=b.index)
        add("A. Ambiguous under strict test (price, EA price and UOM must all be equal)", allm, "baseline", "Baseline for the rows below")
        add("B. Candidates share the same each-level (EA) price - differ only in pack size / UOM", resolved_ea, "YES",
            "Price used for validation is unambiguous -> MATCHED (reason SAME_EACH_PRICE_ACROSS_TIED_RECORDS_PACK_UOM_DIFFERS)")
        add("C. Consumption UOM matches exactly one candidate UOM (would 'choose' a price)", uom_only, "NO",
            "Not applied: see rule test below - choosing by UOM disagrees with the client price most of the time")
        add("D. Candidates differ ONLY by pricing tier (one price per tier)", tier_only, "NO",
            "Tier requirements are blank in Contracts, so the applicable tier cannot be determined")
        add("E. Same contract AND same tier still carry different prices (data conflict)", same_tier_conflict, "NO",
            "Typically overlapping price records of one contract with different end dates")
        add("F. Still ambiguous after applied rules", ~resolved_ea, "-", "Equals AMBIGUOUS_MULTIPLE_PRICES + AMBIGUOUS_MULTIPLE_CONTRACTS rows")
        self.ambiguity_scenarios = pd.DataFrame(rows)
        # residual drivers (from the mapped output)
        dr = b.loc[~resolved_ea].groupby("driver", dropna=False).agg(rows=("rows", "sum"), combos=("rows", "size")).reset_index()
        dr["pct_of_residual_rows"] = (100 * dr["rows"] / max(dr["rows"].sum(), 1)).round(2)
        self.ambiguity_drivers = dr.sort_values("rows", ascending=False)
        # rule scoring vs client price (informational)
        q = pr[pr["cid"].isin(resid)].copy()
        q["client"] = q["cid"].map(g.set_index("cid")["client"])
        den = lambda s: s.replace(0, np.nan)
        q["hit"] = (((q["contract_price"] - q["client"]).abs() / den(q["contract_price"])) < 0.005) | \
                   (((q["ea"] - q["client"]).abs() / den(q["ea"])) < 0.005)
        nh = q.groupby("cid")["hit"].sum()
        uniq = nh[nh == 1].index
        rules = [{"rule": "(reference) client price equals exactly one candidate", "decisive_combos": len(uniq),
                  "decisive_rows": int(b.loc[uniq, "rows"].sum()), "agrees_with_client_price_pct": None}]
        far = int(FAR_FUTURE.value)

        def score(name, keyfn, asc, restrict=None):
            s = q[q["cid"].isin(uniq)]
            if restrict is not None:
                s = s[restrict(s)]
            if s.empty:
                return
            s = s.assign(_k=keyfn(s)).sort_values(["cid", "_k"], ascending=[True, asc])
            top = s.groupby("cid")["_k"].transform("first")
            w = s[s["_k"] == top]
            dec = w.groupby("cid")["ea"].nunique()
            dec = dec[dec == 1].index
            win = w[w["cid"].isin(dec)].drop_duplicates("cid")
            rules.append({"rule": name, "decisive_combos": len(dec), "decisive_rows": int(b.loc[dec, "rows"].sum()),
                          "agrees_with_client_price_pct": round(100 * win["hit"].mean(), 1) if len(win) else None})
        score("Latest contract end date", lambda d: d["contract_end"].fillna(FAR_FUTURE).astype("int64"), False)
        score("Earliest contract end date", lambda d: d["contract_end"].fillna(FAR_FUTURE).astype("int64"), True)
        score("Highest each-level price", lambda d: d["ea"], False)
        score("Lowest each-level price", lambda d: d["ea"], True)
        score("Lowest pricing-tier number", lambda d: d["pricing_tier"].str.extract(r"(\d+)")[0].astype(float).fillna(99), True)
        score("Consumption UOM equals contract UOM", lambda d: (~d["uom_ok"]).astype(int), True,
              restrict=lambda d: d["cid"].isin(b.index[b["has_uom"] & (b["n_ea"] > 1)]))
        self.ambiguity_rules = pd.DataFrame(rules)
        self.timings["analyze_ambiguity"] = time.perf_counter() - t0
        LOG.info("Ambiguity analysis: %d candidate rows; resolved by each-level price %d; residual %d",
                 tot, S(resolved_ea)[0], S(~resolved_ea)[0])

    # ---- 10-11. Excel deliverables --------------------------------------------------------------------------------------
    @staticmethod
    def _sheet(xw, df: pd.DataFrame, name: str, widths: Optional[Dict[str, int]] = None):
        df.to_excel(xw, sheet_name=name, index=False)
        ws = xw.sheets[name]
        ws.freeze_panes(1, 0)
        if len(df.columns):
            ws.autofilter(0, 0, max(len(df), 1), len(df.columns) - 1)
        for i, c in enumerate(df.columns):
            w = (widths or {}).get(c)
            if w is None:
                sample = df[c].head(200).astype(str).str.len().max() if len(df) else 0
                w = int(min(max(len(str(c)), sample if pd.notna(sample) else 0) + 2, 60))
            ws.set_column(i, i, w)

    def _ambiguity_sheet(self, xw):
        """Sheet 'Ambiguity Analysis': three stacked tables (scenarios, residual drivers, tie-break rule test)."""
        name = "Ambiguity Analysis"
        ws = xw.book.add_worksheet(name)
        xw.sheets[name] = ws
        bold = xw.book.add_format({"bold": True, "font_size": 12})
        hdr = xw.book.add_format({"bold": True, "bg_color": "#DDEBF7", "border": 1})
        row = 0
        blocks = [("1. Can UOM / pricing tier reduce ambiguity? (rows that were ambiguous under the strict test)", self.ambiguity_scenarios),
                  ("2. What still differs between tied records after the applied rule (residual ambiguous rows)", self.ambiguity_drivers),
                  ("3. Tie-break rules scored against the client-provided Contract Price (information only, none applied)", self.ambiguity_rules)]
        for title, df in blocks:
            ws.write(row, 0, title, bold)
            row += 1
            for j, c in enumerate(df.columns):
                ws.write(row, j, c, hdr)
            for i, r in enumerate(df.itertuples(index=False), start=row + 1):
                for j, v in enumerate(r):
                    if v is None or (isinstance(v, float) and np.isnan(v)):
                        ws.write_blank(i, j, None)
                    else:
                        ws.write(i, j, v.item() if hasattr(v, "item") else v)
            row += len(df) + 3
        ws.set_column(0, 0, 88)
        ws.set_column(1, 4, 20)
        ws.set_column(5, 5, 100)

    def _stacked_sheet(self, xw, name: str, blocks: List[Tuple[str, pd.DataFrame]], widths: Tuple[int, int] = (60, 22)):
        """Several small tables stacked on one worksheet (title row, header row, data, two blank rows)."""
        ws = xw.book.add_worksheet(name)
        xw.sheets[name] = ws
        bold = xw.book.add_format({"bold": True, "font_size": 12})
        hdr = xw.book.add_format({"bold": True, "bg_color": "#DDEBF7", "border": 1})
        row = 0
        for title, df in blocks:
            ws.write(row, 0, title, bold)
            row += 1
            for j, c in enumerate(df.columns):
                ws.write(row, j, str(c), hdr)
            for i, r in enumerate(df.itertuples(index=False), start=row + 1):
                for j, v in enumerate(r):
                    if v is None or (isinstance(v, float) and np.isnan(v)):
                        ws.write_blank(i, j, None)
                    else:
                        ws.write(i, j, v.item() if hasattr(v, "item") else v)
            row += len(df) + 3
        ws.set_column(0, 0, widths[0])
        ws.set_column(1, 12, widths[1])

    def _current_contract_sheets(self, xw):
        """Sheets for the CURRENT contract price / category / NON-CONTRACTED tag (item level, as of the run date)."""
        tot = max(self.metrics["Total Consumption Rows"], 1)
        cs = self.current_status.groupby("current_contract_status")["rows"].sum()
        meaning = {ST_CONTRACTED: "Item has an active contract today with one unique price -> current price reported",
                   ST_CONTRACTED_AMBIG: "Active contract today but several different prices on the latest start date -> price left blank",
                   ST_CONTRACTED_NOPRICE: "Active contract today but it carries no price",
                   ST_NONCONTRACTED: "No active contract (and therefore no current contract price) for the item today",
                   ST_NOT_ASSESSED: "Consumption row has no Item ID"}
        status = pd.DataFrame([{"current_contract_status": k, "rows": int(cs.get(k, 0)), "pct_of_all_rows": round(100 * int(cs.get(k, 0)) / tot, 2),
                                "distinct_items": int(self.current_items.get(k, 0)), "meaning": meaning[k]} for k in meaning])
        rs = self.current_status[self.current_status["non_contract_reason_code"].notna()].groupby("non_contract_reason_code")["rows"].sum().reset_index()
        rs["pct_of_all_rows"] = (100 * rs["rows"] / tot).round(2)
        rs["meaning"] = rs["non_contract_reason_code"].map(NONCONTRACT_INFO)
        rs = rs.sort_values("rows", ascending=False)
        cross = self.current_status.pivot_table(index="contract_match_status", columns="current_contract_status", values="rows",
                                                aggfunc="sum", fill_value=0).reset_index()
        cross["total"] = cross.drop(columns="contract_match_status").sum(axis=1)
        blocks = [(f"1. Current contract status as of {self.as_of.date()} (rows of the final dataset; item-level, independent of consumption date)", status),
                  ("2. Why NON-CONTRACTED (no active contract today)", rs),
                  ("3. Historical mapping status (at consumption date) vs current contract status", cross)]
        self._stacked_sheet(xw, "Current Contract Status", blocks, widths=(70, 24))
        cat = self.current_cat.copy()
        cat["contract_category"] = cat["contract_category"].fillna("(no category: item not in Contracts)")
        cp = cat.pivot_table(index="contract_category", columns="current_contract_status", values="rows", aggfunc="sum", fill_value=0).reset_index()
        cp["total_rows"] = cp.drop(columns="contract_category").sum(axis=1)
        cp["pct_non_contracted"] = (100 * cp.get(ST_NONCONTRACTED, 0) / cp["total_rows"]).round(1)
        cp.insert(1, "category_code", cp["contract_category"].str.extract(r"^(\d{6,8})")[0])
        self._sheet(xw, cp.sort_values("total_rows", ascending=False), "Category Summary", {"contract_category": 70})
        vp = self.current_vendor.pivot_table(index="vendor", columns="current_contract_status", values="rows", aggfunc="sum", fill_value=0).reset_index()
        vp["total_rows"] = vp.drop(columns="vendor").sum(axis=1)
        vp["pct_non_contracted"] = (100 * vp.get(ST_NONCONTRACTED, 0) / vp["total_rows"]).round(1)
        self._sheet(xw, vp.sort_values("total_rows", ascending=False), "Contract Status by Vendor", {"vendor": 42})
        nci = self.noncontracted_items.sort_values("rows", ascending=False).head(15000)
        self._sheet(xw, nci, "Non-Contracted Items", {"reason": 90, "item_description": 40, "vendor": 36, "non_contract_reason_code": 34})

    def write_exceptions(self):
        """One workbook (output/exceptions/review_exceptions.xlsx) with the row-level review lists that are small
        enough for Excel. ALL unmatched rows (incl. CONTRACT_NOT_FOUND) are in the final dataset - filter on
        contract_match_status / unmapped_reason_code."""
        cfg = self.cfg
        t0 = time.perf_counter()
        first = pq.ParquetFile(self.effective_path(self.files["consumption"][0].name))
        names = first.schema_arrow.names
        ctx = [c for c in CONSUMPTION_CONTEXT_COLUMNS if c in names]
        id_cols = ["row_id"] + [c for c in ("LOG_ID", "ACCOUNT_NUMBER") if c in ctx]
        item_cols = [KEY_ITEM] + [c for c in ("ITEM_DESCRIPTION", "ITEM_UOM") if c in ctx]
        vend = ["vendor_name_original", KEY_VENDOR, "consumption_date"]
        audit = ["contract_match_status", "unmapped_reason", "contract_candidate_count", "active_contract_count", "ambiguity_driver"]
        pv = ["supply_unit_price", "client_contract_price", "mapped_contract_number", "mapped_contract_price",
              "mapped_contract_ea_price", "mapped_contract_uom", "mapped_pricing_tier", "mapped_validation_price", "client_vs_mapped_price_diff",
              "client_vs_mapped_price_diff_pct", "client_contract_price_validation_flag", "supply_vs_mapped_price_diff",
              "supply_vs_mapped_price_diff_pct", "supply_price_validation_flag"]
        specs = {
            "Ambiguous Contracts": (lambda d: d["contract_match_status"] == ST_AMBIG_CONTRACTS, id_cols + item_cols + vend + audit + ["candidate_contract_detail"]),
            "Ambiguous Prices": (lambda d: d["contract_match_status"] == ST_AMBIG_PRICES, id_cols + item_cols + vend + audit + ["candidate_contract_detail"]),
            "Contract Not Active": (lambda d: d["contract_match_status"] == ST_NOT_ACTIVE, id_cols + item_cols + vend + audit + ["closest_contract_number", "closest_contract_start_date", "closest_contract_end_date"]),
            "Price Validation": (lambda d: (d["client_contract_price_validation_flag"] == cfg.above_label) | (d["supply_price_validation_flag"] == cfg.above_label),
                                 id_cols + item_cols + vend + pv),
        }
        need_cols = sorted({c for _, c_ in specs.values() for c in c_} | {"contract_match_status", "client_contract_price_validation_flag",
                                                                            "supply_price_validation_flag"})
        acc = {k: [] for k in specs}
        for p in self.files["consumption"]:
            pf = pq.ParquetFile(self.effective_path(p.name))
            for k in range(pf.num_row_groups):
                df = pf.read_row_group(k, columns=need_cols).to_pandas()
                for name, (pred, cols_) in specs.items():
                    sub = df.loc[pred(df), cols_]
                    if len(sub):
                        acc[name].append(sub)
        frames = {k: (pd.concat(v, ignore_index=True) if v else pd.DataFrame(columns=specs[k][1])) for k, v in acc.items()}
        m = self.metrics
        chk = {"Ambiguous Contracts": m["Ambiguous Contract Matches"], "Ambiguous Prices": m["Multiple-Price / Ambiguous Price Matches"],
               "Contract Not Active": m["Contract Not Active / Applicable"]}
        for k, exp in chk.items():
            assert len(frames[k]) == exp, f"exception sheet {k}: {len(frames[k])} rows != metric {exp}"
        assert len(frames["Price Validation"]) >= max(m["Client Contract Price >20% Exceptions"], m["Supply Unit Price >20% Exceptions"])
        # candidate contracts behind ambiguous keys
        amb_keys = pd.concat([frames["Ambiguous Contracts"][KEYS], frames["Ambiguous Prices"][KEYS]]).drop_duplicates() \
            if len(frames["Ambiguous Contracts"]) + len(frames["Ambiguous Prices"]) else pd.DataFrame(columns=KEYS)
        cand = self.contracts.merge(amb_keys, on=KEYS, how="inner").sort_values(KEYS + ["contract_start"])
        cand = cand[[KEY_VENDOR, KEY_ITEM, "vendor_name_original", "contract_number", "contract_start", "contract_end",
                     "contract_uom", "contract_price", "contract_ea_price"]]
        path = cfg.exc_dir / "review_exceptions.xlsx"
        tmp = path.with_suffix(".xlsx.tmp")
        with pd.ExcelWriter(tmp, engine="xlsxwriter", datetime_format="yyyy-mm-dd") as xw:
            readme = pd.DataFrame({"sheet": list(frames) + ["Candidate Contracts"],
                                   "rows": [len(f) for f in frames.values()] + [len(cand)],
                                   "what it is": ["Rows where several active contracts tie on latest start date with different prices",
                                                  "Rows where one contract has several different prices for the latest start date",
                                                  "Vendor+Item exists in Contracts but no contract active on the consumption date",
                                                  "Rows where client or supply price differs from the mapped price by more than the threshold (REVIEW flag, not an error)",
                                                  "All contract records behind the ambiguous rows (for investigation)"]})
            self._sheet(xw, readme, "Read Me", {"what it is": 110})
            for k, f in frames.items():
                self._sheet(xw, f, k, {"unmapped_reason": 90, "candidate_contract_detail": 90})
            self._sheet(xw, cand, "Candidate Contracts")
        _atomic_replace(tmp, path)
        self.outputs.append(str(path))
        self.exception_counts = {k: len(v) for k, v in frames.items()}
        self.timings["write_exceptions"] = time.perf_counter() - t0
        LOG.info("Exceptions workbook written: %s", self.exception_counts)

    def write_summaries(self):
        """output/summary/mapping_summary.xlsx: KPIs, match rates by vendor / month, unmapped reasons, DQ, alias review."""
        cfg = self.cfg
        m = self.metrics
        prev = (self.prev_state or {}).get("last_metrics") or {}
        srows = []
        for k, v in m.items():
            pv = prev.get(k)
            srows.append({"Metric": k, "Value": v, "Previous run": pv, "Change": (v - pv) if isinstance(v, (int, float)) and isinstance(pv, (int, float)) else None})
        srows.append({"Metric": "Current contract price as-of date", "Value": str(self.as_of.date()), "Previous run": None, "Change": None})
        summary = pd.DataFrame(srows)
        by_vendor, by_month = summarize_by(self.agg)
        self.by_vendor, self.by_month = by_vendor, by_month
        # reasons
        cg = self.agg.groupby("contract_match_status")["rows"].sum()
        tot, elig = m["Total Consumption Rows"], m["Total Eligible Rows"]
        code_rows = self.contract_reason_by_vendor.groupby("contract_gap_code")["rows"].sum()
        vend_cnt = self.contract_reason_by_vendor.groupby("contract_gap_code")["vendor"].nunique()
        creasons = pd.DataFrame({"reason_code": code_rows.index, "rows": code_rows.values})
        creasons["pct_of_all_rows"] = (100 * creasons["rows"] / tot).round(3)
        creasons["distinct_vendors"] = creasons["reason_code"].map(vend_cnt)
        creasons["which_key_or_field"] = creasons["reason_code"].map(lambda c: CODE_INFO.get(c, ("", ""))[0])
        creasons["meaning"] = creasons["reason_code"].map(lambda c: CODE_INFO.get(c, ("", ""))[1])
        creasons = creasons.sort_values("rows", ascending=False)
        igaps = self.item_master_gaps
        ireasons = (igaps.groupby("item_master_gap_code").agg(rows=("consumption_rows", "sum"), distinct_items=(KEY_ITEM, "nunique")).reset_index()
                    .rename(columns={"item_master_gap_code": "reason_code"}) if len(igaps) else pd.DataFrame(columns=["reason_code", "rows", "distinct_items"]))
        ireasons["pct_of_all_rows"] = (100 * ireasons["rows"] / tot).round(3)
        ireasons["which_key_or_field"] = ireasons["reason_code"].map(lambda c: CODE_INFO.get(c, ("", ""))[0])
        ireasons["meaning"] = ireasons["reason_code"].map(lambda c: CODE_INFO.get(c, ("", ""))[1])
        ireasons = ireasons.sort_values("rows", ascending=False)
        by_vr = self.contract_reason_by_vendor.sort_values("rows", ascending=False).head(3000)
        tvi = self.top_vendor_items.sort_values("rows", ascending=False).head(15000) if len(self.top_vendor_items) else pd.DataFrame()
        dq = pd.concat([self.contract_dq, self.consumption_and_item_master_dq()], ignore_index=True)
        prof = pd.DataFrame([{"file": n, "rows": r, "columns": len(self.headers[n]), "status_this_run": self.file_actions[n]}
                             for n, r in self.rows_per_file.items()])
        # alias review with live consumption rows
        al = self.alias_table.copy()
        al["consumption_rows"] = al["alias_name"].map(self.norm_vendor_rows).fillna(0).astype(int)
        self.alias_table = al
        self._write_alias_table()
        review = al[(al["method"].isin(["ITEM_OVERLAP", "FUZZY_NAME", "NO_MATCH", "CORE_KEY", "VENDOR_CODE"])) & (al["consumption_rows"] > 0)]
        review = review.sort_values("consumption_rows", ascending=False).rename(columns={"reviewer_standard_vendor": "reviewer_entry (col G)"})[
            ["alias_name", "standard_vendor", "method", "confidence", "review_status", "vendor_code_check", "vendor_code_evidence",
             "evidence", "reviewer_entry (col G)", "consumption_rows"]]
        path = cfg.summary_dir / "mapping_summary.xlsx"
        tmp = path.with_suffix(".xlsx.tmp")
        with pd.ExcelWriter(tmp, engine="xlsxwriter", datetime_format="yyyy-mm-dd") as xw:
            self._sheet(xw, summary, "Summary", {"Metric": 52, "Value": 16, "Previous run": 16, "Change": 14})
            self._sheet(xw, creasons, "Why Unmapped - Contract", {"meaning": 110, "which_key_or_field": 40, "reason_code": 42})
            self._sheet(xw, ireasons, "Why Unmapped - Item Master", {"meaning": 100, "which_key_or_field": 30, "reason_code": 42})
            self._sheet(xw, by_vendor, "Match by Vendor")
            self._sheet(xw, by_month, "Match by Month")
            self._sheet(xw, by_vr, "Unmapped Vendor x Reason", {"contract_gap_code": 42, "vendor": 42})
            self._sheet(xw, tvi, "Top Unmapped Vendor-Items", {"detail": 100, "vendor": 36, "contract_gap_code": 40, "item_description": 40})
            self._sheet(xw, igaps.sort_values("consumption_rows", ascending=False) if len(igaps) else igaps, "Item Master Gaps", {"item_description": 40})
            self._ambiguity_sheet(xw)
            self._current_contract_sheets(xw)
            tmap = {c: "text" for c in self.raw_columns}
            for c, t in DERIVED_SCHEMA + CURRENT_SCHEMA:
                tmap[c] = {"string": "text", "int64": "integer", "int32": "integer", "uint64": "integer", "double": "decimal"}.get(
                    str(t), "date" if "timestamp" in str(t) else str(t))
            ddict = build_data_dictionary(expected_final_columns(self.raw_columns), tmap)
            self._sheet(xw, ddict, "Data Dictionary", {"column": 40, "group": 44, "description": 110, "allowed_values": 70, "source_dataset": 18})
            self._sheet(xw, dq, "Data Quality", {"metric": 62, "note": 70})
            self._sheet(xw, self.dq_multi.head(20000), "DQ Multi-price Contract Keys")
            self._sheet(xw, self.dq_invalid, "DQ Invalid Contract Records")
            self._sheet(xw, review, "Vendor Alias Review", {"evidence": 80, "vendor_code_evidence": 80, "standard_vendor": 38,
                                                             "alias_name": 38, "reviewer_entry (col G)": 50, "review_status": 26})
            self._sheet(xw, prof, "Input Files")
        _atomic_replace(tmp, path)
        self.outputs.append(str(path))
        self.outputs.append(str(cfg.alias_path))
        LOG.info("Summary workbook written: %s", path.name)

    # ---- 12. final output ---------------------------------------------------------------------------------------
    def write_final(self):
        """Stream ALL enriched rows to one Parquet file (+ CSV parts). Never loads the whole dataset."""
        cfg = self.cfg
        t0 = time.perf_counter()
        cfg.final_dir.mkdir(parents=True, exist_ok=True)
        pq_final, pq_tmp = cfg.final_dir / "consumption_enriched.parquet", cfg.final_dir / "consumption_enriched.parquet.tmp"
        for old in cfg.final_dir.glob("consumption_enriched_part_*.csv*"):
            old.unlink()
        date_cols = ["consumption_date", "lookback_start_date", "mapped_contract_start_date", "mapped_contract_end_date",
                     "closest_contract_start_date", "closest_contract_end_date"] + CURRENT_DATE_COLS
        if not hasattr(self, "current_lk"):
            self.build_current_lookups()
        writer, csvw, part, in_part, total = None, None, 0, 0, 0
        csv_files = []
        for p in self.files["consumption"]:
            pf = pq.ParquetFile(self.effective_path(p.name))
            for k in range(pf.num_row_groups):
                t = pf.read_row_group(k)
                # current contract price / category (item level) go BEFORE the unmapped_reason columns, which stay last
                cb = current_contract_block(t.select([KEY_VENDOR, KEY_ITEM]).to_pandas(), self.current_lk)
                ct = pa.Table.from_pandas(cb, schema=pa.schema(CURRENT_SCHEMA), preserve_index=False)
                fin = t.select(FINAL_COLS)
                t = t.select([c for c in t.column_names if c not in FINAL_COLS])
                for nm in ct.column_names:
                    t = t.append_column(nm, ct.column(nm))
                for nm in FINAL_COLS:
                    t = t.append_column(nm, fin.column(nm))
                for c in date_cols:
                    i = t.schema.get_field_index(c)
                    t = t.set_column(i, c, t.column(c).cast(pa.date32()))
                if writer is None:
                    assert t.column_names[-2:] == FINAL_COLS, "unmapped_reason columns must be last"
                    assert t.column_names == expected_final_columns(self.raw_columns), "final columns differ from the documented data dictionary"
                    build_data_dictionary(t.column_names)   # raises if a column is undocumented
                    writer = pq.ParquetWriter(pq_tmp, t.schema, compression="zstd")
                writer.write_table(t)
                total += t.num_rows
                if cfg.write_final_csv:
                    off = 0
                    while off < t.num_rows:
                        if csvw is None:
                            part += 1
                            fp = cfg.final_dir / f"consumption_enriched_part_{part:03d}.csv.tmp"
                            csvw = pacsv.CSVWriter(fp, t.schema)
                            csv_files.append(fp)
                            in_part = 0
                        take = min(t.num_rows - off, cfg.csv_rows_per_part - in_part)
                        csvw.write_table(t.slice(off, take))
                        off += take
                        in_part += take
                        if in_part >= cfg.csv_rows_per_part:
                            csvw.close()
                            csvw = None
        if writer is not None:
            writer.close()
        if csvw is not None:
            csvw.close()
        csvw = writer = None  # drop references so Windows releases the file handles before renaming
        import gc
        gc.collect()
        assert total == self.total_rows, "final output row count differs from validated row count"
        _atomic_replace(pq_tmp, pq_final)
        self.outputs.append(str(pq_final))
        for fp in csv_files:
            final = fp.with_suffix("")
            _atomic_replace(fp, final)
            self.outputs.append(str(final))
        assert pq.ParquetFile(pq_final).metadata.num_rows == total
        self.timings["write_final"] = time.perf_counter() - t0
        LOG.info("Final dataset written: %d rows -> %s (+%d CSV parts)", total, pq_final.name, len(csv_files))

    # ---- commit: state is only replaced after everything above succeeded ------------------------------------------------
    def commit_state(self):
        cfg = self.cfg
        for p in self.files["consumption"]:
            s = self.stage_path(p.name)
            if s.exists():
                _atomic_replace(s, self.state_path(p.name))
        for n in self.removed_files:
            sp = self.state_path(n)
            if sp.exists():
                sp.unlink()
                self.stats["files_removed"].append(n)
        tmp = cfg.state_dir / "contracts_snapshot.parquet.tmp"
        self.contracts.to_parquet(tmp, index=False)
        _atomic_replace(tmp, cfg.state_dir / "contracts_snapshot.parquet")
        tmp = cfg.state_dir / "item_master_snapshot.parquet.tmp"
        self.im.reset_index().to_parquet(tmp, index=False)
        _atomic_replace(tmp, cfg.state_dir / "item_master_snapshot.parquet")
        now = datetime.now().isoformat(timespec="seconds")
        files = {}
        for name, e in self.manifest_files.items():
            role = ("consumption" if name in self.headers else
                    "contracts" if name == self.files["contracts"].name else "item_master")
            entry = dict(e)
            entry.pop("status", None)
            entry["role"] = role
            prev = self.prev_manifest.get("files", {}).get(name, {})
            if role == "consumption":
                entry["row_count"] = self.row_counts[name]
                entry["chunk_rows"] = cfg.chunk_rows
                entry["processed_at"] = now if name in self.stats["files_processed"] else prev.get("processed_at", now)
            elif role == "contracts":
                entry["row_count"], entry["processed_at"] = len(self.contracts_raw), now
            else:
                entry["row_count"], entry["processed_at"] = len(self.im_raw), now
            files[name] = entry
        write_json_atomic({"pipeline_version": PIPELINE_VERSION, "last_success": now, "files": files},
                          cfg.state_dir / "input_manifest.json")
        write_json_atomic({"pipeline_version": PIPELINE_VERSION, "fingerprint": cfg.fingerprint(), "last_success": now,
                           "detected_columns": {"consumption": self.cons_cols, "contracts": self.con_cols,
                                                "item_master": self.im_cols},
                           "last_metrics": self.metrics}, cfg.state_dir / "pipeline_state.json")
        shutil.rmtree(cfg.staging_dir, ignore_errors=True)
        LOG.info("State committed (manifest written last).")

    # ---- 13. run summary --------------------------------------------------------------------------------------------------
    def run_summary(self) -> pd.DataFrame:
        m, s = self.metrics, self.stats
        rt = time.perf_counter() - self.t0
        proc = s["rows_new"] + s["rows_changed"]
        rows = [
            ("Total Consumption Rows", m["Total Consumption Rows"]),
            ("New/Changed Rows Processed This Run", proc),
            ("Rows Re-mapped Because Contracts/Item Master Changed", s["rows_remapped_contract"] + s["rows_remapped_unspsc"]),
            ("Rows Reused From Previous Run", s["rows_reused"]),
            ("Eligible Rows", m["Total Eligible Rows"]),
            ("Successfully Matched Rows", m["Successfully Matched Rows"]),
            ("Contract Match Rate %", m["Contract Match Rate %"]),
            ("Contract Not Found", m["Contract Not Found"]),
            ("Contract Not Active", m["Contract Not Active / Applicable"]),
            ("Ambiguous Contract Matches", m["Ambiguous Contract Matches"]),
            ("Ambiguous Prices", m["Multiple-Price / Ambiguous Price Matches"]),
            ("Price Validation Exceptions (client or supply >20%)", self.exception_counts.get("Price Validation")
             if hasattr(self, "exception_counts") else None),
            ("Item Master Match Rate %", m["Item Master Match Rate %"]),
            ("Runtime (seconds)", round(rt, 1))]
        df = pd.DataFrame(rows, columns=["Metric", "Value"])
        LOG.info("RUN SUMMARY\n%s", df.to_string(index=False))
        return df

    def finish(self):
        LOG.info("Run end | runtime %.1fs | warnings: %d", time.perf_counter() - self.t0, len(self.warnings))
        for h in LOG.handlers:
            h.flush()


# --------------------------------------------------------------------------------------
# Metrics
# --------------------------------------------------------------------------------------
def calculate_mapping_metrics(agg: pd.DataFrame, cfg: Config, profile: dict = None) -> dict:
    """KPI dictionary from the aggregated status counts (definitions in the README)."""
    S = lambda mask: int(agg.loc[mask, "rows"].sum())
    total = int(agg["rows"].sum())
    elig = S(agg["row_eligibility"] == "ELIGIBLE")
    cs = agg["contract_match_status"]
    matched = S(cs == ST_MATCHED)
    im_m = S(agg["item_master_match_status"] == IM_MATCHED)
    lab = cfg.above_label
    pct = int(round(cfg.price_threshold * 100))
    m = {
        "Total Consumption Rows": total,
        "Total Eligible Rows": elig,
        "Total Ineligible Rows": total - elig,
        "Successfully Matched Rows": matched,
        "Unmatched Rows": elig - matched,
        "Contract Not Found": S(cs == ST_NOT_FOUND),
        "Contract Not Active / Applicable": S(cs == ST_NOT_ACTIVE),
        "Ambiguous Contract Matches": S(cs == ST_AMBIG_CONTRACTS),
        "Multiple-Price / Ambiguous Price Matches": S(cs == ST_AMBIG_PRICES),
        "Active Contract Without Price": S(cs == ST_PRICE_MISSING),
        "Missing Mapping Key": S(cs == ST_MISSING_KEY),
        "Invalid Consumption Date": S(cs == ST_INVALID_DATE),
        "Item Master Matched": im_m,
        "Item Master Not Matched": total - im_m,
        "Item Master - Item Not Found": S(agg["item_master_match_status"] == IM_NOT_FOUND),
        "Item Master - Missing Item ID": S(agg["item_master_match_status"] == IM_MISSING_ID),
        "Item Master - UNSPSC Missing": S(agg["item_master_match_status"] == IM_UNSPSC_MISSING),
        "UNSPSC Ambiguities": S(agg["item_master_match_status"] == IM_AMBIG),
        f"Client Contract Price >{pct}% Exceptions": S(agg["client_contract_price_validation_flag"] == lab),
        f"Supply Unit Price >{pct}% Exceptions": S(agg["supply_price_validation_flag"] == lab),
        "Client Contract Price Comparable Rows": S(agg["client_contract_price_validation_flag"] != FLAG_NA),
        "Supply Unit Price Comparable Rows": S(agg["supply_price_validation_flag"] != FLAG_NA),
        "Contract Match Rate %": round(100.0 * matched / elig, 4) if elig else 0.0,
        "Item Master Match Rate %": round(100.0 * im_m / total, 4) if total else 0.0,
    }
    m["Client Contract Price >20% Exceptions"] = m.get(f"Client Contract Price >{pct}% Exceptions")
    m["Supply Unit Price >20% Exceptions"] = m.get(f"Supply Unit Price >{pct}% Exceptions")
    for st in CONTRACT_STATUSES:
        m[f"% of Total Rows - {st}"] = round(100.0 * S(cs == st) / total, 4) if total else 0.0
    parts = sum(S(cs == st) for st in CONTRACT_STATUSES)
    assert parts == total, "contract status counts do not add up to total rows"
    assert m["Unmatched Rows"] == (m["Contract Not Found"] + m["Contract Not Active / Applicable"] +
                                   m["Ambiguous Contract Matches"] + m["Multiple-Price / Ambiguous Price Matches"] +
                                   m["Active Contract Without Price"]), "unmatched rows do not reconcile with status counts"
    if profile:
        m["Unique Item IDs"] = profile["unique_item_ids"]
        m["Unique Vendor+Item Combinations"] = profile["unique_vendor_item_combinations"]
    return m


def summarize_by(agg: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Vendor and month summaries (match-rate denominators use ELIGIBLE rows only)."""
    def build(key, label):
        a = agg.copy()
        a["eligible"] = np.where(a["row_eligibility"] == "ELIGIBLE", a["rows"], 0)
        for name, st in (("matched", [ST_MATCHED]), ("contract_not_found", [ST_NOT_FOUND]),
                         ("contract_not_active", [ST_NOT_ACTIVE]),
                         ("ambiguous_contracts", [ST_AMBIG_CONTRACTS]), ("ambiguous_prices", [ST_AMBIG_PRICES]),
                         ("missing_mapping_key", [ST_MISSING_KEY]), ("invalid_consumption_date", [ST_INVALID_DATE]),
                         ("active_contract_without_price", [ST_PRICE_MISSING])):
            a[name] = np.where(a["contract_match_status"].isin(st), a["rows"], 0)
        a["client_price_above_threshold_rows"] = np.where(a["client_contract_price_validation_flag"].str.startswith("ABOVE"), a["rows"], 0)
        a["supply_price_above_threshold_rows"] = np.where(a["supply_price_validation_flag"].str.startswith("ABOVE"), a["rows"], 0)
        a["item_master_unmatched"] = np.where(a["item_master_match_status"] != IM_MATCHED, a["rows"], 0)
        g = a.groupby(key, dropna=False).sum(numeric_only=True).reset_index().rename(columns={"rows": "consumption_rows"})
        g["unmatched_eligible"] = g["eligible"] - g["matched"]
        g["ambiguous_total"] = g["ambiguous_contracts"] + g["ambiguous_prices"]
        g["match_rate_pct"] = np.where(g["eligible"] > 0, (100.0 * g["matched"] / g["eligible"]).round(4), np.nan)
        g = g.rename(columns={key: label})
        cols = [label, "consumption_rows", "eligible", "matched", "match_rate_pct", "unmatched_eligible",
                "contract_not_found", "contract_not_active", "ambiguous_total", "ambiguous_contracts", "ambiguous_prices",
                "active_contract_without_price", "missing_mapping_key", "invalid_consumption_date",
                "client_price_above_threshold_rows", "supply_price_above_threshold_rows", "item_master_unmatched"]
        return g[cols]
    v = build("vendor", "standard_vendor").sort_values("consumption_rows", ascending=False)
    mo = build("month", "month").sort_values("month")
    ren = {"eligible": "eligible_rows", "matched": "matched_rows"}
    return v.rename(columns=ren), mo.rename(columns=ren)


# --------------------------------------------------------------------------------------
# End-to-end driver (same stage calls the notebook makes one by one)
# --------------------------------------------------------------------------------------
def run_pipeline(cfg: Optional[Config] = None) -> PipelineRun:
    cfg = cfg or Config()
    r = PipelineRun(cfg)
    try:
        r.discover()
        r.load_reference()
        r.build_vendor_alias()
        r.normalize_reference()
        r.plan_incremental()
        r.contract_data_quality()
        r.process_consumption()
        r.collect_metrics()
        r.analyze_ambiguity()
        r.write_exceptions()
        r.write_summaries()
        r.write_final()
        r.commit_state()
        r.run_summary()
    except Exception:
        LOG.exception("PIPELINE FAILED - previous state left untouched")
        raise
    finally:
        r.finish()
    return r


if __name__ == "__main__":
    run_pipeline(Config(base_dir=Path(sys.argv[1]) if len(sys.argv) > 1 else Path.cwd()))
