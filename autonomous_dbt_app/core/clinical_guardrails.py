"""
Clinical Data Sanitization & Enterprise Guardrails Engine
Provides pre-ingestion checks and balances for heterogeneous, messy hospital EHR/ERP data:
1. Medical/Surgical UOM Packaging Normalization Matrix & Ratio Detector
2. Statistical Price Outlier & Z-Score/IQR Typo Boundaries
3. Negative Quantity & Credit Memo Netting Layer
4. Clinical Taxonomy Zero-Padding (CPT/HCPCS, NDC, DRG, Zip)
5. Contract Boundary Sanity Guard (>2.5x or <0.2x ratio detection)
"""

import re
import math
import numpy as np
import pandas as pd
from typing import Dict, Any, List, Optional, Tuple

class ClinicalUOMMatrix:
    """
    Standardizes healthcare packaging units of measure and identifies
    Case/Box vs Each purchasing discrepancies.
    """
    KNOWN_UOM_FACTORS: Dict[str, float] = {
        "EA": 1.0,
        "EACH": 1.0,
        "PC": 1.0,
        "PIECE": 1.0,
        "UNIT": 1.0,
        "UN": 1.0,
        "DZ": 12.0,
        "DOZEN": 12.0,
        "PR": 2.0,
        "PAIR": 2.0,
        "BX": 10.0,       # default box heuristic unless explicit
        "BOX": 10.0,
        "CS": 50.0,       # default case heuristic
        "CASE": 50.0,
        "PK": 5.0,        # default pack heuristic
        "PACK": 5.0,
        "PKG": 5.0,
        "KIT": 1.0,
        "SET": 1.0,
        "TRAY": 1.0,
        "VIAL": 1.0,
        "BTL": 1.0,
        "BOTTLE": 1.0,
        "AMP": 1.0,
        "AMPOULE": 1.0,
        "SYR": 1.0,
        "SYRINGE": 1.0,
        "ROLL": 1.0,
        "BAG": 1.0
    }

    # Common packaging multiples observed in hospital supply chain
    COMMON_MULTIPLES = [2.0, 4.0, 5.0, 6.0, 8.0, 10.0, 12.0, 15.0, 20.0, 24.0, 25.0, 30.0, 48.0, 50.0, 100.0, 120.0, 144.0, 250.0, 500.0]

    @classmethod
    def detect_uom_factor(cls, uom_str: Optional[str]) -> Tuple[float, str]:
        """
        Parses explicit package quantity strings like 'BX/10', 'CS/50', 'PK/24', 'BOX OF 100'
        Returns (factor, normalized_code)
        """
        if not uom_str:
            return 1.0, "EA"
        
        s = str(uom_str).strip().upper()
        
        # Check explicit regex: BX/10, CS/100, PK 20, BOX 12, CASE/48
        m = re.search(r'(BX|BOX|CS|CASE|PK|PACK|PKG|DZ|CT|CARTON)[/\s_-]*(\d+)', s)
        if m:
            base_type = m.group(1)
            qty = float(m.group(2))
            if qty > 0:
                norm = f"{base_type[:2]}/{int(qty)}"
                return qty, norm
        
        # Exact lookup in known dictionary
        for k, factor in cls.KNOWN_UOM_FACTORS.items():
            if s == k:
                return factor, k
                
        return 1.0, s

    @classmethod
    def check_uom_pricing_mismatch(
        cls, 
        billed_unit_price: float, 
        contract_ea_price: float, 
        tolerance: float = 0.08
    ) -> Optional[Dict[str, Any]]:
        """
        Detects if billed price is actually a box/case price billed as each.
        e.g., Contract EA = $10.00, Billed = $100.00 -> 10x ratio matches Box/10.
        """
        if contract_ea_price <= 0 or billed_unit_price <= 0:
            return None
            
        ratio = billed_unit_price / contract_ea_price
        
        for mult in cls.COMMON_MULTIPLES:
            if abs(ratio - mult) / mult <= tolerance:
                # Strong likelihood of packaging error
                adjusted_ea = round(billed_unit_price / mult, 4)
                return {
                    "is_uom_mismatch": True,
                    "inferred_factor": mult,
                    "uom_description": f"Suspected Box/Case ratio {int(mult)}:1 billed instead of Each",
                    "original_billed_price": billed_unit_price,
                    "adjusted_billed_unit_price": adjusted_ea,
                    "contract_ea_price": contract_ea_price,
                    "corrected_price_variance": round(adjusted_ea - contract_ea_price, 4)
                }
        return None


class OutlierSpikeGuard:
    """
    Statistical checks for data-entry typos and decimal shifts (e.g. $45,000 -> $450,000).
    """
    @staticmethod
    def calculate_bounds(values: List[float], factor: float = 1.5) -> Tuple[float, float, float]:
        """
        Computes 25th percentile, median, 75th percentile, and IQR bounds.
        """
        clean = [v for v in values if v is not None and not math.isnan(v) and v > 0]
        if len(clean) < 4:
            median = clean[0] if clean else 0.0
            return 0.0, median, median * 10.0
            
        arr = np.array(clean)
        q25, q50, q75 = np.percentile(arr, [25, 50, 75])
        iqr = q75 - q25
        lower = max(0.0, q25 - (factor * iqr))
        upper = q75 + (factor * iqr)
        return lower, q50, upper

    @classmethod
    def evaluate_price_spike(
        cls, 
        unit_price: float, 
        benchmark_median: float, 
        historical_stdev: float = 0.0
    ) -> Dict[str, Any]:
        """
        Evaluates if a single unit price is a statistical spike (>3 sigma or 3x median).
        """
        if benchmark_median <= 0 or unit_price <= 0:
            return {"is_spike": False, "severity": "NORMAL", "z_score": 0.0}
            
        ratio = unit_price / benchmark_median
        z_score = 0.0
        if historical_stdev > 0:
            z_score = (unit_price - benchmark_median) / historical_stdev
            
        is_spike = False
        severity = "NORMAL"
        rationale = "Price within acceptable clinical variance"
        
        # 10x or 100x decimal shift typo
        if ratio >= 8.5 and ratio <= 11.5:
            is_spike = True
            severity = "CRITICAL_TYPO_10X"
            rationale = "Suspected 10x decimal shift data entry typo"
        elif ratio >= 85.0 and ratio <= 115.0:
            is_spike = True
            severity = "CRITICAL_TYPO_100X"
            rationale = "Suspected 100x decimal shift data entry typo"
        elif ratio >= 3.0 or z_score >= 3.5:
            is_spike = True
            severity = "HIGH_OUTLIER"
            rationale = f"Price is {ratio:.1f}x higher than clinical baseline median"
        elif ratio <= 0.1:
            is_spike = True
            severity = "LOW_OUTLIER"
            rationale = f"Price is {ratio:.2f}x of clinical baseline median (suspected missing zero)"
            
        return {
            "is_spike": is_spike,
            "severity": severity,
            "ratio_to_median": round(ratio, 2),
            "z_score": round(z_score, 2),
            "rationale": rationale
        }


class CodeSanitizer:
    """
    Preserves and restores leading zeros stripped by Excel/CSV imports.
    """
    @staticmethod
    def pad_cpt_code(code: Any) -> str:
        """CPT / HCPCS: 5 digits"""
        if code is None or pd.isna(code):
            return ""
        s = str(code).strip()
        if re.match(r'^\d{1,4}$', s):
            return s.zfill(5)
        return s

    @staticmethod
    def pad_drg_code(code: Any) -> str:
        """MS-DRG: 3 digits"""
        if code is None or pd.isna(code):
            return ""
        s = str(code).strip()
        if re.match(r'^\d{1,2}$', s):
            return s.zfill(3)
        return s

    @staticmethod
    def pad_ndc_code(code: Any) -> str:
        """NDC Drug Code: 11 digits"""
        if code is None or pd.isna(code):
            return ""
        digits = re.sub(r'\D', '', str(code))
        if 0 < len(digits) < 11:
            return digits.zfill(11)
        return str(code).strip()

    @staticmethod
    def clean_vendor_name(name: Any) -> str:
        """Normalizes vendor names and removes corporate suffixes"""
        if name is None or pd.isna(name):
            return ""
        s = str(name).strip().upper()
        # Remove common business entity suffixes
        s = re.sub(r'[,.]?\s+(INC|INCORPORATED|LLC|L\.L\.C\.|CORP|CORPORATION|CO|COMPANY|LTD|LIMITED|SC\s+INC)$', '', s)
        s = re.sub(r'\s+', ' ', s)
        return s.strip()


class ReversalCreditMemoNetting:
    """
    Nets return and credit memo pairs to prevent spend inflation.
    """
    @staticmethod
    def net_records(records: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Nets positive line items against corresponding negative return lines
        matching (case_id/encounter, item_number, abs(quantity)).
        Returns (active_net_records, netted_audit_ledger).
        """
        positives = []
        negatives = []
        
        for r in records:
            qty = float(r.get("total_quantity") or r.get("quantity") or 0.0)
            if qty < 0:
                negatives.append(r)
            else:
                positives.append(r)
                
        if not negatives:
            return records, []
            
        netted_audit = []
        active = []
        
        # Track matched negatives
        matched_neg_indices = set()
        
        for p in positives:
            p_case = str(p.get("case_id") or p.get("encounter_id") or "")
            p_item = str(p.get("item_number") or p.get("item_description") or "")
            p_qty = float(p.get("total_quantity") or p.get("quantity") or 0.0)
            
            matched = False
            for idx, n in enumerate(negatives):
                if idx in matched_neg_indices:
                    continue
                n_case = str(n.get("case_id") or n.get("encounter_id") or "")
                n_item = str(n.get("item_number") or n.get("item_description") or "")
                n_qty = float(n.get("total_quantity") or n.get("quantity") or 0.0)
                
                # Check match criteria
                if (p_case == n_case or not p_case) and p_item == n_item and abs(p_qty - abs(n_qty)) < 0.001:
                    matched = True
                    matched_neg_indices.add(idx)
                    netted_audit.append({
                        "positive_record": p,
                        "reversal_record": n,
                        "netted_quantity": p_qty,
                        "reason": "Direct Credit Memo / Cancellation Pair Netting"
                    })
                    break
                    
            if not matched:
                active.append(p)
                
        # Unmatched negatives are kept as exception adjustments
        for idx, n in enumerate(negatives):
            if idx not in matched_neg_indices:
                active.append(n)
                
        return active, netted_audit


class ClinicalGuardrailsEngine:
    """
    Unified Façade for executing all clinical checks and balances on datasets.
    """
    def __init__(self):
        self.uom = ClinicalUOMMatrix()
        self.outliers = OutlierSpikeGuard()
        self.sanitizer = CodeSanitizer()
        self.netting = ReversalCreditMemoNetting()

    def inspect_and_sanitize_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Runs comprehensive data quality checks on a single clinical record.
        """
        res = dict(record)
        
        # 1. Code padding
        if "cpt_code" in res:
            res["cpt_code"] = self.sanitizer.pad_cpt_code(res["cpt_code"])
        if "drg_code" in res:
            res["drg_code"] = self.sanitizer.pad_drg_code(res["drg_code"])
        if "vendor" in res or "supplier" in res:
            key = "vendor" if "vendor" in res else "supplier"
            res[key] = self.sanitizer.clean_vendor_name(res[key])
            
        # 2. UOM Normalization & Mismatch check
        uom_str = str(res.get("uom") or res.get("unit_of_measure") or "EA")
        factor, norm_uom = self.uom.detect_uom_factor(uom_str)
        res["normalized_uom"] = norm_uom
        res["uom_packaging_factor"] = factor
        
        supply_price = float(res.get("supply_unit_price") or res.get("unit_price") or 0.0)
        contract_price = float(res.get("contract_ea_price") or res.get("contract_price") or 0.0)
        
        uom_mismatch = self.uom.check_uom_pricing_mismatch(supply_price, contract_price)
        if uom_mismatch:
            res["flag_uom_packaging_mismatch"] = True
            res["uom_audit_rationale"] = uom_mismatch["uom_description"]
            res["effective_billed_ea_price"] = uom_mismatch["adjusted_billed_unit_price"]
        else:
            res["flag_uom_packaging_mismatch"] = False
            res["effective_billed_ea_price"] = supply_price
            
        # 3. Contract Boundary Sanity Guard
        if contract_price > 0:
            ratio = supply_price / contract_price
            if ratio > 2.5:
                res["flag_contract_sanity_review"] = True
                res["contract_sanity_status"] = "SEVERE_OVERPAYMENT_OR_UOM"
            elif ratio < 0.2:
                res["flag_contract_sanity_review"] = True
                res["contract_sanity_status"] = "DEEP_DISCOUNT_OR_MISSING_ZERO"
            else:
                res["flag_contract_sanity_review"] = False
                res["contract_sanity_status"] = "NORMAL_TOLERANCE"
        else:
            res["flag_contract_sanity_review"] = False
            res["contract_sanity_status"] = "OFF_CONTRACT"

        return res
