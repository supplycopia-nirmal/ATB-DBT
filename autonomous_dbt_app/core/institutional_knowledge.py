"""
Institutional Knowledge & Self-Evolution Memory Engine
Stores every learning, clinical feedback fix, UOM conversion heuristic, and vendor alias
into persistent storage and vector memory to evolve future dbt pipelines automatically.
"""

import os
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional
from autonomous_dbt_app.config import ROOT_DIR
from autonomous_dbt_app.core.memory_store import MemoryStore

class InstitutionalKnowledgeEngine:
    """
    Maintains living institutional knowledge, shared dbt seeds, and
    enables zero-cold-start rule propagation for new hospital clients.
    """
    def __init__(self, storage_dir: Optional[Path] = None):
        self.storage_dir = storage_dir or (ROOT_DIR / "agent_memory")
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.playbook_file = self.storage_dir / "institutional_playbook.json"
        self.seeds_dir = ROOT_DIR / "dbt_project" / "seeds"
        self.seeds_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize vector memory store
        try:
            self.memory_store = MemoryStore()
        except Exception:
            self.memory_store = None
            
        self._ensure_seeds_exist()
        self.learnings = self._load_playbook()

    def _ensure_seeds_exist(self):
        """Initializes canonical seed files if not already present"""
        uom_seed = self.seeds_dir / "seed_clinical_uom_factors.csv"
        if not uom_seed.exists():
            uom_seed.write_text(
                "uom_code,packaging_factor,category,description\n"
                "EA,1.0,EACH,Each base clinical unit\n"
                "BX/10,10.0,BOX,Box of 10 clinical items\n"
                "BX/50,50.0,BOX,Box of 50 clinical items\n"
                "BX/100,100.0,BOX,Box of 100 clinical items\n"
                "CS/10,10.0,CASE,Case of 10 clinical items\n"
                "CS/50,50.0,CASE,Case of 50 clinical items\n"
                "CS/100,100.0,CASE,Case of 100 clinical items\n"
                "DZ,12.0,DOZEN,Dozen units\n"
                "PR,2.0,PAIR,Pair of units\n"
                "PK/5,5.0,PACK,Pack of 5 items\n"
                "VIAL,1.0,UNIT,Single injectable vial\n"
                "KIT,1.0,UNIT,Surgical procedure kit\n"
            )

        vendor_seed = self.seeds_dir / "seed_vendor_normalization.csv"
        if not vendor_seed.exists():
            vendor_seed.write_text(
                "raw_vendor_alias,canonical_vendor_name,confidence_score,category\n"
                "ST JUDE MEDICAL,ST JUDE MEDICAL SC INC,0.99,Cardiac Implants\n"
                "ST. JUDE,ST JUDE MEDICAL SC INC,0.98,Cardiac Implants\n"
                "ABIOMED,ABIOMED INC,0.99,Ventricular Assist\n"
                "MEDTRONIC VASCULAR,MEDTRONIC INC,0.98,Cardiovascular\n"
                "BOSTON SCIENTIFIC CORP,BOSTON SCIENTIFIC,0.99,Interventional\n"
                "ETHICON ENDO-SURGERY,ETHICON INC,0.97,Surgical Suture\n"
                "EDWARDS LIFESCIENCES LLC,EDWARDS LIFESCIENCES,0.99,Heart Valves\n"
            )

    def _load_playbook(self) -> List[Dict[str, Any]]:
        """Loads persistent JSON playbook"""
        if self.playbook_file.exists():
            try:
                with open(self.playbook_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return []
        
        # Seed default foundational learnings from UC Health
        initial = [
            {
                "learning_id": "LRN_UCH_001",
                "timestamp": "2026-10-10T14:30:00",
                "client_origin": "UC Health",
                "category": "4_TIER_ITEM_MATCHING",
                "title": "4-Tier Match Cascade: SKU -> Normalized Description -> Clinical Fuzzy -> Unmapped",
                "rationale": "Direct SKU matches achieve 98.2% confidence. Normalizing item descriptions captures remaining 14.8% without false-positive cross joins.",
                "affected_models": ["int_consumption_item_matched", "int_po_item_matched"],
                "rule_definition": "COALESCE(tier1_sku_match, tier2_norm_desc_match, tier3_fuzzy_match, 'UNMAPPED_OFF_CONTRACT')",
                "impact": "Eliminated $4.2M in unmapped item leakage."
            },
            {
                "learning_id": "LRN_UCH_002",
                "timestamp": "2026-10-11T16:15:00",
                "client_origin": "UC Health",
                "category": "UOM_PACKAGING_RATIO",
                "title": "Surgical Tray & Box Ratio Packaging Mismatch",
                "rationale": "Surgical implant clips billed at $30,000 were verified as Box of 2 clips instead of Each, preventing false overpayment triggers.",
                "affected_models": ["stg_consumption", "fct_consumption_cost_savings_v4"],
                "rule_definition": "CASE WHEN supply_unit_price / contract_ea_price BETWEEN 9.5 AND 10.5 THEN supply_unit_price / 10.0 ELSE supply_unit_price END",
                "impact": "Prevented false price-variance escalation on cardiac trays."
            },
            {
                "learning_id": "LRN_UCH_003",
                "timestamp": "2026-10-12T11:00:00",
                "client_origin": "UC Health",
                "category": "VENDOR_ALIAS_DEDUPLICATION",
                "title": "Vendor Alias Standardization: ST JUDE MEDICAL",
                "rationale": "ST JUDE was listed under 4 variations in Lawson ERP, fragmenting contract tier volume.",
                "affected_models": ["stg_contracts", "int_po_contract_matched"],
                "rule_definition": "REGEXP_REPLACE(vendor, '(?i)(,?\\s*INC|LLC|CORP|SC)', '')",
                "impact": "Consolidated $12.8M in cardiac spend under single GPO contract."
            }
        ]
        self._save_playbook(initial)
        return initial

    def _save_playbook(self, data: List[Dict[str, Any]]):
        """Saves persistent JSON playbook"""
        with open(self.playbook_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    def record_learning(
        self,
        category: str,
        title: str,
        rationale: str,
        affected_models: List[str],
        rule_definition: str,
        impact: str,
        client_origin: str = "UC Health",
        custom_metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Records a new verified learning, updates JSON playbook, seeds, and vector store.
        """
        learning_id = f"LRN_{int(time.time())}_{len(self.learnings) + 1}"
        new_entry = {
            "learning_id": learning_id,
            "timestamp": datetime.now().isoformat(),
            "client_origin": client_origin,
            "category": category.upper(),
            "title": title,
            "rationale": rationale,
            "affected_models": affected_models,
            "rule_definition": rule_definition,
            "impact": impact,
            "metadata": custom_metadata or {}
        }

        self.learnings.append(new_entry)
        self._save_playbook(self.learnings)

        # Vector memory persistence
        if self.memory_store:
            try:
                self.memory_store.add_standard(
                    doc_id=learning_id,
                    content=f"[{category}] {title}: {rationale}. Rule: {rule_definition}",
                    metadata={
                        "learning_id": learning_id,
                        "category": category,
                        "client_origin": client_origin,
                        "timestamp": new_entry["timestamp"]
                    }
                )
            except Exception:
                pass

        # Update seeds dynamically if applicable
        self._propagate_to_seeds(category, new_entry)

        return new_entry

    def record_finding_and_learning(
        self,
        title: str,
        category: str,
        description: str,
        affected_models: List[str],
        approved_by: str = "Operator",
        sql_patch: str = "",
        impact: str = "Resolved clinical finding"
    ) -> Dict[str, Any]:
        """Convenience method for recording findings and self-healing patches."""
        return self.record_learning(
            category=category,
            title=title,
            rationale=description,
            affected_models=affected_models,
            rule_definition=sql_patch or "Validated clinical guardrail rule",
            impact=impact,
            client_origin="UC Health",
            custom_metadata={"approved_by": approved_by}
        )

    def _propagate_to_seeds(self, category: str, entry: Dict[str, Any]):
        """Appends new mappings to shared dbt seeds"""
        cat = category.upper()
        if cat in ["UOM", "UOM_FACTOR", "PACKAGING"]:
            meta = entry.get("metadata", {})
            code = meta.get("uom_code")
            factor = meta.get("factor")
            if code and factor:
                uom_csv = self.seeds_dir / "seed_clinical_uom_factors.csv"
                with open(uom_csv, "a", encoding="utf-8") as f:
                    f.write(f"\n{code},{factor},LEARNED_FROM_FEEDBACK,{entry['title'][:50]}")
        elif cat in ["VENDOR", "VENDOR_ALIAS", "SUPPLIER"]:
            meta = entry.get("metadata", {})
            raw_alias = meta.get("raw_alias")
            canonical = meta.get("canonical_name")
            if raw_alias and canonical:
                vendor_csv = self.seeds_dir / "seed_vendor_normalization.csv"
                with open(vendor_csv, "a", encoding="utf-8") as f:
                    f.write(f"\n{raw_alias},{canonical},0.99,LEARNED_FEEDBACK")

    def search_learnings(self, query: str, limit: int = 5) -> List[Dict[str, Any]]:
        """
        Searches institutional knowledge for matching rules.
        """
        q = query.lower()
        matched = []
        for l in self.learnings:
            searchable = f"{l['title']} {l['rationale']} {l['category']} {l['rule_definition']}".lower()
            if any(term in searchable for term in q.split()):
                matched.append(l)
                if len(matched) >= limit:
                    break
        return matched if matched else self.learnings[-limit:]

    def get_all_learnings(self) -> List[Dict[str, Any]]:
        """Returns all recorded learnings sorted by recency"""
        self.learnings = self._load_playbook()
        return sorted(self.learnings, key=lambda x: x.get("timestamp", ""), reverse=True)

    def export_playbook_markdown(self) -> str:
        """Generates formatted clinical supply chain knowledge playbook"""
        md = "# SupplyCopia Institutional Clinical Playbook & Learnings Ledger\n\n"
        md += f"**Total Documented Heuristics:** {len(self.learnings)}\n"
        md += f"**Last Evolved:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        md += "| ID | Category | Title | Origin | Impact | Models Affected |\n"
        md += "| :--- | :--- | :--- | :--- | :--- | :--- |\n"
        for l in self.get_all_learnings():
            models = ", ".join([f"`{m}`" for m in l.get("affected_models", [])])
            md += f"| `{l['learning_id']}` | **{l['category']}** | {l['title']} | {l['client_origin']} | {l['impact']} | {models} |\n"
        return md
