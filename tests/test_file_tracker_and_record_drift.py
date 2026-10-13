import os
import io
import json
import tempfile
import unittest
import pandas as pd
from pathlib import Path
import openpyxl

from autonomous_dbt_app.core.client_file_tracker import ClientFileTracker, EntityMergeEngine, ENTITY_NATURAL_KEYS
from autonomous_dbt_app.core.record_drift_engine import RecordDriftEngine
from autonomous_dbt_app.agents.enterprise_agent import EnterpriseAgent


class TestFileTrackerAndRecordDrift(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_path = Path(self.temp_dir.name)
        self.catalog_dir = self.temp_path / "catalogs"
        self.staged_dir = self.temp_path / "staged"
        self.catalog_dir.mkdir(parents=True, exist_ok=True)
        self.staged_dir.mkdir(parents=True, exist_ok=True)

        self.tracker = ClientFileTracker(catalog_dir=self.catalog_dir)
        self.merge_engine = EntityMergeEngine(tracker=self.tracker, output_staged_dir=self.staged_dir)
        self.drift_engine = RecordDriftEngine()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_file_tracker_registration_and_versioning(self):
        # 1. Create baseline contracts file
        file_v1 = self.temp_path / "UHC_CON_v1.csv"
        df1 = pd.DataFrame([
            {"contract_number": "CON-101", "item_id": "ITM-01", "contract_price": 50.0},
            {"contract_number": "CON-102", "item_id": "ITM-02", "contract_price": 120.0}
        ])
        df1.to_csv(file_v1, index=False)

        entry1 = self.tracker.register_file(
            client_id="CL_TEST_001",
            file_path=file_v1,
            entity_type="contracts",
            notes="Baseline contracts"
        )
        self.assertEqual(entry1["version"], 1)
        self.assertEqual(entry1["entity_type"], "contracts")
        self.assertEqual(entry1["status"], "ACTIVE")
        self.assertEqual(entry1["row_count"], 2)

        # 2. Register updated revision (Addendum)
        file_v2 = self.temp_path / "UHC_CON_v2_addendum.csv"
        df2 = pd.DataFrame([
            {"contract_number": "CON-101", "item_id": "ITM-01", "contract_price": 45.0}, # Updated price
            {"contract_number": "CON-103", "item_id": "ITM-03", "contract_price": 85.0}  # New contract
        ])
        df2.to_csv(file_v2, index=False)

        entry2 = self.tracker.register_file(
            client_id="CL_TEST_001",
            file_path=file_v2,
            entity_type="contracts",
            notes="Q4 Pricing Addendum"
        )
        self.assertEqual(entry2["version"], 2)
        self.assertEqual(entry2["status"], "ACTIVE")
        self.assertEqual(entry2["row_count"], 2)

        # Check catalog listing
        files = self.tracker.list_files(client_id="CL_TEST_001", entity_type="contracts")
        self.assertEqual(len(files), 2)
        self.assertEqual(files[0]["version"], 1)
        self.assertEqual(files[1]["version"], 2)

    def test_entity_merge_latest_wins_deduplication(self):
        # Create v1 and v2 item master
        f1 = self.temp_path / "IM_Batch1.csv"
        df1 = pd.DataFrame([
            {"item_id": "ITEM_A", "item_description": "Catheter Standard", "unit_price": 10.0},
            {"item_id": "ITEM_B", "item_description": "Surgical Mask", "unit_price": 2.0}
        ])
        df1.to_csv(f1, index=False)
        self.tracker.register_file("CL_TEST_002", f1, "item_master")

        f2 = self.temp_path / "IM_Batch2.csv"
        df2 = pd.DataFrame([
            {"item_id": "ITEM_A", "item_description": "Catheter High Flow", "unit_price": 12.5}, # Overwrites ITEM_A
            {"item_id": "ITEM_C", "item_description": "N95 Respirator", "unit_price": 4.0}       # New item
        ])
        df2.to_csv(f2, index=False)
        self.tracker.register_file("CL_TEST_002", f2, "item_master")

        # Execute latest_wins merge
        summary = self.merge_engine.merge_entity(
            client_id="CL_TEST_002",
            entity_type="item_master",
            merge_strategy="latest_wins",
            primary_keys=["item_id"]
        )

        self.assertEqual(summary["status"], "SUCCESS")
        self.assertEqual(summary["files_merged_count"], 2)
        self.assertEqual(summary["total_raw_rows"], 4)
        self.assertEqual(summary["final_merged_rows"], 3) # ITEM_A, ITEM_B, ITEM_C
        self.assertEqual(summary["duplicates_removed"], 1)

        # Verify consolidated file contents
        cons_path = Path(summary["consolidated_path"])
        self.assertTrue(cons_path.exists())
        merged_df = pd.read_csv(cons_path)
        self.assertEqual(len(merged_df), 3)

        # Check ITEM_A has latest v2 description and price
        row_a = merged_df[merged_df["item_id"] == "ITEM_A"].iloc[0]
        self.assertEqual(row_a["item_description"], "Catheter High Flow")
        self.assertEqual(row_a["unit_price"], 12.5)

    def test_record_drift_volume_churn_and_field_modifications(self):
        # Baseline df
        base_df = pd.DataFrame([
            {"item_id": "1", "item_description": "Gauze 4x4", "unit_price": 5.0, "vendor": "Medline"},
            {"item_id": "2", "item_description": "Scalpel #10", "unit_price": 15.0, "vendor": "Becton"},
            {"item_id": "3", "item_description": "Saline 500ml", "unit_price": 8.0, "vendor": "Baxter"}
        ])

        # Incoming df: 1 modified, 1 unchanged, 1 dropped (3 not present), 1 added (4)
        inc_df = pd.DataFrame([
            {"item_id": "1", "item_description": "Gauze 4x4 Sterile", "unit_price": 5.5, "vendor": "Medline"}, # modified
            {"item_id": "2", "item_description": "Scalpel #10", "unit_price": 15.0, "vendor": "Becton"},        # unchanged
            {"item_id": "4", "item_description": "Suture 3-0", "unit_price": 25.0, "vendor": "Ethicon"}          # added
        ])

        f_base = self.temp_path / "base.csv"
        f_inc = self.temp_path / "inc.csv"
        base_df.to_csv(f_base, index=False)
        inc_df.to_csv(f_inc, index=False)

        drift = self.drift_engine.compare_datasets(
            baseline_path=f_base,
            incoming_path=f_inc,
            entity_type="item_master",
            primary_keys=["item_id"]
        )

        self.assertEqual(drift["baseline_row_count"], 3)
        self.assertEqual(drift["latest_row_count"], 3)
        self.assertEqual(drift["volume_delta"], 0)

        churn = drift["record_churn"]
        self.assertEqual(churn["added"], 1)     # item 4
        self.assertEqual(churn["dropped"], 1)   # item 3
        self.assertEqual(churn["modified"], 1)  # item 1
        self.assertEqual(churn["unchanged"], 1) # item 2

        # Check field modifications
        field_mods = {f["field"]: f for f in drift["field_modifications"]}
        self.assertIn("unit_price", field_mods)
        self.assertIn("item_description", field_mods)
        self.assertEqual(field_mods["unit_price"]["modified_records"], 1)

    def test_record_drift_null_spikes_and_numerical_shifts(self):
        base_df = pd.DataFrame([
            {"id": "1", "cost": 100.0, "category": "OR"},
            {"id": "2", "cost": 110.0, "category": "ER"},
            {"id": "3", "cost": 90.0,  "category": "ICU"},
            {"id": "4", "cost": 100.0, "category": "OR"}
        ])

        # Incoming has severe null spike in category and price shift
        inc_df = pd.DataFrame([
            {"id": "1", "cost": 300.0, "category": None}, # null spike + price jump
            {"id": "2", "cost": 320.0, "category": None},
            {"id": "3", "cost": 290.0, "category": "ICU"},
            {"id": "4", "cost": 310.0, "category": None}
        ])

        f_base = self.temp_path / "num_base.csv"
        f_inc = self.temp_path / "num_inc.csv"
        base_df.to_csv(f_base, index=False)
        inc_df.to_csv(f_inc, index=False)

        drift = self.drift_engine.compare_datasets(
            baseline_path=f_base,
            incoming_path=f_inc,
            entity_type="custom",
            primary_keys=["id"]
        )

        # Check null rate drift
        null_spikes = {n["column"]: n for n in drift["null_rate_drift"]}
        self.assertIn("category", null_spikes)
        self.assertEqual(null_spikes["category"]["baseline_null_pct"], 0.0)
        self.assertEqual(null_spikes["category"]["incoming_null_pct"], 75.0)
        self.assertTrue(null_spikes["category"]["spike_alert"])

        # Check numerical shifts
        num_shifts = {s["column"]: s for s in drift["numerical_shifts"]}
        self.assertIn("cost", num_shifts)
        self.assertEqual(num_shifts["cost"]["baseline_mean"], 100.0)
        self.assertGreater(num_shifts["cost"]["mean_shift_pct"], 100.0)
        self.assertTrue(num_shifts["cost"]["shift_alert"])

    def test_enterprise_agent_record_drift_and_excel_export(self):
        agent = EnterpriseAgent()
        drift_res = agent.detect_record_drift(client_id="CL_UCH_001", entity_type="item_master")
        self.assertIn("record_churn", drift_res)
        self.assertIn("field_modifications", drift_res)
        self.assertIn("null_rate_drift", drift_res)

        # Export Excel bytes and verify sheets using openpyxl
        excel_bytes = agent.export_record_drift_excel(client_id="CL_UCH_001", entity_type="item_master")
        self.assertIsInstance(excel_bytes, bytes)
        self.assertGreater(len(excel_bytes), 1000)

        wb = openpyxl.load_workbook(io.BytesIO(excel_bytes))
        sheet_names = wb.sheetnames
        self.assertIn("Executive Churn Summary", sheet_names)
        self.assertIn("Field Modifications", sheet_names)
        self.assertIn("Null Rate Drift", sheet_names)
        self.assertIn("Distribution Shifts", sheet_names)
        self.assertIn("Sample Row Diffs", sheet_names)


if __name__ == "__main__":
    unittest.main()
