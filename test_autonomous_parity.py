import os
import sys
import json
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

from autonomous_dbt_app.orchestrator.pipeline_orchestrator import PipelineOrchestrator

def main():
    print("=================================================================")
    print("🚀 Running Autonomous Multi-Agent DBT Pipeline Studio & Parity Test")
    print("=================================================================")

    orchestrator = PipelineOrchestrator()
    data_folder = PROJECT_ROOT / "Data"
    baseline_db = PROJECT_ROOT / "uc_health" / "uc_health.duckdb"

    print(f"Data Folder: {data_folder}")
    print(f"Baseline DB: {baseline_db}")

    result = orchestrator.run_full_pipeline(data_folder, baseline_db_path=baseline_db)

    print("\n----------------- PIPELINE RESULT SUMMARY -----------------")
    print(f"Status: {result.get('status')}")
    print(f"Autonomous DuckDB: {result.get('autonomous_duckdb_path')}")

    parity = result.get("parity_report", {})
    print("\n----------------- 100% GOLDEN PARITY REPORT -----------------")
    print(f"Parity Status: {parity.get('status')}")
    print(f"Parity Match Percentage: {parity.get('overall_parity_match_pct')}%")
    print("\nTable Breakdowns:")
    for tbl, d in parity.get("table_results", {}).items():
        print(f"  • {tbl}:")
        print(f"      Baseline Rows: {d.get('baseline_row_count'):,} | Auto Rows: {d.get('autonomous_row_count'):,}")
        print(f"      Row Count Match: {d.get('row_count_match')}")
        print(f"      Metrics Match: {d.get('parity_match')}")
        if "metrics_comparison" in d:
            for m_name, m_val in d["metrics_comparison"].items():
                print(f"        - {m_name}: baseline={m_val.get('baseline')}, auto={m_val.get('autonomous')}, match={m_val.get('match')}")

    if parity.get("overall_parity_match_pct") == 100.0:
        print("\n🎉 SUCCESS: 100% Golden Parity Achieved between autonomous and baseline pipelines!")
    else:
        print("\n⚠️ Note: Parity differs, check report details above.")

if __name__ == "__main__":
    main()
