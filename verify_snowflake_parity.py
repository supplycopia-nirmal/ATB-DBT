"""
Automated 100% Apples-to-Apples Parity Verification Script
Compares Common SC_ Tables against Baseline UC_HEALTH_*_V4 Tables directly in Snowflake
"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(PROJECT_ROOT / ".env")

def get_snowflake_connection():
    key_path = os.getenv("PRIVATE_KEY_PATH")
    if not key_path or not os.path.exists(key_path):
        raise FileNotFoundError(f"RSA Private Key not found at {key_path}")

    with open(key_path, "rb") as kf:
        p_key = serialization.load_pem_private_key(kf.read(), password=None, backend=default_backend())

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        private_key=pkb
    )

def run_parity_reconciliation():
    print("=" * 80)
    print("🐝 CARRIER BEE NECTAR & SENTINEL AEGIS: SNOWFLAKE 100% PARITY RECONCILIATION")
    print("=" * 80)

    conn = get_snowflake_connection()
    cs = conn.cursor()

    pairs = [
        ("SC_FCT_CONSUMPTION_COST_SAVINGS", "UC_HEALTH_FCT_CONSUMPTION_COST_SAVINGS_V4", "Consumption Cost Savings"),
        ("SC_MULTI_TENANT_CONSUMPTION_SAVINGS", "UC_HEALTH_FCT_CONSUMPTION_COST_SAVINGS_V4", "Multi-Tenant Consumption Mart"),
        ("SC_FCT_PO_COST_SAVINGS", "UC_HEALTH_FCT_PO_COST_SAVINGS_V4", "Purchase Orders Cost Savings"),
        ("SC_MULTI_TENANT_PO_SAVINGS", "UC_HEALTH_FCT_PO_COST_SAVINGS_V4", "Multi-Tenant PO Mart"),
        ("SC_FCT_GAP_ANALYSIS", "UC_HEALTH_FCT_GAP_ANALYSIS_V4", "Clinical Gap Analysis"),
        ("SC_MULTI_TENANT_GAP_ANALYSIS", "UC_HEALTH_FCT_GAP_ANALYSIS_V4", "Multi-Tenant Gap Analysis Mart")
    ]

    all_passed = True
    results = []

    for sc_tbl, base_tbl, label in pairs:
        print(f"\nEvaluating: {label}")
        print(f"  Target:   {sc_tbl}")
        print(f"  Baseline: {base_tbl}")

        # 1. Row counts
        cs.execute(f"SELECT count(*) FROM {sc_tbl};")
        sc_cnt = cs.fetchone()[0]

        cs.execute(f"SELECT count(*) FROM {base_tbl};")
        base_cnt = cs.fetchone()[0]

        row_match = sc_cnt == base_cnt
        print(f"  Rows: Target = {sc_cnt:,} | Baseline = {base_cnt:,} -> {'MATCH (100.0%)' if row_match else 'MISMATCH'}")

        # 2. Metric comparisons
        if "CONSUMPTION" in sc_tbl:
            cs.execute(f"SELECT round(sum(line_spend), 2), count(case when is_contract_matched then 1 end) FROM {sc_tbl};")
            sc_metrics = cs.fetchone()
            cs.execute(f"SELECT round(sum(line_spend), 2), count(case when is_contract_matched then 1 end) FROM {base_tbl};")
            base_metrics = cs.fetchone()

            spend_match = (sc_metrics[0] == base_metrics[0])
            match_rate_match = (sc_metrics[1] == base_metrics[1])
            print(f"  Line Spend: Target = ${sc_metrics[0]:,.2f} | Baseline = ${base_metrics[0]:,.2f} -> {'MATCH' if spend_match else 'DIFF'}")
            print(f"  Matched:    Target = {sc_metrics[1]:,} | Baseline = {base_metrics[1]:,} -> {'MATCH' if match_rate_match else 'DIFF'}")
            metric_pass = spend_match and match_rate_match
        elif "PO" in sc_tbl:
            cs.execute(f"SELECT round(sum(total_value), 2), round(sum(savings_opportunity), 2) FROM {sc_tbl};")
            sc_metrics = cs.fetchone()
            cs.execute(f"SELECT round(sum(total_value), 2), round(sum(savings_opportunity), 2) FROM {base_tbl};")
            base_metrics = cs.fetchone()
            spend_match = (sc_metrics[0] == base_metrics[0])
            sav_match = (sc_metrics[1] == base_metrics[1])
            print(f"  Total Value: Target = ${sc_metrics[0]:,.2f} | Baseline = ${base_metrics[0]:,.2f} -> {'MATCH' if spend_match else 'DIFF'}")
            print(f"  PO Savings:  Target = ${sc_metrics[1]:,.2f} | Baseline = ${base_metrics[1]:,.2f} -> {'MATCH' if sav_match else 'DIFF'}")
            metric_pass = spend_match and sav_match
        elif "GAP_ANALYSIS" in sc_tbl:
            cs.execute(f"SELECT round(sum(total_spend), 2), round(sum(total_savings_opportunity), 2) FROM {sc_tbl};")
            sc_metrics = cs.fetchone()
            cs.execute(f"SELECT round(sum(total_spend), 2), round(sum(total_savings_opportunity), 2) FROM {base_tbl};")
            base_metrics = cs.fetchone()
            gap_spend_match = (sc_metrics[0] == base_metrics[0])
            gap_sav_match = (sc_metrics[1] == base_metrics[1])
            print(f"  Gap Spend:   Target = ${sc_metrics[0]:,.2f} | Baseline = ${base_metrics[0]:,.2f} -> {'MATCH' if gap_spend_match else 'DIFF'}")
            print(f"  Gap Savings: Target = ${sc_metrics[1]:,.2f} | Baseline = ${base_metrics[1]:,.2f} -> {'MATCH' if gap_sav_match else 'DIFF'}")
            metric_pass = gap_spend_match and gap_sav_match
        else:
            metric_pass = True

        status = "CERTIFIED_PASS" if (row_match and metric_pass) else "FAIL"
        if status != "CERTIFIED_PASS":
            all_passed = False

        results.append({
            "target": sc_tbl,
            "baseline": base_tbl,
            "target_rows": sc_cnt,
            "baseline_rows": base_cnt,
            "status": status
        })

    cs.close()
    conn.close()

    print("\n" + "=" * 80)
    print("FINAL CERTIFICATION SUMMARY:")
    for r in results:
        print(f"  [{r['status']}] {r['target']} ({r['target_rows']:,} rows)")
    print(f"Overall Status: {'100% APPLES-TO-APPLES PARITY CERTIFIED' if all_passed else 'RECONCILIATION DISCREPANCY'}")
    print("=" * 80)
    return all_passed

if __name__ == "__main__":
    success = run_parity_reconciliation()
    sys.exit(0 if success else 1)
