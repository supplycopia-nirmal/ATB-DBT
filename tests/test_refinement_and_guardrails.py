from autonomous_dbt_app.core.clinical_guardrails import (
    ClinicalUOMMatrix,
    OutlierSpikeGuard,
    CodeSanitizer,
    ReversalCreditMemoNetting,
    ClinicalGuardrailsEngine
)
from autonomous_dbt_app.core.institutional_knowledge import InstitutionalKnowledgeEngine
from autonomous_dbt_app.agents.refinement_agent import RefinementAgent

def test_uom_factor_detection():
    factor, norm = ClinicalUOMMatrix.detect_uom_factor("BX/10")
    assert factor == 10.0
    assert norm == "BX/10"

    factor, norm = ClinicalUOMMatrix.detect_uom_factor("CASE/50")
    assert factor == 50.0
    assert norm == "CA/50"

    factor, norm = ClinicalUOMMatrix.detect_uom_factor("DZ")
    assert factor == 12.0

    factor, norm = ClinicalUOMMatrix.detect_uom_factor("EA")
    assert factor == 1.0


def test_uom_pricing_mismatch():
    # Contract Each is $15.00, billed price is $150.00 (10x ratio matches Box of 10)
    mismatch = ClinicalUOMMatrix.check_uom_pricing_mismatch(150.0, 15.0)
    assert mismatch is not None
    assert mismatch["is_uom_mismatch"] is True
    assert mismatch["inferred_factor"] == 10.0
    assert mismatch["adjusted_billed_unit_price"] == 15.0

    # Normal price variance (e.g. $16.50 vs $15.00) should NOT trigger UOM packaging mismatch
    normal = ClinicalUOMMatrix.check_uom_pricing_mismatch(16.50, 15.0)
    assert normal is None


def test_outlier_spike_guard():
    # 10x decimal shift typo
    typo_eval = OutlierSpikeGuard.evaluate_price_spike(unit_price=450000.0, benchmark_median=45000.0)
    assert typo_eval["is_spike"] is True
    assert "10X" in typo_eval["severity"]

    # Normal price
    norm_eval = OutlierSpikeGuard.evaluate_price_spike(unit_price=46000.0, benchmark_median=45000.0)
    assert norm_eval["is_spike"] is False
    assert norm_eval["severity"] == "NORMAL"


def test_code_sanitizer():
    assert CodeSanitizer.pad_cpt_code("142") == "00142"
    assert CodeSanitizer.pad_cpt_code("99213") == "99213"
    assert CodeSanitizer.pad_drg_code("2") == "002"
    assert CodeSanitizer.clean_vendor_name("ST JUDE MEDICAL SC INC") == "ST JUDE MEDICAL"
    assert CodeSanitizer.clean_vendor_name("MEDTRONIC, LLC") == "MEDTRONIC"


def test_reversal_credit_memo_netting():
    records = [
        {"case_id": "1001", "item_number": "ITEM_A", "quantity": 5.0, "price": 100.0},
        {"case_id": "1001", "item_number": "ITEM_A", "quantity": -5.0, "price": 100.0},
        {"case_id": "1002", "item_number": "ITEM_B", "quantity": 2.0, "price": 50.0}
    ]
    active, audit = ReversalCreditMemoNetting.net_records(records)
    assert len(active) == 1
    assert active[0]["case_id"] == "1002"
    assert len(audit) == 1
    assert audit[0]["netted_quantity"] == 5.0


def test_institutional_knowledge_playbook():
    engine = InstitutionalKnowledgeEngine()
    initial_count = len(engine.get_all_learnings())
    assert initial_count >= 3

    new_l = engine.record_learning(
        category="UOM_TEST",
        title="Test UOM Factor for Mitraclip",
        rationale="Box of 2 unit verification",
        affected_models=["fct_consumption_cost_savings_v4"],
        rule_definition="supply_price / 2.0",
        impact="Saved $15k false variance",
        client_origin="UC Health"
    )
    assert new_l["learning_id"].startswith("LRN_")
    updated = engine.get_all_learnings()
    assert len(updated) == initial_count + 1


def test_refinement_agent_plan_lifecycle():
    agent = RefinementAgent()
    plan = agent.generate_implementation_plan(
        feedback_text="Surgical Tray for MitraClip was billed as Box of 2. Adjust UOM multiplier.",
        code_snippet="def uom_fix(p): return p / 2.0",
        client_name="UC Health"
    )
    assert plan["status"] == "PENDING_REVIEW"
    assert plan["revision"] == 1
    assert len(plan["proposed_diffs"]) > 0

    # Test reiterate
    revised = agent.reiterate_plan(plan["plan_id"], "Override factor to 2.0 strictly")
    assert revised["revision"] == 2
    assert revised["status"] == "REVISED"

    # Test execute
    exec_res = agent.execute_plan(plan["plan_id"])
    assert exec_res["status"] == "SUCCESS"
    assert "learning_id" in exec_res


def test_self_healing_engine():
    from autonomous_dbt_app.core.self_healing_engine import SelfHealingEngine
    healer = SelfHealingEngine()

    # 1. Test diagnose and plan for TEST_GAP_008
    err = "TEST_GAP_008: Clinical Gap Procedure Hierarchy Cardinality failed on fct_gap_analysis_v4. 304 Standard Procedures Mapped (Expected: COUNT(*) >= 448 AND COUNT(*) <= 1125 Standard Procedures)"
    plan = healer.diagnose_and_plan(err, "Automated Test Suite (TEST_GAP_008)")
    assert plan["status"] == "SUCCESS"
    assert plan["test_id"] == "TEST_GAP_008"
    assert "fct_gap_analysis_v4" in plan["target_table"]
    assert "304" in plan["plan"]

    # 2. Test surgical execution of TEST_GAP_008 patch
    exec_res = healer.execute_healing_patch(err, plan["plan"], user_feedback="Re-index canonical procedures")
    assert exec_res["status"] == "SUCCESS"
    assert exec_res["healed"] is True
    assert exec_res["assertion_status"] == "PASS"
    assert "1,125" in exec_res["after_metric"]

    # 3. Test gateway timeout recovery
    gw_err = "Unexpected token '<', \"<html> <head>... is not valid JSON\""
    gw_plan = healer.diagnose_and_plan(gw_err, "Pipeline Network/Compilation Fault")
    assert gw_plan["status"] == "SUCCESS"
    assert gw_plan["test_id"] == "NETWORK_GATEWAY_HEAL"
    gw_res = healer.execute_healing_patch(gw_err, gw_plan["plan"])
    assert gw_res["status"] == "SUCCESS"
    assert gw_res["healed"] is True

