from __future__ import annotations

import importlib.util
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "benchmark-optimization-campaign.py"
SPEC = importlib.util.spec_from_file_location("optimization_campaign_evidence_status", SCRIPT)
campaign = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(campaign)


def complete_stage(candidate: str, status: str = "deferred") -> dict:
    return {
        "comparison": {"status": status},
        "samples": [
            {"category": side, "pair": pair}
            for pair in (1, 2, 3)
            for side in ("baseline", candidate)
        ],
    }


def test_all_candidates_skipped_for_budget_is_insufficient_evidence():
    report = {"stages": {}}
    assert campaign._v2_unqualified_status(["outcome_grouping", "queue"], report) == (
        "insufficient_evidence"
    )


def test_one_completed_negative_and_one_skipped_candidate_is_insufficient_evidence():
    report = {"stages": {"outcome_grouping_vs_baseline": complete_stage("outcome_grouping")}}
    assert campaign._v2_unqualified_status(["outcome_grouping", "queue"], report) == (
        "insufficient_evidence"
    )


def test_fully_measured_candidates_that_miss_gate_are_deferred():
    report = {
        "stages": {
            "outcome_grouping_vs_baseline": complete_stage("outcome_grouping"),
            "queue_vs_baseline": complete_stage("queue"),
        }
    }
    assert campaign._v2_unqualified_status(["outcome_grouping", "queue"], report) == "deferred"


def test_no_eligible_candidate_is_deferred():
    assert campaign._v2_unqualified_status([], {"stages": {}}) == "deferred"


def test_missing_pair_is_not_a_complete_negative_comparison():
    stage = complete_stage("queue")
    stage["samples"].pop()
    report = {"stages": {"queue_vs_baseline": stage}}
    assert campaign._v2_unqualified_status(["queue"], report) == "insufficient_evidence"
