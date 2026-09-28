"""Evidence Log tests. Plain file I/O in tmp_path, not dbt"""

import json
from datetime import UTC, datetime

from pipeline_oncall.evidence import EvidenceLog
from pipeline_oncall.models import Finding, IncidentReport, RootCause


def report_citing(*ids: str) -> IncidentReport:
    return IncidentReport(
        incident_id="inc",
        summary="s",
        status="complete",
        root_cause=RootCause.SOURCE_DATA_QUALITY,
        findings=[Finding(summary="f", evidence_ids=list(ids))],
    )

def test_ids_are_sequential_from_one(tmp_path):
    log = EvidenceLog(tmp_path / "inc")
    ids = [log.record("t", {}, f"s{i}", {}).id for i in range(3)]

    assert ids == ["ev_001","ev_002","ev_003"]


def test_each_incident_numbers_independently(tmp_path):
    first = EvidenceLog(tmp_path / "a")
    first.record("t",{},"s",{})
    second = EvidenceLog(tmp_path / "b")

    assert second.record("t",{},"s",{}).id == "ev_001"

def test_payload_is_written_and_raw_ref_is_relative(tmp_path):
    log = EvidenceLog(tmp_path / "inc")
    evidence = log.record("dbt_artifacts", {"file": "x"}, "summary", {"failures": 3630})

    assert evidence.raw_ref == "inc/ev_001.json"
    payload = json.loads((tmp_path / evidence.raw_ref).read_text(encoding="utf-8"))
    assert payload == {"failures": 3630}

def test_payloads_with_datetimes_still_serialise(tmp_path):
    """"dbt artifacts are full of timestamps; recording one must not crash."""
    log = EvidenceLog(tmp_path / "inc")
    evidence = log.record("t", {}, "s", {"at": datetime(2026, 9, 1, tzinfo=UTC)})

    assert "2026-09-01" in (tmp_path / evidence.raw_ref).read_text(encoding="utf-8")

def test_dandling_is_empty_when_every_citation_was_issued(tmp_path):
    log = EvidenceLog(tmp_path / "inc")
    log.record("t", {}, "s", {})
    log.record("t", {}, "s", {})

    assert log.dangling(report_citing("ev_001", "ev_002")) == set()

def test_dangling_catches_a_fabricated_citation(tmp_path):
    log = EvidenceLog(tmp_path / "inc")
    log.record("t", {}, "s", {})
    assert log.dangling(report_citing("ev_001", "ev_042")) == {"ev_042"}

def test_index_lists_every_record(tmp_path):
    log = EvidenceLog(tmp_path / "inc")
    log.record("a", {"k": 1}, "first", {})
    log.record("b", {}, "second", {})

    index = json.loads(log.write_index().read_text(encoding="utf-8"))

    assert [e["id"] for e in index] == ["ev_001", "ev_002"]
    assert index[0]["args"] == {"k": 1}