"""Collector tests using hand-written dbt artifacts.

No dbt runs here. Each test writes a minimal target/ directory into tmp_path
and points the collector at it, which makes awkward situations -- a stale
sources.json, a result with no matching node -- trivial to construct.
"""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from pipeline_oncall.collectors.dbt_artifacts import load_artifacts
from pipeline_oncall.localise import localise

RUN_AT = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)

SCHEMAS = {
    "manifest": "https://schemas.getdbt.com/dbt/manifest/v12.json",
    "run_results": "https://schemas.getdbt.com/dbt/run-results/v6.json",
    "sources": "https://schemas.getdbt.com/dbt/sources/v3.json",
}



# Builders: the smallest shapes the collector actually reads
# --------------------------------------------------------------------------

def a_model(uid, *, deps=(), relation=None, path="models/bronze/m.sql"):
    return {
        "unique_id": uid,
        "resource_type": "model",
        "relation_name": relation,
        "original_file_path": path,
        # Real manifests list macros alongside nodes; the collector must
        # ignore them, since a macro is not part of data lineage.
        "depends_on": {"macros": ["macro.dbt.star"], "nodes": list(deps)},
    }


def a_test(uid, *, attached=None, deps=()):
    return {
        "unique_id": uid,
        "resource_type": "test",
        "relation_name": None,
        "original_file_path": "models/bronze/schema.yml",
        "attached_node": attached,
        "depends_on": {"macros": ["macro.dbt.test_not_null"], "nodes": list(deps)},
    }


def a_source(uid, *, relation='"db"."raw"."t"'):
    return {
        "unique_id": uid,
        "resource_type": "source",
        "relation_name": relation,
        "original_file_path": "models/sources.yml",
    }


def a_result(uid, status, *, message=None, failures=None, seconds=0.5):
    return {
        "unique_id": uid,
        "status": status,
        "message": message,
        "failures": failures,
        "execution_time": seconds,
    }


def a_freshness(uid, status, *, age_seconds=120.0):
    """sources.json results have no message field -- the facts are the numbers."""
    return {
        "unique_id": uid,
        "status": status,
        "max_loaded_at": "2026-09-01T11:58:00+00:00",
        "max_loaded_at_time_ago_in_s": age_seconds,
        "execution_time": 0.1,
    }


def _dump(path: Path, kind: str, body: dict, *, at: datetime, invocation: str) -> None:
    payload = {
        "metadata": {
            "dbt_schema_version": SCHEMAS[kind],
            "invocation_id": invocation,
            "generated_at": at.isoformat().replace("+00:00", "Z"),
            "dbt_version": "1.12.0",
        },
        **body,
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def write_target(
    tmp_path: Path,
    *,
    nodes=(),
    sources=(),
    results=(),
    freshness=None,
    freshness_age=timedelta(seconds=30),
    manifest_invocation="inv-build",
    run_invocation="inv-build",
) -> Path:
    """Write a target/ directory. freshness=None means no sources.json at all."""
    target = tmp_path / "target"
    target.mkdir(parents=True, exist_ok=True)

    _dump(
        target / "manifest.json",
        "manifest",
        {
            "nodes": {n["unique_id"]: n for n in nodes},
            "sources": {s["unique_id"]: s for s in sources},
        },
        at=RUN_AT,
        invocation=manifest_invocation,
    )
    _dump(
        target / "run_results.json",
        "run_results",
        {"results": list(results)},
        at=RUN_AT,
        invocation=run_invocation,
    )
    if freshness is not None:
        _dump(
            target / "sources.json",
            "sources",
            {"results": list(freshness)},
            at=RUN_AT - freshness_age,
            invocation="inv-freshness",
        )
    return target


def by_id(artifacts):
    return {n.unique_id: n for n in artifacts.nodes}



# Parsing
# --------------------------------------------------------------------------

def test_reads_models_tests_and_sources(tmp_path):
    target = write_target(
        tmp_path,
        nodes=[a_model("model.p.m"), a_test("test.p.t", attached="model.p.m")],
        sources=[a_source("source.p.s.raw")],
        results=[a_result("model.p.m", "success"), a_result("test.p.t", "pass")],
    )
    nodes = by_id(load_artifacts(target))

    assert set(nodes) == {"model.p.m", "test.p.t", "source.p.s.raw"}
    assert nodes["model.p.m"].status == "success"
    assert nodes["test.p.t"].resource_type == "test"
    # Sources live in their own top-level dict, not in manifest["nodes"].
    assert nodes["source.p.s.raw"].resource_type == "source"


def test_macro_dependencies_are_dropped(tmp_path):
    target = write_target(
        tmp_path,
        nodes=[a_model("model.p.m", deps=["source.p.s.raw"])],
        sources=[a_source("source.p.s.raw")],
        results=[a_result("model.p.m", "success")],
    )
    assert by_id(load_artifacts(target))["model.p.m"].depends_on == ["source.p.s.raw"]


def test_relation_name_is_unquoted_to_schema_dot_table(tmp_path):
    target = write_target(
        tmp_path,
        nodes=[a_model("model.p.m", relation='"oncall"."bronze"."stg_x"')],
        results=[a_result("model.p.m", "success")],
    )
    assert by_id(load_artifacts(target))["model.p.m"].relation == "bronze.stg_x"


def test_windows_paths_are_normalised(tmp_path):
    target = write_target(
        tmp_path,
        nodes=[a_model("model.p.m", path="models\\bronze\\m.sql")],
        results=[a_result("model.p.m", "success")],
    )
    assert by_id(load_artifacts(target))["model.p.m"].file_path == "models/bronze/m.sql"


def test_failure_counts_are_captured(tmp_path):
    target = write_target(
        tmp_path,
        nodes=[a_test("test.p.t", attached="model.p.m")],
        results=[a_result("test.p.t", "fail", message="Got 3630 results", failures=3630)],
    )
    node = by_id(load_artifacts(target))["test.p.t"]

    assert node.failures == 3630
    assert node.message == "Got 3630 results"


def test_unknown_status_is_rejected(tmp_path):
    """If a future dbt invents a status, fail loudly here rather than
    silently treating it as healthy."""
    target = write_target(
        tmp_path,
        nodes=[a_model("model.p.m")],
        results=[a_result("model.p.m", "exploded")],
    )
    with pytest.raises(ValidationError):
        load_artifacts(target)



# "not run": the status dbt never emits
# --------------------------------------------------------------------------

def test_node_with_no_result_is_not_run(tmp_path):
    target = write_target(
        tmp_path,
        nodes=[a_model("model.p.ran"), a_model("model.p.absent")],
        sources=[a_source("source.p.s.raw")],
        results=[a_result("model.p.ran", "success")],
    )
    nodes = by_id(load_artifacts(target))

    assert nodes["model.p.absent"].status == "not run"
    assert nodes["source.p.s.raw"].status == "not run"


def test_not_run_is_neither_failed_nor_skipped(tmp_path):
    """The whole reason the status exists. Marked "skipped" instead, every
    healthy build would report three sources as cascade."""
    target = write_target(
        tmp_path,
        nodes=[a_model("model.p.m", deps=["source.p.s.raw"])],
        sources=[a_source("source.p.s.raw")],
        results=[a_result("model.p.m", "success")],
    )
    artifacts = load_artifacts(target)
    source = by_id(artifacts)["source.p.s.raw"]

    assert not source.failed
    assert not source.skipped
    assert localise(artifacts.nodes).cascade == ()



# Freshness, and the stale sources.json guard
# --------------------------------------------------------------------------

def test_recent_freshness_results_are_merged(tmp_path):
    target = write_target(
        tmp_path,
        nodes=[],
        sources=[a_source("source.p.s.raw")],
        results=[],
        freshness=[a_freshness("source.p.s.raw", "error", age_seconds=28800.0)],
        freshness_age=timedelta(seconds=30),
    )
    artifacts = load_artifacts(target)

    assert artifacts.freshness_checked
    assert artifacts.ignored_sources is None
    assert by_id(artifacts)["source.p.s.raw"].status == "error"


def test_freshness_message_is_built_from_the_numbers(tmp_path):
    """sources.json has no message field, so one is composed from what it
    does record rather than leaving the failure unexplained."""
    target = write_target(
        tmp_path,
        sources=[a_source("source.p.s.raw")],
        freshness=[a_freshness("source.p.s.raw", "error", age_seconds=28800.0)],
    )
    message = by_id(load_artifacts(target))["source.p.s.raw"].message

    assert "8.0h ago" in message
    assert "2026-09-01T11:58:00+00:00" in message


def test_stale_sources_json_is_ignored(tmp_path):
    target = write_target(
        tmp_path,
        sources=[a_source("source.p.s.raw")],
        freshness=[a_freshness("source.p.s.raw", "error")],
        freshness_age=timedelta(days=7),
    )
    artifacts = load_artifacts(target)

    assert not artifacts.freshness_checked
    assert artifacts.ignored_sources is not None
    assert by_id(artifacts)["source.p.s.raw"].status == "not run"


def test_stale_sources_json_would_otherwise_invent_a_culprit(tmp_path):
    """Regression test for the real substrate/target/, where a week-old
    sources.json reported every source ERROR against a healthy build."""
    target = write_target(
        tmp_path,
        nodes=[a_model("model.p.m", deps=["source.p.s.raw"])],
        sources=[a_source("source.p.s.raw")],
        results=[a_result("model.p.m", "success")],
        freshness=[a_freshness("source.p.s.raw", "error")],
        freshness_age=timedelta(days=7),
    )

    assert localise(load_artifacts(target).nodes).culprits == ()

    let_it_through = load_artifacts(target, max_freshness_lag=timedelta(days=365))
    assert localise(let_it_through.nodes).culprits == ("source.p.s.raw",)



# Artifact hygiene
# --------------------------------------------------------------------------

def test_missing_sources_json_is_fine(tmp_path):
    target = write_target(
        tmp_path,
        nodes=[a_model("model.p.m")],
        results=[a_result("model.p.m", "success")],
    )
    artifacts = load_artifacts(target)

    assert not artifacts.freshness_checked
    assert artifacts.ignored_sources is None


@pytest.mark.parametrize("missing", ["manifest.json", "run_results.json"])
def test_missing_required_artifact_raises(tmp_path, missing):
    target = write_target(
        tmp_path,
        nodes=[a_model("model.p.m")],
        results=[a_result("model.p.m", "success")],
    )
    (target / missing).unlink()

    with pytest.raises(FileNotFoundError, match=missing):
        load_artifacts(target)


def test_wrong_artifact_type_raises(tmp_path):
    """Catches a swapped or truncated file rather than parsing nonsense."""
    target = write_target(
        tmp_path,
        nodes=[a_model("model.p.m")],
        results=[a_result("model.p.m", "success")],
    )
    (target / "manifest.json").write_text(
        (target / "run_results.json").read_text(encoding="utf-8"), encoding="utf-8"
    )

    with pytest.raises(ValueError, match="not a dbt manifest artifact"):
        load_artifacts(target)


def test_orphaned_results_are_recorded(tmp_path):
    """A result for a node the manifest no longer has -- run_results.json left
    over from before a model was renamed."""
    target = write_target(
        tmp_path,
        nodes=[a_model("model.p.m")],
        results=[a_result("model.p.m", "success"), a_result("model.p.deleted", "error")],
    )
    assert load_artifacts(target).orphaned_results == ("model.p.deleted",)


def test_manifest_rewritten_by_a_later_command_is_detected(tmp_path):
    """`dbt source freshness` rewrites manifest.json. Run after a build, the
    manifest and run results carry different invocation ids."""
    target = write_target(
        tmp_path,
        nodes=[a_model("model.p.m")],
        results=[a_result("model.p.m", "success")],
        manifest_invocation="inv-freshness",
        run_invocation="inv-build",
    )
    assert not load_artifacts(target).manifest_matches_build



# End to end: collector output straight into the localiser
# --------------------------------------------------------------------------

def test_failing_test_localises_to_its_model(tmp_path):
    target = write_target(
        tmp_path,
        nodes=[
            a_model("model.p.stg", deps=["source.p.s.raw"]),
            a_model("model.p.mart", deps=["model.p.stg"]),
            a_test("test.p.not_null", attached="model.p.stg", deps=["model.p.stg"]),
        ],
        sources=[a_source("source.p.s.raw")],
        results=[
            a_result("model.p.stg", "success"),
            a_result("model.p.mart", "success"),
            a_result("test.p.not_null", "fail", message="Got 3630 results", failures=3630),
        ],
    )
    result = localise(load_artifacts(target).nodes)

    assert result.primary_culprit == "model.p.stg"
    assert result.blast_radius["model.p.stg"] == ("model.p.mart",)

