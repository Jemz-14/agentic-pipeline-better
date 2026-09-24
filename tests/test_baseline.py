"""Baseline tests over hand-built nodes. No dbt."""

from pipeline_oncall.baseline import classify
from pipeline_oncall.localise import localise
from pipeline_oncall.models import NodeResult, RootCause

# The exact text dbt produced for drop_upstream_column on this substrate.
DROPPED_COLUMN = (
    "Runtime Error in model stg_x (models\\bronze\\stg_x.sql)\n"
    '  Binder Error: Referenced column "intensity_index" not found in FROM clause!\n'
    '  Candidate bindings: "intensity_forecast", "regionid"'
)


def node(uid, *, rt="model", status="success", deps=(), attached=None,
         message=None, failures=None):
    return NodeResult(
        unique_id=uid, resource_type=rt, status=status, depends_on=list(deps),
        attached_node=attached, message=message, failures=failures,
    )


def run(*nodes):
    return classify(nodes, localise(nodes))


def test_healthy_build_is_not_an_incident():
    result = run(node("source.p.s", rt="source"), node("model.p.stg", deps=["source.p.s"]))

    assert result.root_cause is None
    assert result.rule == "no_failures"


def test_failed_source_is_stale_freshness():
    result = run(node("source.p.s", rt="source", status="error", message="8.0h ago"))

    assert result.root_cause is RootCause.SOURCE_FRESHNESS_STALE
    assert result.confidence == "high"
    assert "8.0h ago" in result.reason


def test_missing_column_is_schema_drift():
    result = run(
        node("source.p.s", rt="source", status="not run"),
        node("model.p.stg", status="error", deps=["source.p.s"], message=DROPPED_COLUMN),
    )

    assert result.root_cause is RootCause.UPSTREAM_SCHEMA_DRIFT
    assert result.confidence == "high"
    assert '"intensity_index"' in result.reason


def test_reason_quotes_the_matching_line_not_the_wrapper():
    """dbt's first line is "Runtime Error in model ...". The useful one is second."""
    result = run(node("model.p.stg", status="error", message=DROPPED_COLUMN))

    assert "Runtime Error in model" not in result.reason


def test_empty_model_is_a_missing_relation():
    """This repo's own incident: the model never ran and its tests errored
    because the table did not exist."""
    nodes = [node("model.p.agg", status="not run")]
    nodes += [
        node(f"test.p.t{i}", rt="test", status="error", attached="model.p.agg",
             deps=["model.p.agg"],
             message="Catalog Error: Table with name agg does not exist!")
        for i in range(8)
    ]
    result = run(*nodes)

    assert result.root_cause is RootCause.CONFIG_OR_PERMISSIONS
    assert result.rule == "missing_relation"
    assert result.culprit == "model.p.agg"


def test_permission_denied_is_config():
    result = run(node("model.p.m", status="error", message="permission denied for table t"))
    assert result.root_cause is RootCause.CONFIG_OR_PERMISSIONS
    assert result.confidence == "high"


def test_timeout_is_infra_transient():
    result = run(node("model.p.m", status="error", message="query timed out after 300s"))
    assert result.root_cause is RootCause.INFRA_TRANSIENT


def test_unrecognised_error_is_unknown_not_a_guess():
    result = run(node("model.p.m", status="error", message="something new and strange"))

    assert result.root_cause is RootCause.UNKNOWN
    assert result.rule == "unrecognised_error"


def test_most_specific_cause_wins_across_nodes():
    """Schema drift outranks a timeout even when the timeout is on the culprit."""
    result = run(
        node("model.p.m", status="error", message="connection reset"),
        node("test.p.t", rt="test", status="error", attached="model.p.m",
             deps=["model.p.m"], message=DROPPED_COLUMN),
    )
    assert result.rule == "schema_drift"


def test_test_failure_on_staging_is_source_data_quality():
    result = run(
        node("source.p.s", rt="source", status="not run"),
        node("model.p.stg", deps=["source.p.s"]),
        node("test.p.nn", rt="test", status="fail", attached="model.p.stg",
             deps=["model.p.stg"], failures=3630),
    )

    assert result.root_cause is RootCause.SOURCE_DATA_QUALITY
    assert "3630 rows" in result.reason


def test_test_failure_downstream_suspects_the_logic():
    result = run(
        node("model.p.stg"),
        node("model.p.mart", deps=["model.p.stg"]),
        node("test.p.rng", rt="test", status="fail", attached="model.p.mart",
             deps=["model.p.mart"], failures=14),
    )

    assert result.root_cause is RootCause.TRANSFORMATION_LOGIC_BUG
    assert result.confidence == "low"


def test_warn_does_not_create_an_incident():
    result = run(
        node("model.p.m"),
        node("test.p.t", rt="test", status="warn", attached="model.p.m", deps=["model.p.m"]),
    )
    assert result.root_cause is None


def test_independent_failures_lower_confidence_and_are_named():
    result = run(
        node("model.p.a", status="error", message=DROPPED_COLUMN),
        node("model.p.b", status="error", message="permission denied"),
    )

    assert result.confidence == "medium"          # was high
    assert "Independent failures also at" in result.reason