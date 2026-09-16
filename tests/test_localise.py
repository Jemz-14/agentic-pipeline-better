"""Localiser tests over synthetic DAGs.

Nothing here touches dbt, a manifest or a database. The localiser takes node
statuses and dependencies and returns culprits and blast radius, so it can be
exercised on graph shapes chosen to expose specific mistakes.
"""

import pytest

from pipeline_oncall.localise import build_graph, localise, resolve_test_subjects
from pipeline_oncall.models import NodeResult


def node(uid, *, rt="model", status="success", deps=(), attached=None) -> NodeResult:
    return NodeResult(
        unique_id=uid,
        resource_type=rt,
        status=status,
        depends_on=list(deps),
        attached_node=attached,
    )


# --------------------------------------------------------------------------
# Cascade: one failure upstream, everything downstream is collateral
# --------------------------------------------------------------------------

def test_linear_cascade_blames_only_the_head():
    result = localise([
        node("a", status="error"),
        node("b", status="skipped", deps=["a"]),
        node("c", status="skipped", deps=["b"]),
    ])

    assert result.culprits == ("a",)
    assert result.primary_culprit == "a"
    assert result.blast_radius["a"] == ("b", "c")
    assert result.cascade == ("b", "c")
    assert not result.is_ambiguous


def test_downstream_failure_is_not_a_culprit():
    """b failed too, but it has a failed ancestor, so a is still the culprit."""
    result = localise([
        node("a", status="error"),
        node("b", status="error", deps=["a"]),
    ])

    assert result.culprits == ("a",)
    assert "b" in result.cascade


def test_diamond_blast_radius_reaches_the_join():
    result = localise([
        node("a", status="error"),
        node("b", deps=["a"]),
        node("c", deps=["a"]),
        node("d", deps=["b", "c"]),
    ])

    assert result.culprits == ("a",)
    assert set(result.blast_radius["a"]) == {"b", "c", "d"}


def test_blast_radius_is_topological_not_alphabetical():
    """a -> z -> m. Sorting by name would put m before z and imply the wrong
    direction of flow."""
    result = localise([
        node("a", status="error"),
        node("z", deps=["a"]),
        node("m", deps=["z"]),
    ])

    assert result.blast_radius["a"] == ("z", "m")


# --------------------------------------------------------------------------
# Independent failures
# --------------------------------------------------------------------------

def test_two_disconnected_failures_are_both_culprits():
    result = localise([
        node("a", status="error"),
        node("b", deps=["a"]),
        node("x", status="error"),
        node("y", deps=["x"]),
    ])

    assert set(result.culprits) == {"a", "x"}
    assert result.is_ambiguous


def test_sibling_failures_under_a_healthy_parent_are_both_culprits():
    result = localise([
        node("root"),
        node("a", status="error", deps=["root"]),
        node("b", status="error", deps=["root"]),
    ])

    assert set(result.culprits) == {"a", "b"}


# --------------------------------------------------------------------------
# Tests attach to the node they test, and are never culprits themselves
# --------------------------------------------------------------------------

def test_failing_test_blames_its_model_not_itself():
    result = localise([
        node("m"),
        node("t", rt="test", status="fail", deps=["m"], attached="m"),
    ])

    assert result.culprits == ("m",)
    assert "t" not in result.culprits
    assert result.failing_tests["m"] == ("t",)


def test_failing_test_on_a_downstream_model_is_still_cascade():
    result = localise([
        node("a", status="error"),
        node("b", status="skipped", deps=["a"]),
        node("t", rt="test", status="fail", deps=["b"], attached="b"),
    ])

    assert result.culprits == ("a",)
    assert "b" in result.cascade


def test_blast_radius_excludes_test_nodes():
    result = localise([
        node("a", status="error"),
        node("b", deps=["a"]),
        node("t", rt="test", status="pass", deps=["b"], attached="b"),
    ])

    assert result.blast_radius["a"] == ("b",)


def test_passing_tests_implicate_nothing():
    result = localise([
        node("m"),
        node("t", rt="test", status="pass", deps=["m"], attached="m"),
    ])

    assert result.culprits == ()
    assert result.primary_culprit is None


def test_warn_is_not_a_failure():
    """A warn-severity test does not fail the build and dbt exits 0."""
    result = localise([
        node("m"),
        node("t", rt="test", status="warn", deps=["m"], attached="m"),
    ])

    assert result.culprits == ()


# --------------------------------------------------------------------------
# Singular tests: no attached_node, several dependencies
# --------------------------------------------------------------------------

def test_singular_test_picks_the_most_downstream_dependency():
    """Mirrors assert_region_halfhourly_is_dno_only, which depends on both
    int_region_halfhourly and stg_regions. stg_regions is an ancestor, so the
    subject is int_region_halfhourly."""
    nodes = [
        node("stg"),
        node("int", deps=["stg"]),
        node("t", rt="test", status="fail", deps=["stg", "int"]),
    ]
    graph = build_graph(nodes)

    assert resolve_test_subjects(nodes[2], graph) == ("int",)
    assert localise(nodes).culprits == ("int",)


def test_singular_test_over_incomparable_models_implicates_both():
    """No principled way to choose, so it names both rather than guessing."""
    nodes = [
        node("a"),
        node("b"),
        node("t", rt="test", status="fail", deps=["a", "b"]),
    ]
    graph = build_graph(nodes)

    assert set(resolve_test_subjects(nodes[2], graph)) == {"a", "b"}
    assert set(localise(nodes).culprits) == {"a", "b"}


def test_attached_node_wins_over_depends_on():
    nodes = [
        node("stg"),
        node("int", deps=["stg"]),
        node("t", rt="test", status="fail", deps=["stg", "int"], attached="stg"),
    ]
    graph = build_graph(nodes)

    assert resolve_test_subjects(nodes[2], graph) == ("stg",)


# --------------------------------------------------------------------------
# Skipped, and other edges
# --------------------------------------------------------------------------

def test_skipped_nodes_are_never_culprits():
    """Skipped means never ran. A node that never ran cannot have caused
    anything."""
    result = localise([node("s", status="skipped")])

    assert result.culprits == ()
    assert result.cascade == ("s",)


def test_healthy_run_produces_no_culprits():
    result = localise([node("a"), node("b", deps=["a"])])

    assert result.culprits == ()
    assert result.cascade == ()
    assert result.primary_culprit is None


def test_macro_dependencies_are_ignored():
    """depends_on carries macro ids that are not part of data lineage."""
    graph = build_graph([
        node("m", deps=["macro.dbt.test_not_null", "source.p.s"]),
        node("source.p.s", rt="source"),
    ])

    assert set(graph.nodes) == {"m", "source.p.s"}
    assert graph.has_edge("source.p.s", "m")


def test_cyclic_graph_raises():
    with pytest.raises(ValueError, match="cyclic"):
        build_graph([
            node("a", deps=["b"]),
            node("b", deps=["a"]),
        ])


# --------------------------------------------------------------------------
# Acceptance: the empty-model incident from this repo's own history
# --------------------------------------------------------------------------

def test_empty_model_incident():
    """agg_region_daily.sql was committed as a 0-byte file. dbt parsed it into
    a valid node with no dependencies, materialised nothing, and ran its eight
    tests first -- all of which failed with "table does not exist". Every
    error message named schema.yml, which was entirely correct.

    The localiser has to see through that: one culprit, eight cascade tests.
    """
    nodes = [
        node("model.oncall.int_region_halfhourly"),
        node("model.oncall.agg_region_daily", status="skipped"),
    ]
    nodes += [
        node(
            f"test.oncall.t{i}",
            rt="test",
            status="error",
            deps=["model.oncall.agg_region_daily"],
            attached="model.oncall.agg_region_daily",
        )
        for i in range(8)
    ]

    result = localise(nodes)

    assert result.culprits == ("model.oncall.agg_region_daily",)
    assert not result.is_ambiguous
    assert len(result.failing_tests["model.oncall.agg_region_daily"]) == 8
    assert result.blast_radius["model.oncall.agg_region_daily"] == ()