"""Deterministic fault localisation over the dbt DAG.

No LLM. Given node statuses and dependencies, work out which node failed
first, which failures are merely downstream of it, and what it affected.

The agent receives the output of this module as a starting point. It decides
*why* the culprit failed; it never computes lineage itself.
"""

from collections.abc import Iterable
from dataclasses import dataclass

import networkx as nx

from pipeline_oncall.models import NodeResult


@dataclass(frozen=True)
class Localisation:
    culprits: tuple[str, ...]   # topological order, upstream first
    blast_radius: dict[str, tuple[str, ...]]  # culprit -> affected downstream
    cascade: tuple[str, ...] # failed/skipped that are not culprits
    failing_tests: dict[str, tuple[str, ...]]    # subject node -> failing test ids

    @property
    def primary_culprit(self) -> str | None:
        """The most upstream candidate, or None if nothing failed.
        More than one culprit means independent failures, which the baseline
        should treat as lower confidence rather than picking arbitrarily.
        """
        return self.culprits[0] if self.culprits else None

    @property
    def is_ambiguous(self) -> bool:
        return len(self.culprits) > 1

def build_graph(nodes: Iterable[NodeResult]) -> nx.DiGraph:
    """Dependency -> dependent, so ancestors() is upstream and descendants()
        is downstream."""
    nodes = list(nodes)
    graph = nx.DiGraph()

    for node in nodes:
        graph.add_node(
            node.unique_id,
            resource_type=node.resource_type,
            status=node.status,
        )

    for node in nodes:
        for dep in node.depends_on:
        # depends_on carries macro ids too; anything not in our node set
        # is not part of the data lineage.
            if dep in graph:
                graph.add_edge(dep, node.unique_id)

    if not nx.is_directed_acyclic_graph(graph):
        cycle = nx.find_cycle(graph)
        raise ValueError(f"dependency graph is cyclic: {cycle}")

    return graph

def resolve_test_subjects(test: NodeResult, graph: nx.DiGraph) -> tuple[str, ...]:
    """Which node(s) a failing test is actually about.

    Schema-defined tests carry attached_node. Singular tests do not, and
    depend on every model they reference -- assert_region_halfhourly_is_dno_only
    depends on both int_region_halfhourly and stg_regions. The subject is the
    most downstream of those: the dependency that is not an ancestor of any
    other dependency.

    Returns several ids only when the dependencies are genuinely incomparable,
    in which case the test implicates all of them.
    """
    if test.attached_node and test.attached_node in graph:
        return (test.attached_node,)

    deps = [d for d in test.depends_on if d in graph]
    if len(deps) <= 1:
        return tuple(deps)

    return tuple(
        d
        for d in deps
        if not any(other != d and d in nx.ancestors(graph,other) for other in deps)
    )


def localise (nodes: Iterable[NodeResult]) -> Localisation:
    """Find culprit candidate and their blast radius"""
    nodes = list(nodes)
    graph = build_graph(nodes)


    # A failing test is a signal about the node it tests, not a culprit in its
    # own right. Spec section 6.4: a failing not_null on stg_prices points at
    # stg_prices, not at the test node.

    failing_tests: dict[str, list[str]] = {}
    implicated: set[str] = set()

    for node in nodes:
        if node.resource_type != "test" or not node.failed:
            continue
        for subject in resolve_test_subjects(node, graph):
            failing_tests.setdefault(subject, []).append(node.unique_id)
            implicated.add(subject)

    directly_failed = {
        n.unique_id for n in nodes if n.failed and n.resource_type != "test"
    }
    failed = directly_failed | implicated

    # A culprit is a failure with no failed ancestor. Everything else with a
    # failed ancestor is downstream of something and therefore cascade.

    culprits = {node for node in failed if not (nx.ancestors(graph,node) & failed)}

    order = {uid: i for i, uid in enumerate(nx.topological_sort(graph))}
    ordered_culprits = tuple(sorted(culprits, key = lambda uid: order[uid]))

    radius = {
        culprit: _downstream(graph, culprit, order) for culprit in ordered_culprits
    }

    # Skipped nodes are collateral: dbt skips everything downstream of an
    # error. They are affected but they did not fail.

    affected = failed | {n.unique_id for n in nodes if n.skipped}
    cascade = tuple(
        sorted(affected - culprits, key=lambda uid: order[uid])
    )

    return Localisation(
        culprits = ordered_culprits,
        blast_radius = radius,
        cascade=cascade,
        failing_tests={k: tuple(v) for k, v in failing_tests.items()},

    )

def _downstream(graph: nx.DiGraph, node: str, order: dict[str, int]) -> tuple[str, ...]:
    """Descendants excluding tests.

     Blast radius answers "what data is affected". Tests are assertions about
     data, not data -- and including them would bury two affected models under
     forty test ids.
     """
    descendants  = [
        uid
        for uid in nx.descendants(graph,node)
        if graph.nodes[uid].get("resource_type") != "test"
    ]
    return tuple(sorted(descendants,key = lambda uid: order[uid]))