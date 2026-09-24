"""Rules baseline: classify an incident without an LLM.

Two jobs. It is the fallback when the agent's output fails validation, and it
is the number the agent is measured against. It encodes the checks an on-call
engineer would write on day one. It must not be tuned until it scores
perfectly on this repo's own scenarios: that would make it a lookup table and
the agent-versus-baseline comparison meaningless.
"""

import re
from collections.abc import Iterable
from dataclasses import dataclass, replace
from typing import Literal

from pipeline_oncall.localise import Localisation
from pipeline_oncall.models import NodeResult, RootCause

Confidence = Literal["high", "medium", "low"]

ERRORED = frozenset({"error", "runtime error"})

@dataclass(frozen=True)
class Classification:
    """Rule for classifying an incident without an LLM."""
    root_cause: RootCause | None        # None means no incident
    confidence: Confidence
    rule: str                           # Which rule fired; the eva; breals accuracy dopwn by it
    reason: str
    culprit: str
    supporting: tuple[str,...] = ()    # Unique ids whose reuslts justify the call



# Error-message patterns, checked in this order. Schema drift comes before
# missing relations because both can say "does not exist".
SCHEMA_DRIFT = re.compile(
    r"referenced column .* not found|column .* (?:does not exist|not found)"
    r"|no such column|invalid identifier",
    re.IGNORECASE,
)
PERMISSIONS = re.compile(
    r"permission denied|access denied|not authori[sz]ed|insufficient privilege",
    re.IGNORECASE,
)
MISSING_RELATION = re.compile(
    r"table with name .* does not exist|relation .* does not exist|no such table",
    re.IGNORECASE,
)
INFRA = re.compile(
    r"timed out|timeout|connection (?:reset|refused|closed)|could not connect"
    r"|deadlock|too many connections|temporarily unavailable|out of memory",
    re.IGNORECASE,
)

MESSAGE_RULES: tuple[tuple[str, re.Pattern[str], RootCause, Confidence], ...] = (
    ("schema_drift", SCHEMA_DRIFT, RootCause.UPSTREAM_SCHEMA_DRIFT, "high"),
    ("permissions", PERMISSIONS, RootCause.CONFIG_OR_PERMISSIONS, "high"),
    ("missing_relation", MISSING_RELATION, RootCause.CONFIG_OR_PERMISSIONS, "medium"),
    ("infra_transient", INFRA, RootCause.INFRA_TRANSIENT, "medium"),
)

_COLUMN = re.compile(r'column "(\w+)"', re.IGNORECASE)
_LOWER: dict[Confidence, Confidence] = {"high": "medium", "medium": "low", "low": "low"}

def classify(nodes: Iterable[NodeResult], localisation: Localisation) -> Classification:
    """Classify the incident the localiser found."""
    by_id = {n.unique_id: n for n in nodes}
    culprit_id = localisation.primary_culprit

    if culprit_id is None:
        return Classification(
            root_cause=None,
            confidence="high",
            rule="no_failures",
            reason="No node failed.",
            culprit=None,
        )

    culprit = by_id[culprit_id]
    tests = [by_id[t] for t in localisation.failing_tests.get(culprit_id, ())]
    result = _classify_culprit(culprit, tests)

    # Several independent failures: the rule only explains the first one.
    if localisation.is_ambiguous:
        others = ", ".join(localisation.culprits[1:])
        result = replace(
            result,
            confidence=_LOWER[result.confidence],
            reason=f"{result.reason} Independent failures also at: {others}.",
        )
    return result


def _classify_culprit(culprit: NodeResult, tests: list[NodeResult]) -> Classification:
    cid = culprit.unique_id

    # Rule 1. A source can only be a culprit through a failed freshness check.
    if culprit.resource_type == "source":
        return Classification(
            root_cause=RootCause.SOURCE_FRESHNESS_STALE,
            confidence="high",
            rule="stale_source",
            reason=f"Freshness check failed on {cid}: {culprit.message}.",
            culprit=cid,
            supporting=(cid,),
        )

    # Rules 2-5. Something could not run at all, and its error text says why.
    # Rules are the outer loop, so the most specific cause wins no matter
    # which node's message carries it.

    errored = [n for n in [culprit, *tests] if n.status in ERRORED]
    for rule, pattern, cause, confidence in MESSAGE_RULES:
        for node in errored:
            line = _matching_line(node.message, pattern)
            if line:
                return Classification(
                    root_cause=cause,
                    confidence=confidence,
                    rule=rule,
                    reason=_explain(rule, node.unique_id, line),
                    culprit=cid,
                    supporting=(node.unique_id,),
                )


    if errored:
        return Classification(
            root_cause=RootCause.UNKNOWN,
            confidence="low",
            rule="unrecognised_error",
            reason=f"{errored[0].unique_id} errored with a message matching no known pattern.",
            culprit=cid,
            supporting=tuple(n.unique_id for n in errored),
        )

    # Rules 6-7. Everything ran, but the data violated tests.
    failed = [t for t in tests if t.status == "fail"]
    if failed:
        ids = tuple(t.unique_id for t in failed)
        rows = sum(t.failures or 0 for t in failed)
        if _reads_a_source(culprit):
            return Classification(
                root_cause=RootCause.SOURCE_DATA_QUALITY,
                confidence="medium",
                rule="source_data_quality",
                reason=(
                    f"{len(failed)} test(s) failed on {cid} ({rows} rows), which reads "
                    "directly from a source, so the data most likely arrived bad."
                ),
                culprit=cid,
                supporting=ids,
            )
        return Classification(
            root_cause=RootCause.TRANSFORMATION_LOGIC_BUG,
            confidence="low",
            rule="downstream_test_failure",
            reason=(
                f"{len(failed)} test(s) failed on {cid} ({rows} rows) while its inputs "
                "passed, so its own logic is the first suspect."
            ),
            culprit=cid,
            supporting=ids,
        )

    return Classification(
        root_cause=RootCause.UNKNOWN,
        confidence="low",
        rule="no_rule_matched",
        reason=f"{cid} was localised as the culprit but no rule explains why.",
        culprit=cid,
    )

def _matching_line(message: str | None, pattern: re.Pattern[str]) -> str | None:
    """The first line of a multi-line dbt error that matches, stripped.

    dbt wraps the useful line: "Runtime Error in model ..." comes first and the
    actual Binder Error second. Quoting the matched line keeps the reason short
    and factual.
    """
    if not message:
        return None
    for line in message.splitlines():
        if pattern.search(line):
            return line.strip()
    return None


def _explain(rule: str, node_id: str, line: str) -> str:
    if rule == "schema_drift":
        column = _COLUMN.search(line)
        if column:
            return f'{node_id} references column "{column.group(1)}", not found in its input.'
    return f"{node_id} failed: {line}"


def _reads_a_source(node: NodeResult) -> bool:
    return any(dep.startswith("source.") for dep in node.depends_on)