"""Render an IncidentReport as markdown. Pure: report in, string out.

Used for the local file sink now and as the GitHub issue body in Phase 6, so
it sticks to plain markdown that renders the same anywhere.
"""

from collections.abc import Iterable

from pipeline_oncall.models import CostLedger, Evidence, IncidentReport


def render_markdown(report: IncidentReport, evidence: Iterable[Evidence]) -> str:
    sections = [
        _header(report),
        report.summary,
        _findings(report),
        _blast_radius(report),
        _suggested_fix(report),
        _hypotheses(report),
        _evidence_table(list(evidence)),
        _cost(report.cost),
    ]
    # Optional sections return "" and are dropped, rather than rendering as
    # empty headings.
    return "\n\n".join(s for s in sections if s) + "\n"


def _header(report: IncidentReport) -> str:
    culprit = f"`{report.culprit_node}`" if report.culprit_node else "_not localised_"
    return "\n".join([
        f"# Incident `{report.incident_id}`",
        "",
        # RootCause is a StrEnum, so this renders the value. As (str, Enum) it
        # rendered "RootCause.SOURCE_DATA_QUALITY" -- the reason it was changed.
        f"**Root cause:** `{report.root_cause}` · "
        f"**Confidence:** {report.confidence} · **Status:** {report.status}",
        "",
        f"**Culprit:** {culprit}",
        "",
        f"**Generated:** {report.generated_at.isoformat(timespec='seconds')}",
    ])


def _findings(report: IncidentReport) -> str:
    if not report.findings:
        return "## Findings\n\n_No findings._"
    lines = ["## Findings", ""]
    for i, finding in enumerate(report.findings, start=1):
        node = f" — `{finding.node}`" if finding.node else ""
        cites = ", ".join(finding.evidence_ids)
        lines.append(f"{i}. {finding.summary}{node} [{cites}]")
    return "\n".join(lines)


def _blast_radius(report: IncidentReport) -> str:
    # Rendered even when empty: "nothing downstream is affected" is itself a
    # finding, and a missing section would read as "not checked".
    if not report.blast_radius:
        return "## Blast radius\n\nNothing downstream depends on the culprit."
    return "\n".join(["## Blast radius", ""] + [f"- `{n}`" for n in report.blast_radius])


def _suggested_fix(report: IncidentReport) -> str:
    parts = []
    if report.suggested_fix:
        parts += ["## Suggested fix", "", report.suggested_fix]
    if report.suggested_patch:
        # Four backticks, so a patch that itself contains ``` cannot close the block.
        parts += ["", "````diff", report.suggested_patch.rstrip("\n"), "````"]
    return "\n".join(parts)


def _hypotheses(report: IncidentReport) -> str:
    if not report.unverified_hypotheses:
        return ""
    # The one place an uncited claim may appear, so it is labelled as such
    # and kept visibly apart from the findings.
    return "\n".join(
        ["## Unverified hypotheses", "", "_Not backed by collected evidence._", ""]
        + [f"- {h}" for h in report.unverified_hypotheses]
    )


def _evidence_table(evidence: list[Evidence]) -> str:
    if not evidence:
        return "## Evidence\n\n_No evidence recorded._"
    rows = ["## Evidence", "", "| ID | Tool | Summary | Payload |", "|---|---|---|---|"]
    rows += [
        f"| {e.id} | {_cell(e.tool)} | {_cell(e.summary)} | `{_cell(e.raw_ref)}` |"
        for e in evidence
    ]
    return "\n".join(rows)


def _cost(cost: CostLedger) -> str:
    return (
        "## Cost\n\n"
        f"{cost.input_tokens} input tokens · {cost.output_tokens} output tokens · "
        f"{cost.tool_calls} tool calls · {cost.warehouse_queries} warehouse queries · "
        f"{cost.wall_seconds:.1f}s · ${cost.estimated_usd:.4f}"
    )


def _cell(text: str) -> str:
    """Make text safe inside a markdown table cell.

    dbt error messages span several lines and can contain pipes. Either one
    would break the table, so pipes are escaped and newlines flattened.
    """
    return " ".join(text.replace("|", "\\|").split())