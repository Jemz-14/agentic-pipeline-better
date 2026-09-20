"""Pydantic contracts shared by every layer.

These are the interfaces between the collectors, the localiser, the baseline,
the agent and the sinks. Principle 2 -- every claim cites evidence -- is
enforced here in the type system rather than requested in a prompt.
"""

from datetime import UTC, datetime
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

# Every status dbt can emit, taken from RunStatus, TestStatus and
# FreshnessStatus. The spec's shorter list omits "pass", which is the most
# common value in a healthy run_results.json.

NodeStatus = Literal[
    "success", "error", "skipped", "partial success", "no-op", "reused",
    "pass", "fail", "warn",
    "runtime error",
    "not run",
]

ResourceType = Literal["model", "test", "source", "snapshot", "seed", "operation"]

FAILED_STATUSES: frozenset[str] = frozenset({"error", "fail", "runtime error"})
SKIPPED_STATUSES: frozenset[str] = frozenset({"skipped"})


class RootCause(StrEnum):
    UPSTREAM_SCHEMA_DRIFT = "upstream_schema_drift"
    SOURCE_FRESHNESS_STALE = "source_freshness_stale"
    SOURCE_DATA_QUALITY = "source_data_quality"
    TRANSFORMATION_LOGIC_BUG = "transformation_logic_bug"
    DEPENDENCY_CASCADE = "dependency_cascade"
    INFRA_TRANSIENT = "infra_transient"
    CONFIG_OR_PERMISSIONS = "config_or_permissions"
    UNKNOWN = "unknown"

class FailureEvent(BaseModel):
    """What the orchestrator tells us happened. The entry point to triage."""
    run_id: str
    orchestrator: Literal["dagster", "airflow", "dbt_cli"]
    failed_steps: list[str] = Field(default_factory=list)
    started_at: datetime
    finished_at: datetime

class NodeResult(BaseModel):
    """ One node's outcome, parsed from run_results.json or sources.json"""
    model_config = ConfigDict(extra="forbid")
    unique_id: str
    resource_type: ResourceType
    status: NodeStatus
    message: str | None = None
    failures: int | None = None
    execution_time: float = 0.0
    relation: str | None = None       # schema.table, unquoted
    file_path: str | None = None
    # From manifest.json rather than run_results.json. The collector merges
    # both files, so one type carries the outcome and the topology.
    depends_on: list[str] = Field(default_factory=list)
    attached_node: str | None = None  # tests only: the model under test

    @property
    def failed(self) -> bool:
        return self.status in FAILED_STATUSES

    @property
    def skipped(self) -> bool:
        return self.status in SKIPPED_STATUSES



class Evidence(BaseModel):
    """A single observation. IDs are assigned by the evidence log, never by
     the model, so a report cannot cite something that was never collected."""

    id: str = Field(pattern=r"^ev_\d{3,}$")
    tool: str
    args: dict = Field(default_factory=dict)
    summary: str
    raw_ref: str # path to full payload on disk

class Finding(BaseModel):
    """One claim about the incident.

     evidence_ids must be non-empty. This is principle 2: an uncited claim is
     not a finding, and the type system rejects it rather than a prompt asking
     nicely.
     """

    summary: str
    node: str | None = None
    evidence_ids: list[str] = Field(min_length=1)


class CostLedger(BaseModel):
    """Per-incident spend. Populated with zeros on the deterministic path --
     the fields exist so a rules-baseline report and an agent report are
     directly comparable."""

    input_tokens: int = 0
    output_tokens: int = 0
    tool_calls: int = 0
    warehouse_queries: int = 0
    wall_seconds: float = 0.0
    estimated_usd: float = 0.0

class IncidentReport(BaseModel):
    incident_id: str
    generated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    culprit_node: str | None = None
    root_cause: RootCause = RootCause.UNKNOWN
    confidence: Literal["high", "medium", "low"] = "low"

    findings: list[Finding] = Field(default_factory=list)
    blast_radius: list[str] = Field(default_factory=list)

    suggested_fix: str | None = None
    suggested_patch: str | None = None
    unverified_hypothesis: list[str] = Field(default_factory=list)

    status: Literal["complete", "budget_exhausted", "insufficient_evidence"]
    cost: CostLedger =Field(default_factory=CostLedger)

    @model_validator(mode="after")
    def _classification_requires_findings(self) -> "IncidentReport":
        """You may say "unknown" with nothing to show. You may not name a root
        cause with nothing to show."""
        if self.root_cause is not RootCause.UNKNOWN and not self.findings:
            raise ValueError(
                f"root_cause {self.root_cause.value!r} asserted with no findings"
            )
        return self

    def cited_evidence_ids(self) -> set[str]:
        """Every evidence ID the report references. Phase 3 checks these
        against the log to catch fabricated citations."""
        return {eid for finding in self.findings for eid in finding.evidence_ids}
