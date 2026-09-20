"""Parse dbt's target/ directory into NodeResults. No LLM, no warehouse.

    manifest.json     topology: every node, its dependencies, paths, relations
    run_results.json  outcomes from `dbt build`
    sources.json      outcomes from `dbt source freshness` (optional)

The output is exactly what localise() consumes.
"""
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import get_args

from pipeline_oncall.models import NodeResult, ResourceType

TRACKED_TYPES = frozenset(get_args(ResourceType))

# Assigned when a manifest node has no result in this run. Sources never appear
# in run_results.json, so marking them "skipped" instead would put three
# sources into the cascade of every healthy build.
NOT_RUN = "not run"

# sources.json is written by a separate dbt command and is never cleaned up.
# One much older than run_results.json describes an earlier state of the
# warehouse, not this run. This is a backstop, not a guarantee: see the
# runner, which deletes stale artifacts before each run.

DEFAULT_FRESHNESS_LAG = timedelta(minutes=10)

_QUOTED_IDENT = re.compile(r'"([^"]*)"')

@dataclass(frozen=True)
class ArtifactMeta:
    path: str
    invocation_id: str
    generated_at: datetime
    dbt_version: str

@dataclass(frozen=True)
class DbtArtifacts:
    nodes: tuple[NodeResult, ...]
    manifest: ArtifactMeta
    run_results: ArtifactMeta
    sources: ArtifactMeta | None    # the sources.json actually used
    ignored_sources: ArtifactMeta | None
    orphaned_results: tuple[str, ...] # results with no matching manifest node

    @property
    def freshness_checked(self) -> bool:
        return self.sources is not None
    @property
    def manifest_matches_build(self) -> bool:
        """False when manifest.json was rewritten by a later command, for
               example `dbt source freshness` run after `dbt build`."""
        return self.manifest.invocation_id == self.run_results.invocation_id

    def node(self, unique_id: str) -> NodeResult | None:
        return next((n for n in self.nodes if n.unique_id == unique_id), None)


def load_artifacts(
    target_dir: Path,
        max_freshness_lag: timedelta = DEFAULT_FRESHNESS_LAG,
) -> DbtArtifacts:
    """
    Read a dbt target/directory into NodeResults.
    Raises FileNotFoundError if manifest.json or run_results.json is missing,
    and ValueError if a file is not the dbt artifact its name claims.
    """

    manifest_path = target_dir/"manifest.json"
    results_path = target_dir/"run_results.json"
    sources_path = target_dir/"sources.json"

    manifest_data = _read(manifest_path, "manifest")
    results_data = _read(results_path,"run-results")

    manifest = _meta(manifest_path, manifest_data)
    run_results = _meta(results_path,results_data)

    outcomes = {r["unique_id"]: r for r in results_data["results"]}

    sources = ignored = None
    if sources_path.exists():
        sources_data = _read(sources_path, "sources")
        candidate = _meta(sources_path, sources_data)
        if run_results.generated_at - candidate.generated_at > max_freshness_lag:
            ignored = candidate
        else:
            sources = candidate
            outcomes.update({r["unique_id"]: r for r in sources_data["results"]})

    # Sources live in their own top-level dict, separate from every other node.
    definitions = {**manifest_data["nodes"], **manifest_data["sources"]}

    nodes = tuple(
        _to_node_result(definition, outcomes.get(uid))
        for uid, definition in definitions.items()
        if definition["resource_type"] in TRACKED_TYPES
    )

    return DbtArtifacts(
        nodes = nodes,
        manifest=manifest,
        run_results=run_results,
        sources=sources,
        ignored_sources=ignored,
        orphaned_results=tuple(sorted(set(outcomes) - set(definitions)))
    )

def _read(path: Path, kind: str) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"dbt artifact missing: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    schema = data.get("metadata", {}).get("dbt_schema_version", "")
    if f"/dbt/{kind}/" not in schema:
        raise ValueError(f"{path} is not a dbt {kind} artifact (schema {schema!r})")
    return data

def _meta (path: Path, data: dict) -> ArtifactMeta:
    metadata = data["metadata"]
    return ArtifactMeta(
        path = str(path),
        invocation_id=metadata["invocation_id"],
        generated_at=datetime.fromisoformat(metadata["generated_at"]),
        dbt_version=metadata["dbt_version"],
    )

def _to_node_result(definition: dict, outcome: dict | None) -> NodeResult:
    outcome = outcome or {}
    return NodeResult(
        unique_id=definition["unique_id"],
        resource_type=definition["resource_type"],
        status=outcome.get("status", NOT_RUN),
        message=_message(outcome),
        failures=outcome.get("failures"),
        execution_time=outcome.get("execution_time") or 0.0,
        relation=_unquote_relation(definition.get("relation_name")),
        file_path=_posix(definition.get("original_file_path")),
        depends_on=list(definition.get("depends_on", {}).get("nodes", [])),
        attached_node=definition.get("attached_node"),
    )

def _message(outcome: dict) -> str | None:
    """run_results.json carries a message; sources.json does not. Build one
        from the facts it does carry, so a freshness failure is not unexplained."""

    if outcome.get("message"):
        return outcome["message"]
    age = outcome.get("max_loaded_at_time_ago_in_s")
    if age is not None:
        return f"max_loaded_at {outcome.get('max_loaded_at')} ({age / 3600:.1f}h ago)"
    return outcome.get("error")

def _unquote_relation(relation_name: str | None) -> str | None:
    """'"oncall"."bronze"."stg_x"' -> 'bronze.stg_x'.

    The database name is dropped: the warehouse tools address relations as
    schema.table, and the database is fixed per adapter.
    """
    if not relation_name:
        return None
    parts = _QUOTED_IDENT.findall(relation_name) or relation_name.split(".")
    return ".".join(parts[-2:])


def _posix(path: str | None) -> str | None:
    """manifest.json records Windows paths with backslashes. Normalise, so an
    artifact written on Windows reads the same in Linux CI."""
    return path.replace("\\", "/") if path else None
