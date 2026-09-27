"""Evidence log: every observation gets an ID before anything may cite it.

IDs are issued here, in order, and never by the caller or the model. A report
may only cite IDs this log issued, which is what makes a fabricated citation
detectable rather than merely discouraged (principle 2).

Each record's full payload is written to disk, so the evidence behind a report
can be re-read after the fact.
"""

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from pipeline_oncall.models import Evidence, IncidentReport


class EvidenceLog:
    def __init__(self, incident_dir: Path) -> None:
        self.incident_dir = incident_dir
        self._records: dict[str, Evidence] = {}

    def record(self, tool: str, args: dict, summary: str, payload: Any) -> Evidence:
        """Store one observation and return its Evidence, with an ID issued here."""
        evidence_id = f"ev_{len(self._records) + 1:03d}"

        self.incident_dir.mkdir(parents=True, exist_ok=True)
        payload_path = self.incident_dir / f"{evidence_id}.json"
        payload_path.write_text(
            json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8"
        )

        evidence = Evidence(
            id=evidence_id,
            tool=tool,
            args=args,
            summary=summary,
            # Relative to the artifacts root, so a report reads the same on any
            # machine rather than embedding a Windows absolute path.
            raw_ref=f"{self.incident_dir.name}/{payload_path.name}",
        )
        self._records[evidence_id] = evidence
        return evidence

    def get(self, evidence_id: str) -> Evidence:
        return self._records[evidence_id]

    def dangling(self, report: IncidentReport) -> set[str]:
        """ID's the report cites that this log never issued , empty is correct"""
        return report.cited_evidence_ids() - self._records.keys()

    def write_index(self) -> Path:
        """Write every Evidence record to evidence.json beside the payloads."""
        index = self.incident_dir / "evidence.json"
        index.write_text(
            json.dumps([e.model_dump(mode="json") for e in self], indent=2) + "\n",
            encoding="utf-8",
        )
        return index

    def __iter__(self) -> Iterator[Evidence]:
        return iter(self._records.values())

    def __len__(self) -> int:
        return len(self._records)

    def __contains__(self, evidence_id: object) -> bool:
        return evidence_id in self._records