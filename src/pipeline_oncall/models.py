"""Pydantic contracts shared across the pipeline. Phase 1 fills this out."""

from enum import Enum


class RootCause(str, Enum):
    UPSTREAM_SCHEMA_DRIFT = "upstream_schema_drift"
    SOURCE_FRESHNESS_STALE = "source_freshness_stale"
    SOURCE_DATA_QUALITY = "source_data_quality"
    TRANSFORMATION_LOGIC_BUG = "transformation_logic_bug"
    DEPENDENCY_CASCADE = "dependency_cascade"
    INFRA_TRANSIENT = "infra_transient"
    CONFIG_OR_PERMISSIONS = "config_or_permissions"
    UNKNOWN = "unknown"