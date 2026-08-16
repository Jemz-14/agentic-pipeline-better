"""Reversible fault injections for the local substrate.

Each Scenario carries its own expected diagnosis -- root cause and culprit
node -- so the Phase 4 eval harness scores against this registry rather than
against a separate list of expectations that can drift out of sync with it.

Injections mutate the DuckDB `raw` schema wherever possible, which makes
revert nothing more than load_raw() and keeps `git status` clean across an
eval run. bad_join_grain is the exception: a transformation logic bug lives
in a model file by definition, so the CLI backs up everything a scenario
lists in `files_touched` before calling its injector.
"""

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
import duckdb

from pipeline_oncall.models import RootCause
from pipeline_oncall.substrate import REPO_ROOT

SUBSTRATE_DIR = REPO_ROOT / "substrate"

@dataclass(frozen=True)
class Scenario:
    """A reversible fault plus the diagnosis the agent is expected to reach"""

    name: str
    description: str
    expected_root_cause: RootCause | None # None means no incident at all
    expected_culprit: str | None
    inject: Callable[[Path], None]
    files_touched: tuple[str, ...] = ()

# --------------------------------------------------------------------------
# File helpers. newline="" on both read and write disables newline translation
# in either direction, so a backup/restore round trip is byte-identical and
# does not rewrite CRLF to LF.
# --------------------------------------------------------------------------

def read_file(path: Path) -> str:
    with path.open("r", encoding="utf-8", newline="") as f:
        return f.read()


def write_file(path: Path, text: str) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        f.write(text)

def _sql(db_path: Path, statement: str) -> None:
    with duckdb.connect(str(db_path)) as con:
        con.execute("set TimeZone='UTC'")
        con.execute(statement)


# Injectors
#-------------------
def _no_fault(db_path: Path) -> None:
    return

def _stale_source(db_path: Path) -> None:
    """One feed stops publishing. Trips error_after (6h) on that source only."""
    _sql(db_path,"""
    update raw.regional_intensity
    set _loaded_at = _loaded_at - interval 8 hour
    """)

"""Genuine schema drift: the column is gone, not merely null."""
def _drop_upstream_column (db_path: Path) -> None:
    _sql(db_path, "alter table raw.regional_intensity drop column intensity_index")


def _null_flood(db_path: Path) -> None:
    """ Nulls 30% of forecasts"""
    _sql(db_path, """
    update raw.regional_intensity
    set intensity_forecast = null
     where (epoch(period_start)::bigint / 1800 + regionid) % 10 < 3
    """)


"""
Duplicates first 12 hours.
The cutoff derives from min(period_start) rather than a hardcoded date,
so the scenario survives re-freezing the snapshot with a different window.
"""
def _duplicate_keys(db_path: Path) -> None:
    _sql(db_path, """
     insert into raw.regional_intensity
     select * from raw.regional_intensity
     where period_start < (
        select min(period_start) + interval 12 hour from raw.regional_intensity
     )
     
     """)

"""
Drop a fuel from mix
This is a row loss not a column loss, which is why its SOURCE DATA QUALITY rather than schema drift
"""
def _missing_fields(db_path: Path) -> None:
    _sql(db_path, """delete from raw.regional_intensity where fuel = 'coal' """)

GRAIN_MODEL = "models/gold/agg_national_daily.sql"
GRAIN_ANCHOR = "{{ ref('int_region_halfhourly') }}"
GRAIN_REPLACEMENT = "{{ ref('stg_regional_intensity') }}"


"""
Source regional CTE from unfiltered staging.
Someone may read from staging directly and skip intermediate sivler table, not realising
that int_region_halfhourly exists to filter out regions 15-18. Region count then goes from
14 -> 18 and every regional average shifts.
"""

def _bad_join_grain(db_path: Path) -> None:
    path = SUBSTRATE_DIR / GRAIN_MODEL
    original = read_file(path)

    found = original.cout(GRAIN_ANCHOR)
    if found != 1:
        raise ValueError(
            f"expected exactly 1 occurrence of {GRAIN_ANCHOR} in {path} but found {found}"
        )

    write_file(path original.replace(GRAIN_ANCHOR,GRAIN_REPLACEMENT))


# Registry
#-------------------
SCENARIOS: dict[str, Scenario] = {
    s.name: s
    for s in (
        Scenario(
            name="no_fault",
            description="Healthy build. The agent must not invent a failure.",
            expected_root_cause=None,
            expected_culprit=None,
            inject=_no_fault,
        ),
        Scenario(
            name="stale_source",
            description="regional_intensity stops publishing; _loaded_at rewound 8h.",
            expected_root_cause=RootCause.SOURCE_FRESHNESS_STALE,
            expected_culprit="source.oncall.carbon.regional_intensity",
            inject=_stale_source,
        ),
        Scenario(
            name="drop_upstream_column",
            description="intensity_index dropped from raw.regional_intensity.",
            expected_root_cause=RootCause.UPSTREAM_SCHEMA_DRIFT,
            expected_culprit="model.oncall.stg_regional_intensity",
            inject=_drop_upstream_column,
        ),
        Scenario(
            name="null_flood",
            description="~30% of intensity_forecast nulled across all regions.",
            expected_root_cause=RootCause.SOURCE_DATA_QUALITY,
            expected_culprit="model.oncall.stg_regional_intensity",
            inject=_null_flood,
        ),
        Scenario(
            name="duplicate_keys",
            description="First 12 hours of regional_intensity re-delivered.",
            expected_root_cause=RootCause.SOURCE_DATA_QUALITY,
            expected_culprit="model.oncall.stg_regional_intensity",
            inject=_duplicate_keys,
        ),
        Scenario(
            name="missing_fuel",
            description="All coal rows deleted from raw.regional_genmix.",
            expected_root_cause=RootCause.SOURCE_DATA_QUALITY,
            expected_culprit="model.oncall.int_genmix_wide",
            inject=_missing_fuel,
        ),
        Scenario(
            name="bad_join_grain",
            description="agg_national_daily reads unfiltered staging; aggregate regions leak in.",
            expected_root_cause=RootCause.TRANSFORMATION_LOGIC_BUG,
            expected_culprit="model.oncall.agg_national_daily",
            inject=_bad_join_grain,
            files_touched=(GRAIN_MODEL,),
        ),
    )
}


def get(name: str) -> Scenario:
    try:
        return SCENARIOS[name]
    except KeyError:
        raise KeyError(
            f"unknown scenario {name!r}; available: {', '.join(sorted(SCENARIOS))}"
        ) from None

def names() -> list[str]:
    return sorted(SCENARIOS)
