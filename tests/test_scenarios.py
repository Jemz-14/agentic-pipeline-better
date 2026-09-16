"""Fast tests for the injection mechanism. No dbt involved"""

import json
from pathlib import Path

import duckdb
import pytest
from typer.testing import CliRunner

from pipeline_oncall import scenarios
from pipeline_oncall.cli import app, state_path
from pipeline_oncall.models import RootCause
from pipeline_oncall.scenarios import GRAIN_MODEL, SUBSTRATE_DIR, read_file, write_file
from pipeline_oncall.substrate import RAW_TABLES, load_raw

runner = CliRunner()

def structure(db: Path) -> dict[str, tuple[int, tuple[str, ...]]]:
    """Row count and column list per raw table. Excludes _loaded_at values,
    which legitimately change on every load."""
    out = {}
    with duckdb.connect(str(db), read_only=True) as con:
        for table in RAW_TABLES:
            count = con.execute(f"select count(*) from raw.{table}").fetchone()[0]
            cols = tuple(r[0] for r in con.execute(f"describe raw.{table}").fetchall())
            out[table] = (count, cols)
    return out

def max_age_seconds(db: Path) -> float:
    with duckdb.connect(str(db), read_only=True) as con:
        con.execute("set TimeZone='UTC'")
        return con.execute(
            "select max(epoch(current_timestamp) - epoch(_loaded_at)) "
            "from raw.regional_intensity"
        ).fetchone()[0]

@pytest.fixture
def db(tmp_path: Path) -> Path:
    """Temp database. state_path() derives from it, so state is isolated too."""
    path = tmp_path / "oncall.duckdb"
    load_raw(db_path=path)
    return path

@pytest.fixture(autouse=True)
def restore_grain_model():
    """bad_join_grain mutates a tracked file; never leave it modified."""
    original = read_file(SUBSTRATE_DIR / GRAIN_MODEL)
    yield
    write_file(SUBSTRATE_DIR / GRAIN_MODEL, original)

def invoke(*args: str):
    result = runner.invoke(app,list(args))
    return result



# Registry Integrity

def test_registry_is_non_empty_and_ordered():
    assert scenarios.names()[0] == "no_fault", "baseline should come first"
    assert len(scenarios.names()) == len(set(scenarios.names()))


@pytest.mark.parametrize("name",scenarios.names())
def test_metadata_is_complete(name):
    spec = scenarios.get(name)
    assert spec.description.strip()

    if name == "no_fault":
        assert spec.expected_root_cause is None
        assert spec.expected_culprit is None
    else:
        assert isinstance(spec.expected_root_cause, RootCause)
        assert spec.expected_culprit is not None
        # Must be a real dbt unique_id shape, not a bare model name -- the
        # eval scores culprit accuracy by string match against the manifest.
        assert spec.expected_culprit.startswith(("model.oncall.", "source.oncall."))

def test_unknown_scenario_name_are_listed_in_the_error():
    with pytest.raises(KeyError, match="missing_fuel"):
        scenarios.get("nope")


# Round trip

@pytest.mark.parametrize("name", scenarios.names())
def test_break_then_revert_restores_baseline(db, name):
    baseline = structure(db)
    original_model = read_file(SUBSTRATE_DIR / GRAIN_MODEL)

    assert invoke("break", "-s", name, "--db", str(db)).exit_code == 0
    assert state_path(db).exists()

    assert invoke("revert", "--db",str(db)).exit_code == 0
    assert not state_path(db).exists()

    assert structure(db) == baseline
    assert max_age_seconds(db) < 60
    # Byte-identical, not merely equivalent: the newline="" helpers exist
    # precisely so a restore cannot rewrite CRLF to LF.
    assert read_file(SUBSTRATE_DIR / GRAIN_MODEL) == original_model

def test_break_refuses_when_a_scenario_is_active(db):
    assert invoke("break", "-s", "null_flood", "--db", str(db)).exit_code == 0

    result = invoke("break", "-s", "missing_fuel", "--db", str(db))
    assert result.exit_code == 1
    assert "already active" in result.output + str(result.stderr or "")

    state = json.loads(state_path(db).read_text(encoding="utf-8"))
    assert state["scenario"] == "null_flood"

    invoke("revert", "--db", str(db))

def test_break_rejects_unknown_scenario(db):
    assert invoke("break", "-s", "does_not_exist", "--db", str(db)).exit_code == 2
    assert not state_path(db).exists()

def test_revert_is_a_noop_when_clean(db):
    result = invoke("revert", "--db", str(db))
    assert result.exit_code == 0
    assert "clean" in result.output


# Determinism -- the property the eval numbers depend on

def _nulled_keys(db: Path) -> set[tuple]:
    with duckdb.connect(str(db), read_only=True) as con:
        return set(
            con.execute(
                "select period_start, regionid from raw.regional_intensity "
                "where intensity_forecast is null"
            ).fetchall()
        )

def test_null_flood_is_deterministic(db):
    invoke("break", "-s", "null_flood", "--db", str(db))
    first = _nulled_keys(db)
    invoke("revert", "--db", str(db))

    invoke("break", "-s", "null_flood", "--db", str(db))
    second = _nulled_keys(db)
    invoke("revert", "--db", str(db))

    assert first == second
    assert 0.25 < len(first) / 12096 < 0.35, "should null roughly 30%"

def test_bad_join_grain_raises_if_its_anchor_is_gone(db, monkeypatch):
    """A silent no-op would inject nothing, pass the build, and score as an
     untraceable false negative."""
    monkeypatch.setattr(scenarios, "GRAIN_ANCHOR", "{{ ref('nonexistent') }}")
    with pytest.raises(ValueError, match="expected exactly 1 occurrence"):
        scenarios.get("bad_join_grain").inject(db)

