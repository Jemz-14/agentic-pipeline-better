"""Tests for the raw fixture loader.

The loader is the base of the whole substrate: if it silently loads the wrong
rows, every downstream eval label is wrong and nothing else will tell you.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest

from pipeline_oncall.substrate import DEFAULT_DATA, RAW_TABLES, load_raw

# Expected counts come from the fixture's own provenance file rather than
# being hardcoded, so re-freezing the snapshot with a different window
# updates the assertions instead of breaking them.
SNAPSHOT = json.loads((DEFAULT_DATA / "_snapshot.json").read_text(encoding="utf-8"))
EXPECTED_COUNTS = SNAPSHOT["row_counts"]


def query(db: Path, sql: str) -> list[tuple]:
    with duckdb.connect(str(db), read_only=True) as con:
        return con.execute(sql).fetchall()


@pytest.fixture(scope="module")
def loaded_db(tmp_path_factory) -> Path:
    """One default load, shared by the read-only assertions.

    Module-scoped because loading 108k rows per test would dominate runtime.
    Tests that need different arguments do their own load into tmp_path.
    """
    db = tmp_path_factory.mktemp("substrate") / "oncall.duckdb"
    load_raw(db_path=db)
    return db


@pytest.fixture
def partial_data_dir(tmp_path) -> Path:
    """A data dir holding only two of the three required fixtures."""
    d = tmp_path / "data"
    d.mkdir()
    for table in RAW_TABLES[:2]:
        (d / f"{table}.csv").write_text(
            "period_start,regionid\n2026-07-17T00:00:00Z,1\n", encoding="utf-8"
        )
    return d


def test_returns_expected_row_counts(tmp_path):
    counts = load_raw(db_path=tmp_path / "oncall.duckdb")
    assert counts == EXPECTED_COUNTS


def test_fixture_is_rectangular(loaded_db):
    """18 regions in every period, 9 fuels for every region-period.

    Window-independent, so it survives a re-freeze. Guards the property that
    makes injected faults unambiguous.
    """
    (min_regions, max_regions), = query(
        loaded_db,
        "select min(n), max(n) from "
        "(select period_start, count(*) n from raw.regional_intensity group by 1)",
    )
    assert min_regions == max_regions == 18

    (min_fuels, max_fuels), = query(
        loaded_db,
        "select min(n), max(n) from "
        "(select period_start, regionid, count(*) n from raw.regional_genmix group by 1, 2)",
    )
    assert min_fuels == max_fuels == 9

    assert EXPECTED_COUNTS["regional_genmix"] == EXPECTED_COUNTS["regional_intensity"] * 9


def test_appends_loaded_at_preserving_csv_columns(loaded_db):
    """_loaded_at is appended; the CSV contract is otherwise untouched."""
    for table in RAW_TABLES:
        cols = [row[0] for row in query(loaded_db, f"describe raw.{table}")]
        header = (
            (DEFAULT_DATA / f"{table}.csv")
            .read_text(encoding="utf-8")
            .splitlines()[0]
            .split(",")
        )
        assert cols[-1] == "_loaded_at"
        assert cols[:-1] == header


def test_loaded_at_defaults_to_now_and_is_tz_aware(loaded_db):
    """A naive timestamp here would silently corrupt dbt freshness maths."""
    (stamp,), = query(loaded_db, "select max(_loaded_at) from raw.regional_intensity")
    assert stamp.tzinfo is not None
    assert abs((datetime.now(timezone.utc) - stamp).total_seconds()) < 300


def test_stamps_explicit_loaded_at(tmp_path):
    """The seam the stale_source fault injection rides on."""
    stamp = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    db = tmp_path / "oncall.duckdb"
    load_raw(db_path=db, loaded_at=stamp)

    for table in RAW_TABLES:
        (lo, hi), = query(db, f"select min(_loaded_at), max(_loaded_at) from raw.{table}")
        assert lo == hi == stamp


def test_is_idempotent(tmp_path):
    """create-or-replace, not append: a second load must not double the rows."""
    db = tmp_path / "oncall.duckdb"
    first = load_raw(db_path=db)
    second = load_raw(db_path=db)
    assert first == second == EXPECTED_COUNTS


def test_missing_csv_raises(tmp_path, partial_data_dir):
    with pytest.raises(FileNotFoundError, match=RAW_TABLES[2]):
        load_raw(db_path=tmp_path / "oncall.duckdb", data_dir=partial_data_dir)


def test_missing_csv_writes_nothing(tmp_path, partial_data_dir):
    """Fail-fast: a partial load would look exactly like a real incident."""
    db = tmp_path / "oncall.duckdb"
    with pytest.raises(FileNotFoundError):
        load_raw(db_path=db, data_dir=partial_data_dir)
    assert not db.exists()