"""Load the frozen CSV fixture into DuckDB as the `raw` schema.

Unlike scripts/ingest.py, this is on the runtime path: every dbt build and
every eval scenario depends on it, so it lives in the package and is tested.
"""

from datetime import datetime, timezone
from pathlib import Path

import duckdb

# Resolved from this file, not the cwd: src/pipeline_oncall/ -> src/ -> repo root
REPO_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_DB = REPO_ROOT / "substrate" / "oncall.duckdb"
DEFAULT_DATA = REPO_ROOT / "substrate" / "data"

RAW_SCHEMA = "raw"
RAW_TABLES = ("regional_intensity", "regional_genmix", "national_intensity")


def load_raw(
    db_path: Path | None = None,
    data_dir: Path | None = None,
    loaded_at: datetime | None = None,
) -> dict[str, int]:
    """Create raw.<table> from each fixture CSV, stamped with _loaded_at.

    Idempotent: re-running replaces the tables in place.

    Args:
        db_path: DuckDB file to write. Defaults to substrate/oncall.duckdb.
        data_dir: Directory holding the frozen CSVs.
        loaded_at: Value for the _loaded_at column. Defaults to now (UTC).
            Pass an older timestamp to simulate a stale source feed.

    Returns:
        {table_name: row_count} for each loaded table.

    Raises:
        FileNotFoundError: if any fixture CSV is missing. Nothing is written.
    """
    db_path = db_path or DEFAULT_DB
    data_dir = data_dir or DEFAULT_DATA
    loaded_at = loaded_at or datetime.now(timezone.utc)

    # Resolve and check every CSV before touching the database, so a missing
    # fixture can't leave a half-loaded raw schema behind.
    csv_paths: dict[str, Path] = {}
    for table in RAW_TABLES:
        path = data_dir / f"{table}.csv"
        if not path.exists():
            raise FileNotFoundError(f"fixture CSV missing: {path}")
        csv_paths[table] = path

    db_path.parent.mkdir(parents=True, exist_ok=True)

    counts: dict[str, int] = {}
    with duckdb.connect(str(db_path)) as con:
        con.execute("set TimeZone='UTC'")
        con.execute(f"create schema if not exists {RAW_SCHEMA}")

        for table, csv_path in csv_paths.items():
            con.execute(
                f"create or replace table {RAW_SCHEMA}.{table} as "
                "select *, ?::timestamptz as _loaded_at from read_csv_auto(?)",
                [loaded_at, str(csv_path)],
            )
            counts[table] = con.execute(
                f"select count(*) from {RAW_SCHEMA}.{table}"
            ).fetchone()[0]

    return counts