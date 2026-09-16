"""
Integration tests: each scenario causes the failure its metadata claims
Slower (~15s) because each case shells out to dbt. Marked so CI can split
them from the fast suite.
"""

import json
import subprocess

import pytest
from typer.testing import CliRunner

from pipeline_oncall.cli import app, state_path
from pipeline_oncall.scenarios import SUBSTRATE_DIR
from pipeline_oncall.substrate import load_raw

pytestmark = pytest.mark.dbt

runner = CliRunner()
TEST_DB = SUBSTRATE_DIR / "oncall_test.duckdb"
TARGET_PATH = "target_test"

# Substring of the unique_id that must appear among the failures
EXPECTED_FAILURE = {
    "drop_upstream_column": "model.oncall.stg_regional_intensity",
    "null_flood": "not_null_stg_regional_intensity_intensity_forecast",
    "duplicate_keys": "unique_combination_of_columns_stg_regional_intensity",
    "missing_fuel": "accepted_range_int_genmix_wide_fuel_count",
    "bad_join_grain": "accepted_range_agg_national_daily_region_count",
}

def dbt(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["dbt", *args, "--target", "test", "--target-path", TARGET_PATH],
        cwd=SUBSTRATE_DIR,
        capture_output=True,
        text=True,
        check = False,
    )

def failed_ids() -> set[str]:
    results = json.loads((SUBSTRATE_DIR /TARGET_PATH/ "run_results.json").read_text())
    return {r["unique_id"] for r in results["results"] if r["status"] in {"fail", "error"}}


@pytest.fixture(autouse=True)
def clean_substrate():
    """Always revert, even on failure -- bad_join_grain mutates a tracked file."""
    load_raw(db_path=TEST_DB)
    yield
    if state_path(TEST_DB).exists():
        runner.invoke(app, ["revert", "--db", str(TEST_DB)])

def break_(name: str) -> None:
    result = runner.invoke(app, ["break", "-s", name, "--db", str(TEST_DB)])
    assert result.exit_code == 0, result.output

def test_no_fault_builds_clean():
    break_("no_fault")

    result = dbt("build")
    assert result.returncode == 0, (
        f"healthy substrate must not fail:\n{result.stdout[-2000:]}"
    )
    assert failed_ids() == set()

def test_stale_source_trips_freshness_but_not_build():
    break_("stale_source")

    assert dbt("build").returncode == 0

    result = dbt("source", "freshness")
    assert result.returncode != 0

    sources = json.loads((SUBSTRATE_DIR / TARGET_PATH / "sources.json").read_text())
    stale = {r["unique_id"] for r in sources["results"] if r["status"] == "error"}
    assert "source.oncall.carbon.regional_intensity" in stale

@pytest.mark.parametrize("name,expected", sorted(EXPECTED_FAILURE.items()))
def test_scenario_fails_the_expected_node(name, expected):
    break_(name)

    result = dbt("build")
    # Exit 1 = dbt ran and nodes failed, which is what an injection should
    # produce. Exit 2 = dbt could not run at all (bad profile, parse error),
    # which means the harness is broken, not the substrate.
    assert result.returncode == 1, (
        f"expected dbt failures (exit 1), got {result.returncode}:\n{result.stdout[-2000:]}"
    )

    assert any(expected in uid for uid in failed_ids()), (
        f"{name} expected a failure matching {expected!r}, got {sorted(failed_ids())}"
    )