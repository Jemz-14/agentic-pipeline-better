"""
Integration tests: each scenario causes the failure its metadata claims
Slower (~15s) because each case shells out to dbt. Marked so CI can split
them from the fast suite.
"""

import json
import subprocess

import pytest
from typer.testing import CliRunner

from pipeline_oncall import scenarios
from pipeline_oncall.baseline import classify
from pipeline_oncall.cli import app
from pipeline_oncall.collectors.dbt_artifacts import load_artifacts
from pipeline_oncall.localise import localise
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
    """Start and end every test with no scenario active.

    Reverting at setup as well as teardown recovers from an interrupted
    previous run: Ctrl+C mid-build skips teardown and leaves a scenario
    injected, which the next run would otherwise trip over.
    """
    runner.invoke(app, ["rever", "--db", str(TEST_DB)])
    load_raw(db_path=TEST_DB)

    for stale in ("run_results.json", "sources.json"):
        (SUBSTRATE_DIR / TARGET_PATH / stale).unlink(missing_ok=True)

    yield

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

def triage():
    """Run dbt the way the eval runner will, then collect the artifacts.

    Freshness first, then build. Both commands rewrite manifest.json, so this
    order leaves the manifest carrying the build's invocation id. It also means
    sources.json is seconds older than run_results.json rather than left over
    from an earlier scenario.
    """

    dbt("source","freshness")
    dbt("build")
    return load_artifacts(SUBSTRATE_DIR / TARGET_PATH)

@pytest.mark.parametrize("name", scenarios.names())
def test_scenario_localises_to_its_expected_culprit(name):
    """The deterministic core, end to end. Here we inject a fault, run dbt, parse the
    artifacts, and check the localiser names the node the registry predicted.

    No LLM anywhere in this path.
    """
    spec = scenarios.get(name)
    break_(name)

    artifacts = triage()
    result = localise(artifacts.nodes)

    assert artifacts.freshness_checked, "sources.json was rejected as stale"
    assert artifacts.manifest_matches_build, "manifest is from a different invocation"
    assert artifacts.orphaned_results == ()

    expected = () if spec.expected_culprit is None else (spec.expected_culprit,)
    assert result.culprits == expected, (
        f"{name}: expected {expected}, got {result.culprits}; "
        f"(failing tests: {sorted(result.failing_tests)}"
    )

def test_no_fault_produces_no_cascade():
    """ False positive check -> healthy build must localise to nothing at all, no
    culprits / no affected nodes"""
    break_("no_fault")
    result = localise(triage().nodes)
    assert result.culprits == ()
    assert result.cascade == ()


# Scenarios the baseline is expected to get wrong, and why. Each is marked
# strict, so if the baseline ever starts getting one right the suite fails --
# that means someone tuned the rules to a scenario, and the agent comparison
# is no longer honest.
BASELINE_KNOWN_MISSES = {
    "missing_fuel": (
        "the failing test sits two hops from the source; from dbt artifacts "
        "alone, missing source rows and a logic bug look identical"
    ),
}

@pytest.mark.parametrize(
    "name",
    [
        pytest.param(n, marks=pytest.mark.xfail(strict=True, reason=BASELINE_KNOWN_MISSES[n]))
        if n in BASELINE_KNOWN_MISSES
        else n
        for n in scenarios.names()
    ],
)
def tests_baseline_classifies_scenario(name):
    spec = scenarios.get(name)
    break_(name)

    nodes = triage().nodes
    result = classify(nodes,localise(nodes))

    assert result.root_cause == spec.expected_root_cause, (
        f"{name}: baseline said {result.root_cause} via {result.rule!r}; "
        f"expected {spec.expected_root_cause}"
    )
