# pipeline-oncall

An agent that triages data pipeline failures. It collects evidence from dbt
artifacts, orchestrator logs, the warehouse and git history, localises the
first-failing node, classifies the root cause, and emits a structured incident
report in which every claim cites an evidence ID.

**Status: Phase 0 (substrate) complete.** There is no agent yet and no measured
accuracy. This README will carry a metrics table once the eval harness in
Phase 4 produces one.

## Design principles

1. **The LLM does judgement, not computation.** Lineage traversal, topological
   sorting, row counting, schema diffing and freshness maths are deterministic
   Python, unit tested, and exposed to the agent as tools. The agent decides
   what to look at next and what the evidence means; it never computes lineage
   itself.
2. **Every claim cites an evidence ID.** A finding with no evidence fails
   validation. Anything unobserved is reported as `unknown`.
3. **Read-only by default.** No write path to the warehouse, no push access.
   The only mutating action is opening a draft PR, behind a feature flag.
4. **Bounded loops.** Hard caps on tool calls, wall clock and spend. Exceeding
   a cap returns a partial report with `status: budget_exhausted`.
5. **Measured, not claimed.** Nothing here asserts a number the eval harness
   does not produce.

---

## The substrate

A dbt-duckdb project under `substrate/`, built on **UK National Grid ESO
Carbon Intensity** data (`api.carbonintensity.org.uk` — public, no key).

`substrate/data/` holds a frozen 14-day snapshot, 2026-07-17 to 2026-07-31:
672 half-hourly periods × 18 regions, with a 9-fuel generation mix per
region-period. Provenance is recorded in `substrate/data/_snapshot.json`.
The snapshot is committed so every eval run is reproducible, and nothing on
the runtime path touches the network.

### Raw data is sources, not seeds

`scripts/ingest.py` (manual, hits the live API) freezes the CSVs.
`src/pipeline_oncall/substrate.py` loads them into DuckDB's `raw` schema with
a `_loaded_at` column, and dbt declares those tables as **sources**.

This is deliberate. `dbt source freshness` only applies to sources with a
`loaded_at_field`, and it is the command that emits `target/sources.json`.
Using seeds for the raw layer would mean hand-fabricating that artifact and
would make `SOURCE_FRESHNESS_STALE` unreachable. The 18-row region dimension
stays a seed, which is what seeds are actually for.

`load_raw()` takes `loaded_at` as a parameter, so simulating a stalled feed is
one argument rather than a separate code path.

### The region grain

Regions 1–14 are DNO regions (North Scotland, London, …). Regions **15–18 are
`England`, `Scotland`, `Wales` and `GB`** — aggregates of regions 1–14 sitting
at the same grain key in the same table. Any roll-up that does not filter to
`region_type = 'dno'` double-counts every half-hour.

That trap is native to the source rather than invented, and it is what the
`bad_join_grain` scenario exploits.

### Models

| Layer | Model | Rows | Notes |
|---|---|---|---|
| bronze | `stg_regions` | 18 | region dimension |
| bronze | `stg_regional_intensity` | 12,096 | rename only |
| bronze | `stg_regional_genmix` | 108,864 | one row per fuel |
| bronze | `stg_national_intensity` | 672 | forecast + settled actual |
| silver | `int_genmix_wide` | 12,096 | 9 fuels pivoted to columns |
| silver | `int_region_halfhourly` | 9,408 | DNO regions only |
| gold | `agg_region_daily` | 196 | 14 dates × 14 regions |
| gold | `agg_national_daily` | 14 | national vs regional mean |

46 dbt tests, including two singular tests.

Three columns exist purely as fault detectors: `fuel_count` (always 9),
`period_count` (always 48) and `region_count` (always 14). Each collapses a
class of faults into a single range test. `fuel_count` earns its place
specifically because UK coal is frequently 0% — deleting every coal row leaves
`sum(perc)` at 100, so a sum-based test sees nothing while `fuel_count` drops
to 8 immediately.

Fuel columns in `int_genmix_wide` are deliberately **not** coalesced. Wrapping
them in `coalesce(_, 0)` would turn a missing fuel into a silent zero and push
the symptom downstream, away from the node that caused it.

---

## Fault injection

Seven reversible scenarios. Each carries its own expected diagnosis — root
cause and culprit `unique_id` — so the eval harness scores against the same
object that performs the injection, rather than a separate list that can drift.

| Scenario | Injection | Expected |
|---|---|---|
| `no_fault` | nothing | no incident |
| `stale_source` | rewind `_loaded_at` 8h | `source_freshness_stale` |
| `drop_upstream_column` | `alter table … drop column` | `upstream_schema_drift` |
| `null_flood` | null ~30% of forecasts | `source_data_quality` |
| `duplicate_keys` | re-deliver the first 12 hours | `source_data_quality` |
| `missing_fuel` | delete all coal rows | `source_data_quality` |
| `bad_join_grain` | re-point a `ref()` past the DNO filter | `transformation_logic_bug` |

```bash
uv run oncall list
uv run oncall break --scenario missing_fuel
uv run oncall status
uv run oncall revert
```

`break` always reloads the substrate first, so every scenario — including
`no_fault` — starts from an identical baseline. Without that, `_loaded_at`
simply ages out and a healthy run trips the freshness thresholds for no reason.

Injections target the DuckDB `raw` schema wherever possible, so `revert` is
just `load_raw()` and `git status` stays clean across a run. `bad_join_grain`
is the exception, since a transformation bug lives in a model file by
definition; the CLI backs up and restores those files byte-for-byte.

`break` and `revert` never invoke dbt. The eval runner has to run dbt itself to
capture the artifacts, and folding the build in here would hide the thing being
measured.

**Note:** `dbt build` does not evaluate source freshness. `stale_source`
produces a completely green build and is only caught by `dbt source freshness`,
so both commands are needed.

---

## Quickstart

Requires Python 3.11 and [uv](https://docs.astral.sh/uv/).

```bash
uv sync
uv run python -c "from pipeline_oncall.substrate import load_raw; print(load_raw())"
cd substrate && uv run dbt deps && uv run dbt build && uv run dbt source freshness
```

Expected: `12096 / 108864 / 672` from the loader, then `PASS=55` from dbt and
three freshness passes.

Always run dbt from inside `substrate/` — `profiles.yml` resolves the database
path relative to the working directory.

To regenerate the frozen snapshot from the live API (rarely needed):

```bash
uv run python scripts/ingest.py --start 2026-07-17 --end 2026-07-31
```

## Tests

```bash
uv run pytest -m "not dbt"   # 29 tests, ~20s
uv run pytest -m dbt         # 7 tests, ~50s
uv run pytest                # 36 tests
```

The fast tests prove the injection mechanism: break/revert round-trips to a
byte-identical baseline, `break` refuses to inject on top of an active
scenario, and `null_flood` selects the same rows on every run.

The `dbt`-marked tests prove the ground truth: each scenario actually fails the
node its registry entry names. That layer is not optional — `missing_fuel`
once targeted the wrong table, injecting nothing, while every fast test still
passed and its recorded culprit was fiction.

## Layout

```
src/pipeline_oncall/   package (runtime path, tested)
  substrate.py           CSV -> DuckDB raw, stamps _loaded_at
  scenarios.py           fault registry with expected diagnoses
  cli.py                 oncall break / revert / status / list
  models.py              RootCause enum; Phase 1 fills in the rest
scripts/                 operator tooling, never imported
  ingest.py              live API -> frozen CSVs
substrate/               dbt-duckdb fixture project
  data/                  frozen snapshot (committed)
  models/                bronze / silver / gold
tests/                   pytest
```

## Roadmap

| Phase | | |
|---|---|---|
| 0 | Substrate, fault injection CLI | ✅ |
| 1 | Pydantic contracts, dbt artifact collector, lineage graph, localiser, rules baseline | |
| 2 | Tool layer with `run_sql` guards and evidence recording | |
| 3 | Bounded agent loop, triage playbook, structured output | |
| 4 | Eval harness, metrics, baseline comparison, CI gate | |
| 5 | Dagster assets and failure sensor | |
| 6 | Sinks: markdown, GitHub issue, draft PR | |
| 7 | Second warehouse adapter, writeup | |

## Data attribution

Carbon intensity data from the [Carbon Intensity API](https://carbonintensity.org.uk/),
developed by National Grid ESO in partnership with the University of Oxford,
the Environmental Defense Fund and WWF. Licensed CC BY 4.0.
