"""Command line interface for injecting and reverting substrate faults.

    oncall list
    oncall break --scenario null_flood
    oncall status
    oncall revert

break and revert deliberately do not run dbt. The eval runner has to invoke
dbt itself and capture the artifacts; folding the build in here would hide
the thing being measured.
"""

import json
from datetime import UTC, datetime
from pathlib import Path

import typer

from pipeline_oncall import scenarios
from pipeline_oncall.scenarios import SUBSTRATE_DIR, read_file, write_file
from pipeline_oncall.substrate import DEFAULT_DB, load_raw

app = typer.Typer(add_completion = False, help="Inject and revert substrate faults.")


"""State lives beside the database it describes.

   Deriving it from db_path means passing --db to a temp file isolates the
   state too, so tests never see or clobber the dev substrate's state.
   """
def state_path(db_path: Path) -> Path:
    return db_path.with_name(db_path.stem + ".state.json")


def _fail(message: str, code: int = 1) -> None:
    typer.secho(message, fg=typer.colors.RED, err=True)
    raise typer.Exit(code=code)

@app.command("list")
def list_scenarios() -> None:
    """List available scenarios and the diagnosis each expects."""
    for name in scenarios.names():
        spec = scenarios.get(name)
        cause = spec.expected_root_cause.value if spec.expected_root_cause else "(no incident)"

        typer.secho(name, fg=typer.colors.CYAN, bold=True)
        typer.echo(f"    {spec.description}")
        typer.echo(f"    expects:  {cause}")
        typer.echo(f"    culprit:  {spec.expected_culprit or '-'}")
        typer.echo()

    typer.echo(f"{len(scenarios.names())} scenarios")

@app.command('break')
def break_(
    scenario: str = typer.Option(..., "--scenario", "-s", help="Scenario name."),
    db: Path = typer.Option(DEFAULT_DB, "--db", help="DuckDB file to break."),
) -> None:
    """Reset the substrate to a clean baseline, then inject a fault."""
    try:
        spec = scenarios.get(scenario)
    except KeyError as exc:
        _fail(str(exc), code=2)

    state_file = state_path(db)
    if state_file.exists():
        active = json.loads(state_file.read_text(encoding="utf-8"))["scenario"]
        _fail(f"{active!r} is already active; run `oncall revert` first")

    load_raw(db_path = db)

    # Always reload first. Every scenario, including no_fault, must start from
    # an identical baseline -- otherwise _loaded_at simply ages out and a
    # healthy run trips the freshness thresholds for no reason.


    backups = {rel: read_file(SUBSTRATE_DIR / rel) for rel in spec.files_touched}

    # State is written BEFORE the injection runs. If an injector raises after
    # it has already mutated a file, the backup is on disk and revert still
    # works. Writing it afterwards would strand that file.
    state_file.write_text(
        json.dumps(
            {
                "scenario": spec.name,
                "injected_at": datetime.now(UTC).isoformat(),
                "db_path": str(db),
                "file_backups": backups,
            },
            indent=2,
        )
    + "\n",
    encoding="utf-8",
    )

    try:
        spec.inject(db)
    # Injectors run arbitrary SQL and file edits, so any exception type is possible.
    # Catching broadly is the point: state is already on disk and revert still works.
    except Exception as exc:  # noqa: BLE001
        typer.secho(f"injection failed: {exc}", fg=typer.colors.RED, err=True)
        _fail("state was recorded; run `oncall revert` to restore")

    typer.secho(f"injected {spec.name}", fg=typer.colors.YELLOW, bold=True)
    typer.echo(f"    {spec.description}")
    if spec.files_touched:
        typer.echo(f"    modified: {', '.join(spec.files_touched)}")

@app.command()
def revert(
    db: Path = typer.Option(DEFAULT_DB, "--db", help="DuckDB file to restore."),
) -> None:
    """Restore substrate to its clean baseline."""
    state_file = state_path(db)
    if not state_file.exists():
        typer.echo("nothing to revert; substrate is clean")
        return

    state = json.loads(state_file.read_text(encoding="utf-8"))

    # Files first, then data, then drop the state file last. If any step fails,
    # the state file survives and revert can simply be re-run.

    for rel, content in state.get("file_backups",{}).items():
        write_file(SUBSTRATE_DIR / rel, content)

    counts = load_raw(db_path = db)
    state_file.unlink()

    typer.secho(f"reverted {state['scenario']}", fg=typer.colors.GREEN, bold=True)
    typer.echo(f"    {counts}")

@app.command()
def status(
    db: Path = typer.Option(DEFAULT_DB, "--db", help="DuckDB file to inspect."),
) -> None:
    # Reports which scenario is currently injected (if any)

    state_file = state_path(db)
    if not state_file.exists():
        typer.secho("clean", fg=typer.colors.GREEN)
        return

    state = json.loads(state_file.read_text(encoding="utf-8"))
    typer.secho(f"broken: {state['scenario']}", fg=typer.colors.YELLOW, bold=True)
    typer.echo(f"    injected_at: {state['injected_at']}")
    typer.echo(f"    db:          {state['db_path']}")
    files = list(state.get("file_backups", {}))
    typer.echo(f"    files:       {', '.join(files) if files else '-'}")


if __name__ == "__main__":
    app()