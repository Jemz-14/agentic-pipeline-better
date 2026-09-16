"""
Fetch UK Carbon intensity data and freeze it to CSV

Not part of the runtime path. Run manually to regenerate substrate/data/.
"""
import argparse
import csv
import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx

BASE = "https://api.carbonintensity.org.uk"
FUELS = ["biomass", "coal", "imports", "gas", "nuclear", "other", "hydro", "solar", "wind"]
FUEL_SET = frozenset(FUELS)

def chunks(start: datetime, end: datetime, days: int = 7) -> Iterator[tuple[datetime, datetime]]:

    current = start
    step = timedelta(days=days)

    while current < end:
        chunk_end = min(current + step, end)
        yield current, chunk_end
        current = chunk_end


def fetch(client: httpx.Client, path: str, start: datetime, end: datetime) -> list[dict]:
    start_str = start.strftime("%Y-%m-%dT%H:%MZ")
    end_str = end.strftime("%Y-%m-%dT%H:%MZ")
    url = f"{BASE}{path}/{start_str}/{end_str}"

    response = client.get(url)
    response.raise_for_status()

    return response.json()["data"]

def parse_ts(s: str) -> str:
    if s.endswith("Z") and len(s) == 17:
        return s.replace("Z", ":00Z")
    return s

def flatten_regional(periods: list[dict]) -> tuple[dict, dict]:
    intensity_rows = {}
    genmix_rows = {}

    for period in periods:
        period_start = parse_ts(period["from"])
        period_end = parse_ts(period["to"])

        for region in period["regions"]:
            region_id = region["regionid"]

            # --- intensity table ---
            intensity_key = (period_start, region_id)

            intensity_rows[intensity_key] = {
                "period_start": period_start,
                "period_end": period_end,
                "regionid": region_id,
                "intensity_forecast": region["intensity"]["forecast"],
                "intensity_index": region["intensity"]["index"],
            }

            # --- generation mix table ---
            fuels_seen = {f["fuel"] for f in region["generationmix"]}
            if fuels_seen != set(FUELS):
                raise ValueError(
                    f"unexpected fuel set at {period_start} region {region_id}: "
                    f"{sorted(fuels_seen)}"
                )

            for fuel in region["generationmix"]:
                genmix_key = (period_start, region_id, fuel["fuel"])

                genmix_rows[genmix_key] = {
                    "period_start": period_start,
                    "regionid": region_id,
                    "fuel": fuel["fuel"],
                    "perc": fuel["perc"],
                }

    return intensity_rows, genmix_rows

def flatten_national(periods: list[dict]) -> dict:

    rows = {}

    for period in periods:
        period_start = parse_ts(period["from"])
        intensity = period["intensity"]

        rows[period_start] = {
            "period_start": period_start,
            "period_end": parse_ts(period["to"]),
            "intensity_forecast": intensity["forecast"],
            "intensity_actual": intensity.get("actual"),
            "intensity_index": intensity["index"],
        }

    return rows

def write_csv(path: Path, rows: dict, fieldnames: list[str]) -> None:
    """Write rows sorted by key. newline='' + lineterminator='\\n' keeps the
    committed file byte-stable on Windows (otherwise you get \\r\\r\\n)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        for _key, row in sorted(rows.items()):
            writer.writerow(row)


def main(start: datetime, end: datetime, out: Path) -> None:
    regional_intensity: dict = {}
    regional_genmix: dict = {}
    national: dict = {}

    c_reg_int = len(regional_intensity)
    c_reg_gen = len(regional_genmix)
    c_nat = len(national)

    with httpx.Client(timeout=60.0) as client:
        for c_start, c_end in chunks(start, end):
            print(f"fetching {c_start:%Y-%m-%d} -> {c_end:%Y-%m-%d}")
            ri, gm = flatten_regional(fetch(client, "/regional/intensity", c_start, c_end))
            regional_intensity.update(ri)
            regional_genmix.update(gm)
            national.update(flatten_national(fetch(client, "/intensity", c_start, c_end)))

    # The API returns the period ENDING at `from`, i.e. one extra period before
    # the window. Clamp both ends so the window is half-open: [start, end).
    floor = start.strftime("%Y-%m-%dT%H:%M:%SZ")
    cutoff = end.strftime("%Y-%m-%dT%H:%M:%SZ")

    def keep(rows: dict) -> dict:
        return {k: v for k, v in rows.items() if floor <= v["period_start"] < cutoff}

    regional_intensity = keep(regional_intensity)
    regional_genmix = keep(regional_genmix)
    national = keep(national)

    write_csv(out / "regional_intensity.csv", regional_intensity,
              ["period_start", "period_end", "regionid", "intensity_forecast", "intensity_index"])
    write_csv(out / "regional_genmix.csv", regional_genmix,
              ["period_start", "regionid", "fuel", "perc"])
    write_csv(
        out / "national_intensity.csv",
        national,
        ["period_start", "period_end", "intensity_forecast", "intensity_actual", "intensity_index"],
    )

    (out / "_snapshot.json").write_text(
        json.dumps(
            {
                "source": BASE,
                "window_start": start.isoformat(),
                "window_end": end.isoformat(),
                "fetched_at": datetime.now(UTC).isoformat(),
                "row_counts": {
                    "regional_intensity": c_reg_int,
                    "regional_genmix": c_reg_gen,
                    "national_intensity": c_nat,
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    counts = f"{c_reg_int} / {c_reg_gen} / {c_nat}"
    print(f"wrote {counts} rows to {out}")



def _day(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=UTC)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--start", type=_day, required=True)
    p.add_argument("--end", type=_day, required=True, help="exclusive; keep >=3 days in the past")
    p.add_argument("--out", type=Path, default=Path("substrate/data"))
    a = p.parse_args()
    main(a.start, a.end, a.out)