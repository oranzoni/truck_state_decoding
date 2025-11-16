#!/usr/bin/env python
import glob
import pathlib

import polars as pl

# ---------- Paths ----------

BASE = pathlib.Path(__file__).resolve().parents[2]  # routing-state-time/
IN_DIR = BASE / "variant_precise_hybrid" / "outputs" / "by_trip"
OUT_DIR = BASE / "variant_precise_hybrid" / "outputs" / "analytics"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------- Load all per-trip parquet files ----------

files = sorted(glob.glob(str(IN_DIR / "*.parquet")))
if not files:
    raise SystemExit(f"[error] No parquet files found in {IN_DIR}")

dfs = [pl.read_parquet(fp) for fp in files]
df = pl.concat(dfs, how="vertical_relaxed")

print(f"[info] Loaded {len(files)} route files")
print(f"[info] Total rows: {df.height}")

# ---------- 1) Per-state totals ----------

state_totals = (
    df
    .group_by("state")
    .agg(
        pl.col("drive_seconds").sum().alias("total_drive_seconds"),
        pl.col("trip_id").n_unique().alias("num_trips"),
    )
    .with_columns(
        (pl.col("total_drive_seconds") / 3600).alias("total_drive_hours"),
        (pl.col("total_drive_seconds") / 3600 / pl.col("num_trips")).alias("avg_hours_per_trip"),
    )
    .sort("total_drive_hours", descending=True)
)

state_totals_fp_parq = OUT_DIR / "state_totals_precise.parquet"
state_totals_fp_csv = OUT_DIR / "state_totals_precise.csv"
state_totals.write_parquet(state_totals_fp_parq)
state_totals.write_csv(state_totals_fp_csv)
print(f"[save] state_totals_precise.* written to {OUT_DIR} ({state_totals.height} rows)")

# ---------- 2) Per-trip summaries ----------

trip_summary = (
    df
    .group_by("trip_id")
    .agg(
        pl.col("drive_seconds").sum().alias("total_drive_seconds"),
        pl.col("state").n_unique().alias("num_states"),
    )
    .with_columns(
        (pl.col("total_drive_seconds") / 3600).alias("total_drive_hours"),
    )
    .sort("total_drive_hours", descending=True)
)

trip_summary_fp_parq = OUT_DIR / "trip_summary_precise.parquet"
trip_summary_fp_csv = OUT_DIR / "trip_summary_precise.csv"
trip_summary.write_parquet(trip_summary_fp_parq)
trip_summary.write_csv(trip_summary_fp_csv)
print(f"[save] trip_summary_precise.* written to {OUT_DIR} ({trip_summary.height} rows)")

# ---------- 3) Per-state-per-trip (>= 1 hour) ----------

per_state_trip = (
    df
    .with_columns(
        (pl.col("drive_seconds") / 3600).alias("drive_hours"),
    )
    .filter(pl.col("drive_hours") >= 1.0)
    .sort(["trip_id", "state"])
)

per_state_trip_fp_parq = OUT_DIR / "per_state_trip_ge1h_precise.parquet"
per_state_trip_fp_csv = OUT_DIR / "per_state_trip_ge1h_precise.csv"
per_state_trip.write_parquet(per_state_trip_fp_parq)
per_state_trip.write_csv(per_state_trip_fp_csv)
print(f"[save] per_state_trip_ge1h_precise.* written to {OUT_DIR} ({per_state_trip.height} rows)")

