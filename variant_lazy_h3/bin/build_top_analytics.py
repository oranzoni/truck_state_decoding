import polars as pl
import pathlib

BASE = pathlib.Path("~/state-time").expanduser()
ANALYTICS_FP = BASE / "analytics.parquet"
OUT_DIR = BASE / "outputs" / "analytics"
OUT_DIR.mkdir(parents=True, exist_ok=True)

df = pl.read_parquet(ANALYTICS_FP)
print("Loaded analytics:", df.height, "rows,", df.width, "columns")

# 1) Per-state totals
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

state_totals.write_csv(OUT_DIR / "state_totals.csv")
state_totals.write_parquet(OUT_DIR / "state_totals.parquet")
print("Wrote state_totals.* with", state_totals.height, "rows")

# 2) Per-trip summary
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

trip_summary.write_csv(OUT_DIR / "trip_summary.csv")
trip_summary.write_parquet(OUT_DIR / "trip_summary.parquet")
print("Wrote trip_summary.* with", trip_summary.height, "rows")

# 3) Per-state-per-trip filtered (only segments >= 1 hour)
per_state_trip = (
    df
    .with_columns(
        (pl.col("drive_seconds") / 3600).alias("drive_hours"),
    )
    .filter(pl.col("drive_hours") >= 1.0)
    .sort(["trip_id", "state"])
)

per_state_trip.write_csv(OUT_DIR / "per_state_trip_ge1h.csv")
per_state_trip.write_parquet(OUT_DIR / "per_state_trip_ge1h.parquet")
print("Wrote per_state_trip_ge1h.* with", per_state_trip.height, "rows")

