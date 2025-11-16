#!/usr/bin/env python
import pathlib
import polars as pl

BASE = pathlib.Path(__file__).resolve().parent

lazy_fp = BASE / "variant_lazy_h3" / "analytics.parquet"
precise_fp = BASE / "variant_precise_hybrid" / "outputs" / "analytics" / "analytics_precise.parquet"

print(f"[info] Lazy analytics:    {lazy_fp}")
print(f"[info] Precise analytics: {precise_fp}")

lazy = (
    pl.read_parquet(lazy_fp)
    .rename({"drive_seconds": "drive_seconds_lazy"})
)

precise = (
    pl.read_parquet(precise_fp)
    .rename({"drive_seconds": "drive_seconds_precise"})
)

print(f"[info] Loaded lazy rows:    {lazy.height}")
print(f"[info] Loaded precise rows: {precise.height}")

# Outer/full join on trip_id + state so that any state that appears in one
# but not the other is still visible.
joined = (
    lazy.join(
        precise,
        on=["trip_id", "state"],
        how="full",  # use 'full' instead of deprecated 'outer'
        suffix="_precise",
    )
    # fill nulls in drive seconds so diff math is defined
    .with_columns([
        pl.col("drive_seconds_lazy").fill_null(0),
        pl.col("drive_seconds_precise").fill_null(0),
    ])
    # create difference_sec and pct_diff
    .with_columns([
        (pl.col("drive_seconds_precise") - pl.col("drive_seconds_lazy"))
        .alias("difference_sec"),
        (
            pl.when(pl.col("drive_seconds_precise") > 0)
            .then(
                (pl.col("drive_seconds_precise") - pl.col("drive_seconds_lazy"))
                / pl.col("drive_seconds_precise")
                * 100.0
            )
            .otherwise(None)
            .alias("pct_diff")
        ),
    ])
)

out_fp = BASE / "lazy_vs_precise.parquet"
joined.write_parquet(out_fp)
print(f"[save] Joined comparison written to {out_fp}")

# Quick summary
summary = joined.select(
    [
        pl.count().alias("rows"),
        pl.col("difference_sec").abs().max().alias("max_abs_diff_sec"),
        pl.col("difference_sec").abs().mean().alias("mean_abs_diff_sec"),
        pl.col("pct_diff").abs().max().alias("max_abs_pct_diff"),
        pl.col("pct_diff").abs().mean().alias("mean_abs_pct_diff"),
    ]
)
print("[summary]")
print(summary)

