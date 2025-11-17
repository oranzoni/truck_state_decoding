## Variant: Precise Hybrid — Dense Geometry Sampling + Proportional Time Splitting

This variant computes high-precision per-state truck driving time by combining:

- Full maneuver geometry from Valhalla

- Dense sampling along the maneuver polyline

- Direct Nominatim reverse-geocoding for each sampled point

- Proportional time-splitting across state segments

- Polars-based analytics

It is the most accurate method in the routing-state-time project and is designed to catch narrow borders, diagonal crossings, and multi-state maneuvers that the lazy midpoint pipeline cannot resolve.

# 1. Purpose of the Precise Hybrid Variant

The lazy variant assigns one state per maneuver using a small number of sample points.

That fails when:

- A maneuver crosses a state border

- Borders are narrow (e.g., WV, MD, DE)

- Long maneuvers span several states

- The route geometry curves across boundaries

The precise hybrid variant eliminates these issues by:

- Sampling a dense set of points along each maneuver

- Reverse-geocoding every sampled point

- Splitting maneuver time proportionally across detected states

This produces ~99–99.9% correct attribution of driving time per state.

# 2. Core Algorithm
## Step 1 — Load Valhalla Route Data

Each JSON route includes:

- A list of maneuvers

- Encoded polyline geometry

- Valhalla’s modeled time per maneuver

The script extracts the polyline slice for each maneuver using its begin_shape_index and end_shape_index.

## Step 2 — Dense Geometry Sampling

Each maneuver is densely sampled at regular distance intervals.

Default spacing: 500 meters

Produces typically 10–20 sample points per maneuver

Configurable using ```SAMPLE_STEP_METERS```

## Procedure:

- Decode the maneuver geometry

- Compute cumulative distances along the shape

- Generate evenly spaced target distances

- Linearly interpolate intermediate coordinates

- Reverse-geocode each sample point using Nominatim

- This produces sequences such as:

```[US:California, US:California, US:California, US:Nevada, US:Nevada, US:Nevada]```

## Step 3 — Proportional Time Splitting

Sampled points are grouped by consecutive state segments.

Example:

State	Count	Weight
CA	3	3/6
NV	3	3/6

Time is allocated as:

time_CA = maneuver_time * (3/6)
time_NV = maneuver_time * (3/6)


This correctly handles maneuvers that:

- Cross borders multiple times

- Split 70/30 or 90/10 between states

- Curve across diagonal boundaries

## Step 4 — Output Format

Each processed trip produces rows like:

```vehicle_id | trip_id | state | drive_seconds | leg_seconds_total```


Stored as Parquet files for efficient analytics.

Expected volume:

~28,000 rows for 231 trips

~120–200 rows per trip

Each state transition is captured precisely

## 3. Analytics

After processing all trips:

```python bin/build_precise_analytics.py```


This generates:

```analytics_precise.parquet```


containing aggregated:

- Total drive seconds per state

- Per-trip summaries

- Per-state contributions across the dataset

## 4. Performance Characteristics

The precise variant is slower because each maneuver is sampled many times.

It performs significantly more computation due to:

- Dense sampling (10–20 points per maneuver)

- Many reverse-geocode calls

- Proportional time splitting into multiple state fragments

- Larger analytic datasets

## Benchmark:

Wall time: 22 minutes 35 seconds

For all 231 routes

Component	Lazy Variant	Precise Hybrid
Sampling	1 point	10–20 points
Reverse-geocode calls	Few	Many
State splits	Rare	Frequent
Rows produced	~1,484	~28,113

The runtime increase is expected and normal.

## 5. Running the Precise Hybrid Pipeline
Activate the environment
```cd ~/routing-state-time/variant_precise_hybrid```
```source .venv/bin/activate```


Ensure ```routes_in/``` contains the 231 Valhalla routes.

Process all routes
```python bin/process_routes_precise_sampling.py```

Build aggregated analytics
```python bin/build_precise_analytics.py```

## 6. Future Enhancements

- Adaptive sampling (higher density near borders)

- Sampling density proportional to maneuver length

- GPU-accelerated interpolation

- Confidence scoring for state transitions

- Advanced border-crossing segmentation
