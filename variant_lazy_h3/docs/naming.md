## Input routes (preferred)
- One JSON per route: routes_in/<vehicleId>_<tripId>.json
- OR batched NDJSON: routes_in/batch_YYYYMMDD_hhmm.ndjson

## Outputs
- Per-trip: outputs/by_trip/<vehicleId>_<tripId>.parquet (or .csv)
- Aggregates: outputs/by_vehicle/<vehicleId>_YYYYMMDD.parquet
