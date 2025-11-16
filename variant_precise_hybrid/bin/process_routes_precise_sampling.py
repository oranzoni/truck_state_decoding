#!/usr/bin/env python
import glob
import math
import pathlib
from collections import Counter

import orjson
import polars as pl
import polyline
import requests

# --------------------
# Config
# --------------------

BASE = pathlib.Path(__file__).resolve().parents[2]  # routing-state-time/
ROUTES_DIR = BASE / "variant_lazy_h3" / "routes_in"
OUT_DIR = BASE / "variant_precise_hybrid" / "outputs" / "by_trip"
OUT_DIR.mkdir(parents=True, exist_ok=True)

NOM_URL = "http://localhost:8080/reverse"
SAMPLE_STEP_METERS = 500.0  # target spacing between samples inside a maneuver


# --------------------
# Helpers
# --------------------

def haversine_m(lat1, lon1, lat2, lon2):
    """Distance in meters between two WGS84 points."""
    R = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def decode_shape(shape_str):
    """Decode Valhalla polyline (precision=6)."""
    return polyline.decode(shape_str, precision=6)


def build_cumdist(segment):
    """Given a list of (lat, lon), return cumulative distances from start (meters)."""
    if not segment:
        return []

    dists = [0.0]
    for i in range(1, len(segment)):
        lat1, lon1 = segment[i - 1]
        lat2, lon2 = segment[i]
        d = haversine_m(lat1, lon1, lat2, lon2)
        dists.append(dists[-1] + d)
    return dists


def sample_points_by_length(points, i0, i1, step_m=SAMPLE_STEP_METERS, min_samples=3):
    """
    Dense sampling: take points along [i0, i1] at ~step_m spacing using
    cumulative distance + linear interpolation.
    """
    if i1 <= i0 or i0 < 0 or i1 >= len(points):
        return []

    segment = points[i0 : i1 + 1]
    if len(segment) == 1:
        return [segment[0]]

    dists = build_cumdist(segment)
    total = dists[-1]

    if total <= 0:
        # collapsed segment, just midpoint
        mid_idx = len(segment) // 2
        return [segment[mid_idx]]

    n_steps = max(min_samples, int(total / step_m))
    targets = [k * total / n_steps for k in range(n_steps + 1)]

    samples = []
    j = 0
    for t in targets:
        while j < len(dists) - 2 and dists[j + 1] < t:
            j += 1
        # interpolate between j and j+1
        t0, t1 = dists[j], dists[j + 1]
        lat0, lon0 = segment[j]
        lat1, lon1 = segment[j + 1]

        if t1 <= t0:
            alpha = 0.0
        else:
            alpha = (t - t0) / (t1 - t0)

        lat = lat0 + alpha * (lat1 - lat0)
        lon = lon0 + alpha * (lon1 - lon0)
        samples.append((lat, lon))

    return samples


# Simple in-memory cache for reverse geocoding
_state_cache = {}


def nominatim_state(lat, lon):
    """Reverse-geocode a point to 'CC:StateName' (e.g. 'US:Colorado') or 'UNK'."""
    key = (round(lat, 4), round(lon, 4))
    if key in _state_cache:
        return _state_cache[key]

    try:
        resp = requests.get(
            NOM_URL,
            params={
                "lat": lat,
                "lon": lon,
                "format": "jsonv2",
                "zoom": 10,  # city/region level
            },
            timeout=5.0,
        )
        resp.raise_for_status()
        data = resp.json()
        addr = data.get("address", {})
        country_code = addr.get("country_code", "").upper()
        state = addr.get("state")
        if not country_code or not state:
            state_code = "UNK"
        else:
            state_code = f"{country_code}:{state}"
    except Exception:
        state_code = "UNK"

    _state_cache[key] = state_code
    return state_code


def split_time_by_states(samples):
    """
    Given a list of (lat, lon) samples along a maneuver, return
    a mapping state -> weight (0–1), based on counts of states.
    """
    if not samples:
        return {"UNK": 1.0}

    states = [nominatim_state(lat, lon) for (lat, lon) in samples]
    # if everything is UNK, don't pretend
    if all(s == "UNK" for s in states):
        return {"UNK": 1.0}

    # Count non-UNK; if some UNK, ignore them in proportions
    non_unk = [s for s in states if s != "UNK"]
    if not non_unk:
        return {"UNK": 1.0}

    counts = Counter(non_unk)
    total = sum(counts.values())
    return {s: c / total for s, c in counts.items()}


def process_route_file(fp):
    """
    Process a single Valhalla route JSON file into a per-state time breakdown
    using dense sampling and proportional splitting per maneuver.
    """
    text = fp.read_bytes()
    j = orjson.loads(text)

    trip = j.get("trip", {})
    legs = trip.get("legs", [])
    if not legs:
        print(f"[warn] No legs in {fp.name}")
        return None

    leg = legs[0]
    mans = leg.get("maneuvers", [])
    shape_str = leg.get("shape")
    if not mans or not shape_str:
        print(f"[warn] Missing maneuvers or shape in {fp.name}")
        return None

    pts = decode_shape(shape_str)
    if not pts:
        print(f"[warn] Empty decoded shape in {fp.name}")
        return None

    # Trip metadata
    trip_id = fp.stem  # e.g. New_York_to_San_Francisco
    vehicle_id = trip_id.split("_to_")[0] if "_to_" in trip_id else trip_id

    leg_summary = leg.get("summary", {})
    leg_seconds_total = leg_summary.get("time", None)

    records = []

    for m in mans:
        b = m.get("begin_shape_index")
        e = m.get("end_shape_index")
        t = m.get("time", 0)

        if b is None or e is None or t is None:
            continue
        if e <= b or t <= 0:
            continue
        if b < 0 or e >= len(pts):
            continue

        samples = sample_points_by_length(pts, b, e, step_m=SAMPLE_STEP_METERS)
        state_weights = split_time_by_states(samples)

        for state, w in state_weights.items():
            seconds = t * w
            records.append(
                {
                    "vehicle_id": vehicle_id,
                    "trip_id": trip_id,
                    "state": state,
                    "drive_seconds": seconds,
                    "leg_seconds_total": float(leg_seconds_total) if leg_seconds_total is not None else None,
                }
            )

    if not records:
        print(f"[warn] No records for {fp.name}")
        return None

    df = pl.DataFrame(records)

    # Optional: round seconds and enforce per-trip total consistency
    df = df.with_columns(
        pl.col("drive_seconds").round(0).cast(pl.Int64)
    )

    

    out_fp = OUT_DIR / f"{trip_id}.parquet"
    df.write_parquet(out_fp)
    print(f"[route-precise] {fp} -> {out_fp} ({df.height} rows)")
    return out_fp


def main():
    files = sorted(glob.glob(str(ROUTES_DIR / "*.json")))
    if not files:
        print(f"[error] No JSON routes found in {ROUTES_DIR}")
        return

    print(f"[info] Found {len(files)} route JSON files in {ROUTES_DIR}")
    for path_str in files:
        fp = pathlib.Path(path_str)
        process_route_file(fp)


if __name__ == "__main__":
    main()

