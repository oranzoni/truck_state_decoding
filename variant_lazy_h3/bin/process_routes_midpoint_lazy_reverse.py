print(">>> EXECUTING PATCHED SCRIPT <<<", __file__)

import os, sys, glob, math
from pathlib import Path

import orjson as json
import polars as pl
import h3
import polyline
import requests
from collections import Counter

# ---------------------------------------------------------------------
# Paths & Config
# ---------------------------------------------------------------------
PROJ = os.path.expanduser("~/state-time")
ROUTES_DIR = os.environ.get("ROUTES_DIR", f"{PROJ}/routes_in")
OUT_DIR = os.environ.get("OUT_DIR", f"{PROJ}/outputs/by_trip")
CACHE_DIR = os.path.join(PROJ, "cache", "h3")
NOM_URL = os.environ.get("NOM_URL", "http://localhost:8080")
H3_RES = int(os.environ.get("H3_RES", "9"))

os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

CACHE_FILE = os.path.join(CACHE_DIR, f"h3_lazy_r{H3_RES}.parquet")

# H3 API (4.x)
LATLNG_TO_CELL = h3.latlng_to_cell
CELL_TO_LATLNG = h3.cell_to_latlng


# ---------------------------------------------------------------------
# Cache Handling
# ---------------------------------------------------------------------
def load_h3_cache():
    """Load existing H3→state mapping."""
    if not os.path.exists(CACHE_FILE):
        return {}
    df = pl.read_parquet(CACHE_FILE)
    mapping = {row[0]: row[1] for row in df.iter_rows()}
    print(f"[lazy-cache] loaded {len(mapping)} cells")
    return mapping

def save_h3_cache(cache):
    """Save updated H3→state mapping."""
    if not cache:
        return
    df = pl.DataFrame(
        [(k, v) for k, v in cache.items()],
        schema=["h3", "state_code"]
    ).unique(subset=["h3"])
    df.write_parquet(CACHE_FILE)
    print(f"[lazy-cache] saved {df.height} cells to {CACHE_FILE}")


# ---------------------------------------------------------------------
# Reverse Geocoding
# ---------------------------------------------------------------------
def reverse_state(lat, lon):
    """Reverse geocode lat/lon → state code, fallback UNK."""
    try:
        r = requests.get(
            f"{NOM_URL}/reverse",
            params={
                "format": "jsonv2",
                "lat": lat,
                "lon": lon,
                "zoom": 10,
                "addressdetails": 1,
            },
            timeout=5,
        )
        r.raise_for_status()
        j = r.json()
        addr = j.get("address", {})
        state = (
            addr.get("state") or
            addr.get("region") or
            addr.get("province")
        )
        if not state:
            return None
        cc = (addr.get("country_code") or "").upper()
        return f"{cc}:{state}"
    except Exception:
        return None


def get_state_for_latlon(lat, lon, cache):
    """Map lat/lon → H3 cell → cached or reverse-geocoded state."""
    cell = LATLNG_TO_CELL(lat, lon, H3_RES)
    if cell in cache:
        return cache[cell]
    code = reverse_state(lat, lon)
    if code is None:
        code = "UNK"
    cache[cell] = code
    return code


# ---------------------------------------------------------------------
# Geometry Helpers
# ---------------------------------------------------------------------
def great_circle_m(p0, p1):
    """Great-circle distance in meters."""
    lat1, lon1 = math.radians(p0[0]), math.radians(p0[1])
    lat2, lon2 = math.radians(p1[0]), math.radians(p1[1])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = (
        math.sin(dlat/2)**2 +
        math.cos(lat1)*math.cos(lat2)*math.sin(dlon/2)**2
    )
    return 6371000.0 * 2 * math.asin(math.sqrt(a))


def sample_points_by_length(points, i0, i1, n=5):
    """
    Return n evenly spaced sample points along the segment [i0, i1].
    """
    if i1 <= i0:
        return [points[i0]]

    segs = points[i0:i1+1]

    # cumulative distances
    d = [0.0]
    for a, b in zip(segs[:-1], segs[1:]):
        d.append(d[-1] + great_circle_m(a, b))

    total = d[-1]
    if total <= 0:
        return [segs[0]]

    targets = [(k + 1) * total / (n + 1) for k in range(n)]
    samples = []

    for t in targets:
        for k in range(1, len(d)):
            if d[k] >= t:
                prev, nxt = segs[k-1], segs[k]
                seglen = d[k] - d[k-1]
                if seglen <= 0:
                    samples.append(prev)
                else:
                    frac = (t - d[k-1]) / seglen
                    lat = prev[0] + frac * (nxt[0] - prev[0])
                    lon = prev[1] + frac * (nxt[1] - prev[1])
                    samples.append((lat, lon))
                break

    return samples


# ---------------------------------------------------------------------
# Route Processing
# ---------------------------------------------------------------------
def parse_vehicle_trip(fp):
    base = Path(fp).stem
    if "_" in base:
        v, t = base.split("_", 1)
        return v, t
    return "veh", base


def process_route_file(fp, cache, samples_per_maneuver=5):
    data = json.loads(Path(fp).read_bytes())
    trip = data.get("trip", {})
    legs = trip.get("legs", [])
    if not legs:
        return None

    leg = legs[0]

    shape = leg.get("shape")
    if not shape:
        return None
    pts = polyline.decode(shape,precision=6)

    mans = leg.get("maneuvers", [])
    per_state = {}
    total_time = 0.0

    for m in mans:
        b = m.get("begin_shape_index")
        e = m.get("end_shape_index")
        t = float(m.get("time", 0))

        if b is None or e is None or t <= 0:
            continue

        b, e = int(b), int(e)
        if e < b:
            b, e = e, b

        # sample multiple points
        samples = sample_points_by_length(pts, b, e, n=samples_per_maneuver)
        state_codes = [
            get_state_for_latlon(lat, lon, cache)
            for lat, lon in samples
        ]

        # majority vote
        state_code = Counter(state_codes).most_common(1)[0][0]

        per_state[state_code] = per_state.get(state_code, 0.0) + t
        total_time += t

    vid, tid = parse_vehicle_trip(fp)
    leg_sum = float(leg.get("summary", {}).get("time", total_time))

    rows = [
        {
            "vehicle_id": vid,
            "trip_id": tid,
            "state": st,
            "drive_seconds": int(round(sec)),
            "leg_seconds_total": int(round(leg_sum)),
        }
        for st, sec in per_state.items()
    ]

    return pl.DataFrame(rows) if rows else None


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main():
    cache = load_h3_cache()

    files = []
    for ext in ("*.json",):
        files.extend(glob.glob(os.path.join(ROUTES_DIR, ext)))

    if not files:
        print(f"No routes in {ROUTES_DIR}")
        sys.exit(0)

    total_rows = 0

    for fp in files:
        df = process_route_file(fp, cache, samples_per_maneuver=5)
        if df is None or df.height == 0:
            continue

        row0 = df.row(0, named=True)
        out_fp = os.path.join(
            OUT_DIR,
            f"{row0['vehicle_id']}_{row0['trip_id']}.parquet"
        )
        df.write_parquet(out_fp)

        total_rows += df.height
        print(f"[route] {fp} -> {out_fp} ({df.height} rows)")

    save_h3_cache(cache)
    print(f"Done. Total rows: {total_rows}, cache size: {len(cache)}")


if __name__ == "__main__":
    main()

