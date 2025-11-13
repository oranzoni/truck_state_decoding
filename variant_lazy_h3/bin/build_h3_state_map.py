import os, sys, time
import orjson as json
import requests
import polars as pl
import h3

NOM_URL = os.environ.get("NOM_URL", "http://localhost:8080")
OUT_DIR = os.path.expanduser("~/state-time/cache/h3")
POLY_DIR = os.path.expanduser("~/state-time/cache/polygons")
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(POLY_DIR, exist_ok=True)

# H3 capability detection
HAS_POLY_TO_CELLS = hasattr(h3, "polygon_to_cells")
HAS_POLYFILL     = hasattr(h3, "polyfill")
GRID_DISK        = getattr(h3, "grid_disk", None) or getattr(h3, "k_ring", None)
LATLNG_TO_CELL   = getattr(h3, "latlng_to_cell", None) or getattr(h3, "geo_to_h3", None)
CELL_TO_LATLNG   = getattr(h3, "cell_to_latlng", None) or getattr(h3, "h3_to_geo", None)

US_STATES = ["Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut","Delaware",
"District of Columbia","Florida","Georgia","Hawaii","Idaho","Illinois","Indiana","Iowa","Kansas",
"Kentucky","Louisiana","Maine","Maryland","Massachusetts","Michigan","Minnesota","Mississippi",
"Missouri","Montana","Nebraska","Nevada","New Hampshire","New Jersey","New Mexico","New York",
"North Carolina","North Dakota","Ohio","Oklahoma","Oregon","Pennsylvania","Rhode Island",
"South Carolina","South Dakota","Tennessee","Texas","Utah","Vermont","Virginia","Washington",
"West Virginia","Wisconsin","Wyoming"]

CA_PROVINCES = ["Alberta","British Columbia","Manitoba","New Brunswick","Newfoundland and Labrador",
"Nova Scotia","Ontario","Prince Edward Island","Quebec","Saskatchewan","Northwest Territories",
"Nunavut","Yukon"]

MX_STATES = ["Aguascalientes","Baja California","Baja California Sur","Campeche","Chiapas","Chihuahua",
"Ciudad de México","Coahuila","Colima","Durango","Guanajuato","Guerrero","Hidalgo","Jalisco","México",
"Michoacán","Morelos","Nayarit","Nuevo León","Oaxaca","Puebla","Querétaro","Quintana Roo",
"San Luis Potosí","Sinaloa","Sonora","Tabasco","Tamaulipas","Tlaxcala","Veracruz","Yucatán","Zacatecas"]

SETS = [("US","us",US_STATES), ("CA","ca",CA_PROVINCES), ("MX","mx",MX_STATES)]

def nominatim_state_q(countrycode, name):
    r = requests.get(f"{NOM_URL}/search", params={
        "format":"jsonv2","q":name,"countrycodes":countrycode,"limit":1,
        "polygon_geojson":1,"addressdetails":1,"dedupe":1,"extratags":1
    }, timeout=120)
    r.raise_for_status()
    return r.json()

def state_code(rec, cc_up):
    extra = rec.get("extratags") or {}
    code = extra.get("ISO3166-2")
    if code: return code
    nm = rec.get("name") or (rec.get("display_name","").split(",")[0])
    return f"{cc_up}:{nm}"

# simple ray-casting point-in-polygon (lon,lat arrays)
def _point_in_ring(lon, lat, ring):
    inside = False
    for i in range(len(ring)-1):
        x1,y1 = ring[i]; x2,y2 = ring[i+1]
        if ((y1>lat) != (y2>lat)) and (lon < (x2-x1)*(lat-y1)/(y2-y1+1e-18)+x1):
            inside = not inside
    return inside

def _point_in_poly(lon, lat, geom):
    t = geom["type"]
    if t == "Polygon":
        outer = geom["coordinates"][0]
        if not _point_in_ring(lon, lat, outer): return False
        # ignore holes for speed
        return True
    elif t == "MultiPolygon":
        for poly in geom["coordinates"]:
            outer = poly[0]
            if _point_in_ring(lon, lat, outer): return True
        return False
    return False

def polygon_to_cells_adaptive(geom, res):
    # Fast path: native H3 polygon coverage
    if HAS_POLY_TO_CELLS or HAS_POLYFILL:
        cells = set()
        if geom["type"] == "Polygon":
            gj = {"type":"Polygon","coordinates":[geom["coordinates"][0]]}
            if HAS_POLY_TO_CELLS: cells |= set(h3.polygon_to_cells(gj, res))
            else:                  cells |= set(h3.polyfill(gj, res))
        elif geom["type"] == "MultiPolygon":
            for poly in geom["coordinates"]:
                gj = {"type":"Polygon","coordinates":[poly[0]]}
                if HAS_POLY_TO_CELLS: cells |= set(h3.polygon_to_cells(gj, res))
                else:                  cells |= set(h3.polyfill(gj, res))
        return cells

    # Fallback (no polygon API): scan bbox, keep cells whose centroid is inside
    coords = geom["coordinates"]
    # compute bbox (lon/lat order per GeoJSON)
    lons, lats = [], []
    if geom["type"] == "Polygon":
        for x,y in geom["coordinates"][0]:
            lons.append(x); lats.append(y)
    elif geom["type"] == "MultiPolygon":
        for poly in geom["coordinates"]:
            for x,y in poly[0]:
                lons.append(x); lats.append(y)
    minx,maxx = min(lons), max(lons)
    miny,maxy = min(lats), max(lats)

    # step roughly by ~cell size (deg) to generate candidate cells
    # H3 res 9 ≈ 0.5–1 km; use ~0.25 deg coarse grid and expand via neighbors
    step = 0.25
    seeds = set()
    y = miny
    while y <= maxy+1e-9:
        x = minx
        while x <= maxx+1e-9:
            seeds.add(LATLNG_TO_CELL(y, x, res))
            x += step
        y += step
    # expand a bit
    cells = set()
    for s in seeds:
        cells.add(s)
        for nb in GRID_DISK(s, 1):
            cells.add(nb)
    # keep only cells whose centroid is inside polygon
    keep = set()
    for c in cells:
        lat, lon = CELL_TO_LATLNG(c)
        if _point_in_poly(lon, lat, geom):
            keep.add(c)
    return keep

def main():
    if len(sys.argv) < 2:
        print("Usage: build_h3_state_map.py <h3_res:int> [--save-polygons]")
        sys.exit(1)
    res = int(sys.argv[1])
    save_polys = "--save-polygons" in sys.argv

    rows=[]
    for cc_up, cc, names in SETS:
        print(f"== {cc_up} ==")
        for name in names:
            try:
                recs = nominatim_state_q(cc, name)
                if not recs:
                    print(f"  {name}: NOT FOUND"); continue
                rec = recs[0]
                geom = rec.get("geojson")
                if not geom or geom.get("type") not in ("Polygon","MultiPolygon"):
                    print(f"  {name}: no polygon"); continue
                code = state_code(rec, cc_up)
                if save_polys:
                    with open(os.path.join(POLY_DIR, f"{code.replace(':','_')}.geojson"), "wb") as f:
                        f.write(json.dumps(geom))
                cells = polygon_to_cells_adaptive(geom, res)
                for c in cells:
                    rows.append((c, code, name, cc_up))
                print(f"  {name}: {len(cells)} cells")
                time.sleep(0.05)
            except Exception as e:
                print(f"  {name}: ERROR {e}")

    if not rows:
        print("No rows generated."); sys.exit(2)
    df = pl.DataFrame(rows, schema=["h3","state_code","state_name","country"]).unique(subset=["h3"], keep="first")
    df.write_parquet(os.path.join(OUT_DIR, f"h3_to_state_r{res}.parquet"))
    df.write_csv(os.path.join(OUT_DIR, f"h3_to_state_r{res}.csv"))
    print(f"Wrote h3_to_state_r{res}.parquet with {df.height} cells")

    # Border ring
    print("Building border ring …")
    lut = {r[0]: r[1] for r in df.iter_rows()}
    border=[]
    for cell, sc in lut.items():
        for nb in GRID_DISK(cell, 1):
            if lut.get(nb) not in (None, sc):
                border.append((cell,1)); break
    pl.DataFrame(border, schema=["h3","border_flag"]).write_parquet(
        os.path.join(OUT_DIR, f"h3_border_ring_r{res}.parquet")
    )
    print("Done.")
if __name__ == "__main__":
    main()

