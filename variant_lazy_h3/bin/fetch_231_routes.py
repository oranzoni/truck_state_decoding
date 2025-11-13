import os, itertools, requests, orjson as json

VALHALLA_URL = "http://localhost:8002/route"
OUT_DIR = os.path.expanduser("~/state-time/routes_in")
os.makedirs(OUT_DIR, exist_ok=True)

CITIES = {
  "New_York":        (40.7128, -74.0060),
  "Los_Angeles":     (34.0522, -118.2437),
  "Chicago":         (41.8781, -87.6298),
  "Dallas":          (32.7767, -96.7970),
  "Houston":         (29.7604, -95.3698),
  "Washington":      (38.9072, -77.0369),
  "Miami":           (25.7617, -80.1918),
  "Philadelphia":    (39.9526, -75.1652),
  "Atlanta":         (33.7490, -84.3880),
  "Phoenix":         (33.4484, -112.0740),
  "Boston":          (42.3601, -71.0589),
  "San_Francisco":   (37.7749, -122.4194),
  "Detroit":         (42.3314, -83.0458),
  "Seattle":         (47.6062, -122.3321),
  "Minneapolis":     (44.9778, -93.2650),
  "San_Diego":       (32.7157, -117.1611),
  "Denver":          (39.7392, -104.9903),
  "Orlando":         (28.5383, -81.3792),
  "Charlotte":       (35.2271, -80.8431),
  "Baltimore":       (39.2904, -76.6122),
  "San_Antonio":     (29.4241, -98.4936),
  "Austin":          (30.2672, -97.7431),
}

def fetch(cityA, cityB):
    (lat1, lon1) = CITIES[cityA]
    (lat2, lon2) = CITIES[cityB]

    payload = {
        "locations": [
            {"lat": lat1, "lon": lon1},
            {"lat": lat2, "lon": lon2}
        ],
        "costing": "truck",
        "directions_options": {"units": "kilometers"}
    }

    r = requests.post(VALHALLA_URL, json=payload, timeout=30)
    r.raise_for_status()

    fp = os.path.join(OUT_DIR, f"{cityA}_to_{cityB}.json")
    with open(fp, "wb") as f:
        f.write(json.dumps(r.json()))

    print("Saved:", fp)

if __name__ == "__main__":
    pairs = list(itertools.combinations(CITIES.keys(), 2))
    print("Total routes to compute:", len(pairs))

    for a, b in pairs:
        fetch(a, b)

    print("All routes saved.")

