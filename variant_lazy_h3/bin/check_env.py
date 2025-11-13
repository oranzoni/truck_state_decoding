import sys, importlib, json
mods = ["h3", "polyline", "orjson", "polars", "tqdm", "rich"]
optional = ["shapely", "pyproj", "simdjson"]
ok = {}
for m in mods + optional:
    try:
        importlib.import_module(m)
        ok[m] = "OK"
    except Exception as e:
        ok[m] = f"MISS ({e.__class__.__name__})"
print(json.dumps(ok, indent=2))
