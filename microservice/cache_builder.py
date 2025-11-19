#!/usr/bin/env python3
"""
H3 Cache Builder for Truck State Classification

Builds a comprehensive H3 cell -> state mapping from US Census TIGER shapefile.
This eliminates the need for Nominatim reverse geocoding.
"""

import time
from pathlib import Path

import geopandas as gpd
import h3
import polars as pl
from shapely.geometry import mapping


def build_h3_cache_from_shapefile(
    shapefile_path: str,
    resolution: int = 9,
    output_path: str = None
) -> dict:
    """
    Build comprehensive H3 cache from US states shapefile.

    Args:
        shapefile_path: Path to TIGER shapefile (.shp)
        resolution: H3 resolution (9 = ~174m hexagons)
        output_path: Optional path to save parquet cache

    Returns:
        Dict mapping H3 cell ID -> state abbreviation
    """
    print(f"Loading shapefile: {shapefile_path}")
    start = time.time()

    states_gdf = gpd.read_file(shapefile_path)
    print(f"Loaded {len(states_gdf)} state geometries in {time.time() - start:.2f}s")

    cache = {}
    total_cells = 0

    print(f"Building H3 cache at resolution {resolution}...")

    for idx, state in states_gdf.iterrows():
        # Get state abbreviation (e.g., 'NY', 'CA')
        abbrev = state.get('STUSPS')
        if abbrev is None:
            # Try alternative column names
            abbrev = state.get('STATE_ABBR') or state.get('STATEABBR')

        if abbrev is None:
            print(f"Warning: No abbreviation found for state at index {idx}")
            continue

        geometry = state['geometry']
        if geometry is None or geometry.is_empty:
            continue

        # Convert geometry to H3 cells
        try:
            cells = geometry_to_h3_cells(geometry, resolution)

            for cell in cells:
                cache[cell] = abbrev

            total_cells += len(cells)
            print(f"  {abbrev}: {len(cells):,} cells")

        except Exception as e:
            print(f"Warning: Could not process {abbrev}: {e}")

    elapsed = time.time() - start
    print(f"\nBuilt cache with {len(cache):,} unique H3 cells in {elapsed:.2f}s")
    print(f"Total cells generated: {total_cells:,}")

    # Save to parquet if output path provided
    if output_path:
        save_cache_to_parquet(cache, output_path)

    return cache


def geometry_to_h3_cells(geometry, resolution: int) -> set:
    """
    Convert a Shapely geometry to H3 cells.

    Handles Polygon and MultiPolygon geometries.
    """
    cells = set()

    if geometry.geom_type == 'Polygon':
        cells.update(polygon_to_h3_cells(geometry, resolution))

    elif geometry.geom_type == 'MultiPolygon':
        for poly in geometry.geoms:
            cells.update(polygon_to_h3_cells(poly, resolution))

    else:
        print(f"Warning: Unsupported geometry type: {geometry.geom_type}")

    return cells


def polygon_to_h3_cells(polygon, resolution: int) -> set:
    """
    Convert a single Polygon to H3 cells using H3 v4 API.
    """
    # Convert to GeoJSON format
    geojson = mapping(polygon)

    # H3 v4 API: geo_to_cells expects a GeoJSON-like dict
    # with 'type' and 'coordinates'
    try:
        # Use h3.geo_to_cells for H3 v4
        # This function expects the geometry in GeoJSON format
        cells = h3.geo_to_cells(geojson, resolution)
        return set(cells)
    except AttributeError:
        # Fallback for older h3 versions
        try:
            cells = h3.polyfill(geojson, resolution, geo_json_conformant=True)
            return set(cells)
        except Exception as e:
            print(f"Error in polyfill: {e}")
            return set()
    except Exception as e:
        print(f"Error in geo_to_cells: {e}")
        return set()


def save_cache_to_parquet(cache: dict, output_path: str):
    """Save H3 cache to parquet file."""
    print(f"Saving cache to {output_path}...")

    # Convert to polars DataFrame
    df = pl.DataFrame({
        'h3': list(cache.keys()),
        'state_code': list(cache.values())
    })

    # Sort by h3 for better compression
    df = df.sort('h3')

    # Save to parquet
    df.write_parquet(output_path)

    # Get file size
    size_mb = Path(output_path).stat().st_size / (1024 * 1024)
    print(f"Saved {len(cache):,} cells to {output_path} ({size_mb:.2f} MB)")


def load_cache_from_parquet(cache_path: str) -> dict:
    """Load H3 cache from parquet file."""
    print(f"Loading cache from {cache_path}...")
    start = time.time()

    df = pl.read_parquet(cache_path)

    cache = {}
    for row in df.iter_rows(named=True):
        cache[row['h3']] = row['state_code']

    elapsed = time.time() - start
    print(f"Loaded {len(cache):,} cells in {elapsed:.2f}s")

    return cache


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Build H3 cache from shapefile')
    parser.add_argument(
        '--shapefile',
        default='/home/sbanjac/stateclassifier/data/tl_2023_us_state.shp',
        help='Path to US states shapefile'
    )
    parser.add_argument(
        '--resolution',
        type=int,
        default=9,
        help='H3 resolution (default: 9)'
    )
    parser.add_argument(
        '--output',
        default='cache/h3_states_r9.parquet',
        help='Output parquet file path'
    )

    args = parser.parse_args()

    # Build cache
    cache = build_h3_cache_from_shapefile(
        args.shapefile,
        args.resolution,
        args.output
    )

    print(f"\nCache built successfully!")
    print(f"Total cells: {len(cache):,}")

    # Show sample
    sample_cells = list(cache.items())[:5]
    print("\nSample entries:")
    for cell, state in sample_cells:
        print(f"  {cell} -> {state}")
