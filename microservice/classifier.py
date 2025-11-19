#!/usr/bin/env python3
"""
Core classification logic using H3 spatial indexing.

Classifies GPS coordinates to US states using a pre-built H3 cache.
"""

import time
from pathlib import Path

import h3
import numpy as np
import polars as pl


class TruckStateClassifier:
    """
    High-performance GPS point to state classifier using H3 indexing.
    """

    def __init__(
        self,
        resolution: int = 9,
        cache_path: str = None,
        shapefile_path: str = None
    ):
        """
        Initialize classifier with H3 cache.

        Args:
            resolution: H3 resolution (9 = ~174m hexagons)
            cache_path: Path to pre-built parquet cache
            shapefile_path: Path to shapefile for building cache
        """
        self.resolution = resolution
        self.cache = {}

        # State abbreviation mapping for Arrow IPC
        self.state_to_id = {}
        self.id_to_state = []

        # Load or build cache
        if cache_path and Path(cache_path).exists():
            self._load_cache(cache_path)
        elif shapefile_path and Path(shapefile_path).exists():
            self._build_cache_from_shapefile(shapefile_path)
        else:
            # Try default locations
            default_cache = Path(__file__).parent / 'cache' / 'h3_states_r9.parquet'
            default_shapefile = Path('/home/sbanjac/stateclassifier/data/tl_2023_us_state.shp')

            if default_cache.exists():
                self._load_cache(str(default_cache))
            elif default_shapefile.exists():
                self._build_cache_from_shapefile(str(default_shapefile))
                # Save for future use
                cache_dir = Path(__file__).parent / 'cache'
                cache_dir.mkdir(exist_ok=True)
                self._save_cache(str(default_cache))
            else:
                raise ValueError(
                    "No cache source provided. Please provide cache_path or shapefile_path."
                )

        # Build state ID lookup arrays
        self._build_state_arrays()

    def _load_cache(self, cache_path: str):
        """Load H3 cache from parquet file."""
        print(f"Loading H3 cache from {cache_path}...")
        start = time.time()

        df = pl.read_parquet(cache_path)

        # Build cache dict
        h3_col = df['h3'].to_list()
        state_col = df['state_code'].to_list()

        self.cache = dict(zip(h3_col, state_col))

        elapsed = time.time() - start
        print(f"Loaded {len(self.cache):,} H3 cells in {elapsed:.2f}s")

    def _build_cache_from_shapefile(self, shapefile_path: str):
        """Build H3 cache from US states shapefile."""
        from cache_builder import build_h3_cache_from_shapefile

        print(f"Building H3 cache from {shapefile_path}...")
        self.cache = build_h3_cache_from_shapefile(
            shapefile_path,
            self.resolution
        )

    def _save_cache(self, output_path: str):
        """Save cache to parquet for future use."""
        from cache_builder import save_cache_to_parquet
        save_cache_to_parquet(self.cache, output_path)

    def _build_state_arrays(self):
        """Build state ID lookup arrays for fast conversion."""
        # Get unique states sorted alphabetically
        unique_states = sorted(set(self.cache.values()))

        self.id_to_state = unique_states
        self.state_to_id = {state: i for i, state in enumerate(unique_states)}

        # Add unknown state
        if 'UNK' not in self.state_to_id:
            self.state_to_id['UNK'] = len(self.id_to_state)
            self.id_to_state.append('UNK')

        print(f"State mapping: {len(self.id_to_state)} states")

    def classify(self, lats, lons):
        """
        Classify points to states.

        Args:
            lats: Array of latitudes
            lons: Array of longitudes

        Returns:
            states: numpy array of state abbreviations
            cache_hits: number of successful cache lookups
        """
        n = len(lats)
        states = []
        cache_hits = 0

        # Ensure numpy arrays
        if not isinstance(lats, np.ndarray):
            lats = np.array(lats, dtype=np.float64)
        if not isinstance(lons, np.ndarray):
            lons = np.array(lons, dtype=np.float64)

        for i in range(n):
            try:
                # Convert to H3 cell using H3 v4 API
                cell = h3.latlng_to_cell(lats[i], lons[i], self.resolution)

                # Lookup in cache
                state = self.cache.get(cell, 'UNK')

                if state != 'UNK':
                    cache_hits += 1

            except Exception:
                state = 'UNK'

            states.append(state)

        return np.array(states), cache_hits

    def states_to_ids(self, states):
        """
        Convert state abbreviations to numeric IDs.

        Args:
            states: Array of state abbreviations

        Returns:
            numpy array of uint8 state IDs
        """
        unk_id = self.state_to_id.get('UNK', 255)

        return np.array(
            [self.state_to_id.get(s, unk_id) for s in states],
            dtype=np.uint8
        )

    def ids_to_states(self, ids):
        """
        Convert numeric IDs back to state abbreviations.

        Args:
            ids: Array of state IDs

        Returns:
            List of state abbreviations
        """
        return [self.id_to_state[i] if i < len(self.id_to_state) else 'UNK'
                for i in ids]


if __name__ == '__main__':
    # Test classifier
    import time

    print("Initializing classifier...")
    classifier = TruckStateClassifier(resolution=9)

    # Test points
    test_lats = [40.7128, 34.0522, 41.8781, 29.7604, 33.4484]
    test_lons = [-74.0060, -118.2437, -87.6298, -95.3698, -112.0740]

    print("\nTest classification:")
    start = time.time()
    states, hits = classifier.classify(test_lats, test_lons)
    elapsed = time.time() - start

    for lat, lon, state in zip(test_lats, test_lons, states):
        print(f"  ({lat:.4f}, {lon:.4f}) -> {state}")

    print(f"\nCache hits: {hits}/{len(test_lats)}")
    print(f"Time: {elapsed*1000:.3f}ms")

    # Performance test
    print("\nPerformance test (10,000 points)...")
    import random

    n_points = 10000
    lats = [40.0 + random.uniform(-5, 5) for _ in range(n_points)]
    lons = [-100.0 + random.uniform(-20, 20) for _ in range(n_points)]

    start = time.time()
    states, hits = classifier.classify(lats, lons)
    elapsed = time.time() - start

    print(f"Classified {n_points} points in {elapsed*1000:.1f}ms")
    print(f"Throughput: {n_points/elapsed:,.0f} pts/sec")
    print(f"Cache hits: {hits}/{n_points} ({100*hits/n_points:.1f}%)")
