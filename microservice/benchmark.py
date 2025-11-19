#!/usr/bin/env python3
"""
Benchmark script for Truck State Classification Microservice.

Tests throughput and latency for various batch sizes.
"""

import json
import random
import time
import urllib.request
from urllib.error import URLError

# Configuration
BASE_URL = "http://localhost:8444"

# Major US cities for realistic test data
CITIES = [
    (40.7128, -74.0060),   # New York, NY
    (34.0522, -118.2437),  # Los Angeles, CA
    (41.8781, -87.6298),   # Chicago, IL
    (29.7604, -95.3698),   # Houston, TX
    (33.4484, -112.0740),  # Phoenix, AZ
    (39.7392, -104.9903),  # Denver, CO
    (47.6062, -122.3321),  # Seattle, WA
    (25.7617, -80.1918),   # Miami, FL
    (42.3601, -71.0589),   # Boston, MA
    (33.7490, -84.3880),   # Atlanta, GA
]


def generate_test_points(n_points):
    """Generate random GPS points around major US cities."""
    lats = []
    lons = []

    for i in range(n_points):
        # Select random city
        city = CITIES[i % len(CITIES)]

        # Add random offset (within ~100km)
        lat = city[0] + random.uniform(-1, 1)
        lon = city[1] + random.uniform(-1, 1)

        lats.append(lat)
        lons.append(lon)

    return lats, lons


def check_health():
    """Check if service is healthy."""
    try:
        req = urllib.request.Request(f"{BASE_URL}/health")
        with urllib.request.urlopen(req, timeout=5) as response:
            result = json.load(response)
            print(f"Service status: {result['status']}")
            print(f"Cache size: {result.get('cache_size', 'N/A'):,}")
            print(f"H3 resolution: {result.get('h3_resolution', 'N/A')}")
            return True
    except URLError as e:
        print(f"Service not available: {e}")
        return False


def benchmark_json(n_points, warmup=False):
    """Benchmark JSON endpoint."""
    lats, lons = generate_test_points(n_points)
    payload = json.dumps({"lat": lats, "lon": lons}).encode('utf-8')

    req = urllib.request.Request(
        f"{BASE_URL}/classify_points",
        data=payload,
        headers={"Content-Type": "application/json"}
    )

    start = time.time()
    with urllib.request.urlopen(req, timeout=300) as response:
        result = json.load(response)
    elapsed = time.time() - start

    if not warmup:
        throughput = n_points / elapsed
        print(f"\nJSON Response ({n_points:,} points):")
        print(f"  Total time: {elapsed*1000:.1f}ms")
        print(f"  Throughput: {throughput:,.0f} pts/sec")
        print(f"  Cache hits: {result['metadata']['cache_hits']:,}")
        print(f"  Server throughput: {result['performance']['throughput_pts_sec']:,} pts/sec")

        # Show sample states
        states = result['states'][:10]
        print(f"  Sample states: {states}")

    return elapsed, result


def benchmark_arrow(n_points):
    """Benchmark Arrow IPC endpoint."""
    lats, lons = generate_test_points(n_points)
    payload = json.dumps({"lat": lats, "lon": lons}).encode('utf-8')

    req = urllib.request.Request(
        f"{BASE_URL}/classify_points",
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/vnd.apache.arrow.stream"
        }
    )

    start = time.time()
    with urllib.request.urlopen(req, timeout=300) as response:
        data = response.read()
    elapsed = time.time() - start

    throughput = n_points / elapsed
    print(f"\nArrow IPC Response ({n_points:,} points):")
    print(f"  Total time: {elapsed*1000:.1f}ms")
    print(f"  Throughput: {throughput:,.0f} pts/sec")
    print(f"  Response size: {len(data):,} bytes")
    print(f"  Bytes per point: {len(data)/n_points:.2f}")

    return elapsed, data


def run_full_benchmark():
    """Run comprehensive benchmark."""
    print("=" * 60)
    print("Truck State Classification Microservice Benchmark")
    print("=" * 60)

    # Check service health
    print("\n1. Health Check")
    print("-" * 40)
    if not check_health():
        print("ERROR: Service not available. Please start the server first.")
        return

    # Warmup
    print("\n2. Warmup (1,000 points)")
    print("-" * 40)
    benchmark_json(1000, warmup=True)
    print("Warmup complete")

    # Latency test (small batches)
    print("\n3. Latency Test")
    print("-" * 40)
    for n in [10, 100, 1000]:
        elapsed, _ = benchmark_json(n)
        print(f"  {n:,} points: {elapsed*1000:.2f}ms ({n/elapsed:,.0f} pts/sec)")

    # Throughput test (large batches)
    print("\n4. Throughput Test (JSON)")
    print("-" * 40)
    for n in [10000, 100000, 500000, 1000000]:
        try:
            benchmark_json(n)
        except Exception as e:
            print(f"  {n:,} points: ERROR - {e}")

    # Arrow IPC test
    print("\n5. Arrow IPC Test")
    print("-" * 40)
    for n in [10000, 100000, 500000]:
        try:
            benchmark_arrow(n)
        except Exception as e:
            print(f"  {n:,} points: ERROR - {e}")

    # Maximum throughput test
    print("\n6. Maximum Throughput Test (5M points)")
    print("-" * 40)
    try:
        n = 5000000
        lats, lons = generate_test_points(n)
        payload = json.dumps({"lat": lats, "lon": lons}).encode('utf-8')

        print(f"Payload size: {len(payload)/1024/1024:.1f} MB")

        req = urllib.request.Request(
            f"{BASE_URL}/classify_points",
            data=payload,
            headers={"Content-Type": "application/json"}
        )

        start = time.time()
        with urllib.request.urlopen(req, timeout=600) as response:
            result = json.load(response)
        elapsed = time.time() - start

        throughput = n / elapsed
        print(f"\nResults:")
        print(f"  Points: {n:,}")
        print(f"  Time: {elapsed:.2f}s")
        print(f"  Throughput: {throughput:,.0f} pts/sec")
        print(f"  Cache hits: {result['metadata']['cache_hits']:,}")

    except Exception as e:
        print(f"ERROR: {e}")

    print("\n" + "=" * 60)
    print("Benchmark Complete")
    print("=" * 60)


if __name__ == '__main__':
    run_full_benchmark()
