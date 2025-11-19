#!/usr/bin/env python3
"""
Truck State Classification Microservice
Port: 8444

Flask API server for classifying GPS coordinates to US states
using H3 spatial indexing.
"""

import time

import numpy as np
import orjson
import pyarrow as pa
import pyarrow.ipc as ipc
from flask import Flask, Response, request

from classifier import TruckStateClassifier

app = Flask(__name__)

# Global classifier instance
classifier = None


def init_classifier():
    """Initialize the classifier at startup."""
    global classifier

    print("Initializing Truck State Classifier...")
    start = time.time()

    classifier = TruckStateClassifier(resolution=9)

    elapsed = time.time() - start
    print(f"Classifier initialized in {elapsed:.2f}s")
    print(f"Cache size: {len(classifier.cache):,} cells")
    print(f"States: {len(classifier.id_to_state)}")


def orjson_response(data, status=200):
    """Create a Flask response with orjson serialization."""
    return Response(
        orjson.dumps(data),
        status=status,
        mimetype='application/json'
    )


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint."""
    return orjson_response({
        'status': 'healthy',
        'service': 'truck-state-classifier',
        'h3_resolution': classifier.resolution if classifier else None,
        'cache_size': len(classifier.cache) if classifier else 0,
        'timestamp': time.time()
    })


@app.route('/classify_points', methods=['POST'])
def classify_points():
    """
    Classify GPS coordinates to US states.

    Request JSON:
        {
            "lat": [40.7128, 34.0522, ...],
            "lon": [-74.0060, -118.2437, ...]
        }

    Response JSON:
        {
            "states": ["NY", "CA", ...],
            "metadata": {
                "total_points": 3,
                "cache_hits": 3,
                "cache_misses": 0
            },
            "performance": {
                "classification_time_sec": 0.001,
                "total_time_sec": 0.002,
                "throughput_pts_sec": 1500000
            }
        }

    Or Arrow IPC stream if Accept header includes:
        application/vnd.apache.arrow.stream
    """
    start_total = time.time()

    # Parse input
    try:
        data = orjson.loads(request.data)
        lats = np.array(data['lat'], dtype=np.float64)
        lons = np.array(data['lon'], dtype=np.float64)
    except Exception as e:
        return orjson_response({
            'error': f'Invalid input: {str(e)}'
        }, status=400)

    n_points = len(lats)

    if len(lats) != len(lons):
        return orjson_response({
            'error': 'lat and lon arrays must have same length'
        }, status=400)

    # Classify points
    start_class = time.time()
    states, cache_hits = classifier.classify(lats, lons)
    class_time = time.time() - start_class

    # Check response format
    accept = request.headers.get('Accept', 'application/json')

    if 'application/vnd.apache.arrow.stream' in accept:
        # Return Arrow IPC stream
        state_ids = classifier.states_to_ids(states)

        # Create Arrow table
        table = pa.table({
            'state_id': pa.array(state_ids, type=pa.uint8())
        })

        # Serialize to IPC stream
        sink = pa.BufferOutputStream()
        with ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)

        return Response(
            sink.getvalue().to_pybytes(),
            mimetype='application/vnd.apache.arrow.stream'
        )
    else:
        # Return JSON
        total_time = time.time() - start_total
        throughput = int(n_points / total_time) if total_time > 0 else 0

        return orjson_response({
            'states': states.tolist() if isinstance(states, np.ndarray) else states,
            'metadata': {
                'total_points': n_points,
                'cache_hits': cache_hits,
                'cache_misses': n_points - cache_hits
            },
            'performance': {
                'classification_time_sec': round(class_time, 6),
                'total_time_sec': round(total_time, 6),
                'throughput_pts_sec': throughput
            }
        })


@app.route('/state_mapping', methods=['GET'])
def state_mapping():
    """Return the state ID to abbreviation mapping."""
    return orjson_response({
        'mapping': {i: state for i, state in enumerate(classifier.id_to_state)},
        'total_states': len(classifier.id_to_state)
    })


@app.errorhandler(404)
def not_found(e):
    return orjson_response({'error': 'Not found'}, status=404)


@app.errorhandler(500)
def internal_error(e):
    return orjson_response({'error': 'Internal server error'}, status=500)


if __name__ == '__main__':
    # Initialize classifier before starting server
    init_classifier()

    print("\nStarting Truck State Classification API on port 8444...")
    print("Endpoints:")
    print("  GET  /health          - Health check")
    print("  POST /classify_points - Classify GPS points to states")
    print("  GET  /state_mapping   - Get state ID mapping")
    print()

    app.run(
        host='0.0.0.0',
        port=8444,
        threaded=True,
        debug=False
    )
