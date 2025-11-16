# Precise Hybrid State-Time Pipeline (Valhalla + Dense Sampling)

This variant uses dense sampling along the full Valhalla route geometry and proportional time splitting per maneuver to compute near ground-truth per-state driving time, while still avoiding heavy polygon intersection.

Work in progress – see variant_lazy_h3 for the approximate, high-throughput baseline.
