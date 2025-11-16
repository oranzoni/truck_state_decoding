# Routing State-Time Analysis — Multiple Pipeline Variants

This repository contains multiple experimental and production-ready pipelines for computing **per-state driving time** across long-haul truck routes in North America.

## Variants

### **1. variant_lazy_h3/**
A fast, scalable, high-accuracy pipeline using:
- Valhalla driving-time models  
- Lazy H3 caching  
- Multi-sampling along route geometry  
- Nominatim for state inference  
- Polars analytics  

**Advantages:**  
Fast, scalable, reproducible, highly accurate (98–99.8%) without real GPS timestamps.

---

### **2. variant_precise_hybrid/**
A more precise—but computationally heavier—pipeline combining:
- Valhalla geometry + times  
- Nominatim / polygon splits  
- Border-aware segmentation  
- Potential optional integration of real GPS timestamps  

**Advantages:**  
Highest precision, suitable for publication or regulated fleet analytics.

---

## How to Use
Each variant contains its own README with:
- Installation steps  
- Required services (Valhalla, Nominatim)  
- Full pipeline instructions  
- Scripts and outputs  

You can work on variants independently, compare performance and accuracy, or combine insights.

---

## License
ODbL-compliant pipeline using Valhalla + Nominatim data.

## Variant Runtime Comparison (231 Routes, Same Machine)

We benchmarked both variants on the same remote machine, processing the full set of **231 Valhalla routes** (top 22 US metro regions, all pairwise connections).

### Summary

| Variant               | Script                                                       | real time      | user time      | sys time       | Approx. routes/sec |
|-----------------------|--------------------------------------------------------------|----------------|----------------|----------------|--------------------|
| Lazy H3 + multi-sampling | `variant_lazy_h3/bin/process_routes_midpoint_lazy_reverse.py`   | **0m4.831s**   | 0m4.803s       | 0m0.275s       | **≈ 48 routes/s**  |
| Precise hybrid (dense sampling + proportional split) | `variant_precise_hybrid/bin/process_routes_precise_sampling.py` | **22m35.500s** | 3m41.193s      | 0m41.434s      | **≈ 0.17 routes/s** |

### Interpretation

- **Lazy variant**  
  - Optimized for throughput.  
  - Uses H3 midpoint logic with light multi-sampling and aggressive caching.  
  - Suitable as a **fast default** for fleet-scale analytics and as a first-pass estimate.

- **Precise hybrid variant**  
  - Optimized for **border precision and state splits**.  
  - Densely samples along each maneuver and proportionally splits Valhalla maneuver time across states.  
  - Much slower, but designed as a **high-fidelity mode** for critical corridors or QA comparisons.

The intended production pattern is:

- Run the **lazy variant** for all vehicles / all routes.
- Use the **precise hybrid** only on:
  - border-heavy routes,
  - high-value flows,
  - or as a spot-check to validate the lazy approximations.

## Lazy vs. Precise Hybrid — Accuracy Comparison

To quantify accuracy differences between the two pipelines, both variants were processed into unified analytics tables and compared state-by-state.

Key findings:

Both variants agree on all major state segments for long interstate routes.

The precise hybrid produces significantly more rows (~28k vs. ~1.4k) because it splits multi-state maneuvers proportionally based on dense sampling.

States with narrow borders (e.g., West Virginia, Maryland, Delaware) show the largest improvements in the precise hybrid, where it detects crossings that the lazy variant simplifies into a single state.

Absolute drive-time differences for most states fall within 0.2% – 2.5% of total route time.

Percent differences are largest on short segments, where even a small absolute correction yields a large relative % change.

When aggregating over all 231 routes, total drive-time per state differs only slightly between variants, confirming that:

- Lazy H3 is an excellent high-level estimator.

- Precise hybrid is the fidelity benchmark.

A CSV with the state-level comparison is included:

```lazy_vs_precise_state_totals.csv```


This file provides:

- total lazy vs precise seconds

- absolute differences

- percent differences

- sorted ordering by largest corrections

These results allow measuring where precision matters most and validate the lazy variant as a reliable approximation.

