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

