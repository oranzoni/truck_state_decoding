FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies for geopandas
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgdal-dev \
    libgeos-dev \
    libproj-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Copy shapefile for cache building (optional, can mount as volume)
# COPY /path/to/tl_2023_us_state.shp /data/

# Expose port
EXPOSE 8444

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8444/health || exit 1

# Run server
CMD ["python", "api_server.py"]
