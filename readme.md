# High-Value Freshwater Ecosystem (HVFE) Mapping

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.7+-blue.svg)](https://www.python.org/downloads/)

This repository contains Python scripts for generating **High-Value Freshwater Ecosystem (HVFE) maps** at 30-meter resolution globally. The workflow processes geospatial data to identify and map critical freshwater ecosystems using multiple scenario-based delineation approaches.

## Data Availability

The **global HVFE maps**, delineated under both minimum and maximum scenarios at 30-meter resolution, are available on Zenodo: 

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.15338535.svg)](https://doi.org/10.5281/zenodo.15338535)

## Overview

The HVFE mapping system generates **9 global tiles** covering freshwater ecosystems worldwide. Each tile is processed independently using aligned geospatial datasets to ensure consistency and accuracy. The methodology supports both maximum and minimum delineation scenarios (with and without floodplain).

The HVFE mapping system divides the globe into 9 tiles for comprehensive coverage:

![HVFE Global Tiles Overview](hvfe_global_tiles_overview.svg)

## Input Datasets

All input datasets are aligned to the **30-meter UMD Land Cover layer** for spatial consistency:

| Dataset | Resolution | Description |
|---------|------------|-------------|
| **UMD Land Cover** | 30m | Primary land cover classification layer |
| **MERIT Hydro Stream** | 90m → 30m | Stream network (downsampled and aligned) |
| **Slope** | 90m → 30m | Terrain slope (resampled and aligned) |
| **Catchments** | 30m | drived from downscaled MERIT stream and slope  |
| **GFplain** | 90m → 30m | Global Floodplain dataset (resampled and aligned) |

> 📋 **Note**: See the associated research paper for complete methodological details and data sources.

## Core Scripts

### Scenario Delineation Scripts

#### `maximum_delineation.py` & `minimum_delineation.py`
**Purpose**: Generate HVFE maps using maximum and minimum delineation scenarios

**Inputs**:
- UMD Land Cover layer
- Stream network layer
- Catchment boundaries
- GFplain (floodplain extent)

**Output**: 
- Tile-based HVFE maps in WGS84 projection
- Classified ecosystem types with scenario-specific extents

**Usage**:

**Requirements**: JupyterHub environment with computational resources

```python
# Load and run in JupyterHub notebook cell:
exec(open('GlobalHVFE_Max_delineation.py').read())
exec(open('GlobalHVFE_Min_delineation.py').read())
```

### Country-Level Analysis Scripts

#### `country_HVFE_max.py`
**Purpose**: Generate country-level HVFE statistics and maps

**Features**:
- Uses UN country boundaries for precise clipping
- Calculates area statistics for each HVFE class per country
- Handles proper reprojection for accurate area calculations
- Outputs results in hectares

**Inputs**:
- HVFE tile outputs (from delineation scripts)
- UN country shapefile (https://www.naturalearthdata.com/downloads/10m-cultural-vectors/10m-admin-0-countries/)

**Output**:
- Country-clipped HVFE maps
- Area statistics by HVFE class per country

#### `country_UMD.py`
**Purpose**: Analyze land cover composition within HVFE classes by country

**Features**:
- Intersects UMD land cover with HVFE classifications
- Calculates detailed land cover statistics per HVFE class
- User-configurable HVFE class selection
- Area calculations in hectares

**Example Configuration**:
```python
# Extract land cover stats for floodplain areas (class 6) under maximum scenario
target_hvfe_class = 6  # Floodplain from maximum delineation
```

## Repository Structure

```
HVFE-Mapping/
├── scripts/
│   ├── maximum_delineation.py
│   ├── minimum_delineation.py
│   ├── country_HVFE_max.py
│   └── country_UMD.py
└── README.md
```


---

**Keywords**: freshwater ecosystems, geospatial analysis, conservation mapping, MERIT Hydro, land cover analysis

