# Fire Proposal Map Project Log

This log captures intent, workflow, and important edits so map refinements can be made quickly and safely over the proposal lifecycle.

## Project Goal

Create and iteratively refine a proposal-quality figure showing wildfire perimeters relative to higher-order stream networks within the study region, with readable cartography and reproducible outputs.

## Primary Workflow

1. Ensure required data caches are present or regenerated.
2. Resolve MTBS source from local path/env var.
3. Build map and diagnostics through build_nsf_fire_figure.R.
4. Review figure readability and diagnostics.
5. Apply targeted cartographic edits and re-run full build.

## Canonical Build Commands

- Hydrology cache:
  - Rscript download_hydrology_cache.R
- Basemap cache:
  - Rscript download_basemap_cache.R
- Figure build:
  - Rscript build_nsf_fire_figure.R

## Inputs, Caches, and Outputs

Inputs:
- catch.geojson (study extent)
- mtbs_perims_DD.shp (local-only)

MTBS resolution order:
1. ./mtbs_perims_DD.shp
2. /home/srearl/Desktop/fire_proposal_map/mtbs_perims_DD.shp
3. MTBS_SHP_PATH environment variable

Caches:
- data_cache/conus_states_2024_epsg5070.gpkg
- data_cache/hillshade_bbox_epsg5070.tif
- data_cache/nhdplus_flowlines_so5_bbox.gpkg (or fallback cache candidates in script)

Outputs:
- outputs/nsf_fire_stream_figure.png
- outputs/nsf_fire_stream_diagnostics.csv
- outputs/fire_counts_by_decade.csv

## Current Cartographic Configuration

- CRS: EPSG:5070
- Stream filtering: streamorde >= 5
- Fire polygons: decade gradient from Ig_Date
- Map clipping: north clipped by MAP_CLIP_LAT (default 38)
- Cities: top 10 by population in map extent
- Rivers: major named labels from gnis_name
- Inset: CONUS locator in top-right

## Runtime Tuning Knobs

- MAP_CLIP_LAT
  - Purpose: adjust northern extent clip latitude
  - Default: 38
  - Example: MAP_CLIP_LAT=39 Rscript build_nsf_fire_figure.R

- CITY_LABEL_JIGGER
  - Purpose: scales city point jitter and label repel distance/force
  - Default: 1.75
  - Example: CITY_LABEL_JIGGER=2.2 Rscript build_nsf_fire_figure.R

## Quality Checks After Each Edit

1. Parse check: Rscript -e "parse(file='build_nsf_fire_figure.R')"
2. Full build: Rscript build_nsf_fire_figure.R
3. Confirm figure readability:
   - city labels do not collide heavily
   - city labels avoid major symbol overlap where possible
   - river labels remain legible
   - legend and inset are unobstructed
4. Confirm outputs were refreshed in outputs/

## Change History

### 2026-03-20

- Added/updated end-to-end builder at build_nsf_fire_figure.R.
- Shifted hydrology threshold from stream order >= 7 to >= 5 for denser stream context.
- Added configurable map north clipping with MAP_CLIP_LAT (default 38).
- Added major city points and labels.
- Added major river labels from GNIS names.
- Updated fire style to decade gradient.
- Added MTBS local-source resolution and diagnostics fields (including selected source path and clip area).
- Increased city label separation:
  - added ggrepel dependency
  - added CITY_LABEL_JIGGER control (default 1.75)
  - added deterministic city symbol jitter and repel-based labels to reduce overlap

## Notes for Future Edits

- Prefer incremental adjustments to existing parameters before changing data selection logic.
- Preserve output filenames to avoid breaking downstream proposal assembly.
- If introducing a new aesthetic rule, expose it as an env var when practical.
- Keep this log current whenever cartographic behavior changes.
