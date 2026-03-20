# NSF Fire Map Editing Prompt

Use this prompt as a starting point for future map revisions so each editing session preserves project intent and workflow.

## Copy/Paste Prompt

I am working in fire_proposal_map and need an incremental edit to the NSF wildfire-stream figure.

Project goals:
- Produce a proposal-quality map of wildfire perimeters and higher-order streams.
- Keep edits reproducible and script-driven through build_nsf_fire_figure.R.
- Preserve existing output contract:
  - outputs/nsf_fire_stream_figure.png
  - outputs/nsf_fire_stream_diagnostics.csv
  - outputs/fire_counts_by_decade.csv

Current map design and constraints:
- Projection: EPSG:5070.
- Stream threshold: Strahler order >= 5.
- Default north clip: MAP_CLIP_LAT=38.
- Fire polygons colored by decade from Ig_Date.
- Major cities: top 10 inside extent, labels use jitter + repel.
- Major rivers: named labels from gnis_name.
- Inset: CONUS locator in top-right.

Data and path assumptions:
- Catchment geometry: catch.geojson.
- MTBS perimeters are local-only and resolved in this order:
  1) ./mtbs_perims_DD.shp
  2) /home/srearl/Desktop/fire_proposal_map/mtbs_perims_DD.shp
  3) MTBS_SHP_PATH env var
- Hydrology and basemap caches are in data_cache/.

Requested edit for this session:
- [Describe exactly what should change]
- [Include any preference tradeoffs: readability vs density, aesthetic style, legend behavior, label count, etc.]

Editing requirements:
- Make the smallest safe code change that achieves the requested outcome.
- Keep style consistent with existing script structure and naming.
- If adding tunable behavior, expose it via environment variable with sensible default.
- Do not remove existing workflow outputs.

Validation requirements:
- Run a syntax/parse check on build_nsf_fire_figure.R.
- Run full build: Rscript build_nsf_fire_figure.R.
- Confirm the three expected outputs were regenerated.
- Summarize what changed, why, and how to tune it.

Deliverables:
- Updated code.
- Short change summary with file references.
- Any new knobs and example commands.

## Optional Tuning Commands

- Default build:
  - Rscript build_nsf_fire_figure.R
- More/less north clipping:
  - MAP_CLIP_LAT=39 Rscript build_nsf_fire_figure.R
- More/less city label separation:
  - CITY_LABEL_JIGGER=2.2 Rscript build_nsf_fire_figure.R
  - CITY_LABEL_JIGGER=1.2 Rscript build_nsf_fire_figure.R
