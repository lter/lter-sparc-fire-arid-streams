# general workflow for calculating hydrologic distances between fire boundaries
# and the pour point within a given catchment 

# 1. ensure that flowlines for target catchments are added to the database
# 2. export catchment, pour_point, fires_catchment, and flowline geometries as
# GeoJSON files because this analysis is addressed on the HPC that cannot access
# the database directly (see `postgres_to_geojson`)
# 3. upload GeoJSON files, `run_single_site.R`, `run_single_site.sh`,
# `submit_all_sites.sh`, and the sites file (e.g. `nitrate_sites.csv`) to the
# HPC
# 4. set file, directory, and sbatch parameters as appropriate for their
# respective names and locations
# 5. contatenate distance output
# 6. there will be failed runs, these will not derail the workflow but
# investigate problems (usually the catchment is too large) (`simple_plot` is a
# function to help visualize issues)

source("distance_helper_functions.R")
