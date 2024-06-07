# Spatial Covariate Processing

The scripts in this folder handle processing of spatial/environmental co-variates of likely interest. Specific script explanations are included below but the broad strokes is that these scripts extract spatial data (e.g., raster, netCDF, etc.) from within catchment polygons (preserved as GeoJSONs).

Note that _these scripts take a <u>long</u> time to run on a standard computer._ For best results, **run these scripts in a server or cloud environment with greater computing power.** These scripts were developed using the Aurora server at the National Center for Ecological Analysis and Synthesis ([NCEAS](https://www.nceas.ucsb.edu/)).

## Script Explanations

- `extract_...` - Extract climate data (for the variable defined in the `...` part of the script name) within site polygons (provided as a GeoJSON)
    - Data product name and information link is provided in the first few lines of the respective script
    - Note that `extract-elevation.R` handles elevation _and_ basin slope information as both require a DEM (digital elevation model)
