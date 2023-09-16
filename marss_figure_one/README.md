## overview

Workflow to generate figure 1 (study site map(s)) for the *Persistent and lagged effects of wildfire on stream biogeochemistry due to intermittent precipitation in arid lands* manuscript.

The work of delineating catchments and extracting fire data was addressed in other workflows but those workflows are recreated as much as possible in `valles_caldera_fires.R` and `santa_barbara_fires.R`. Each of those workflows require MTBS data. Owing to the size of MTBS data, that resource is not included here. So as not to need to perform these workflows every time the figure is generated, their outputs are stored as geojson files in the data directory.

- [x]   One small note - this may be just a scaling thing between the SB and VC catchments, but would it be possible to make the SB stream site points a bit bigger?
- [x]   Could the fire layer be made transparent? The catchment boundaries get lost underneath the fires.
- [ ]   Could the US map be much smaller and the site maps larger?
- [ ]   Can we label each catchment with the site name used in the manuscript?
- [ ]   I wonder if it would be possible to add the fire year as well without cluttering things if the site maps were larger
- [x]   Should add MAP to the legend title for the US map
- [x]   Should "site" be something like "outlet" or "observatory"?
