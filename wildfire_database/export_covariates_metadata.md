# Covariate Export Metadata

Applies to analyte exports from
`firearea.export_analyte_covariates_pre_post(...)` such as
`nitrate_largest_pre_post_covariates.csv`.

All covariate joins are left joins, so covariate fields may be NULL even when
an analyte observation row is present.

| column | type | meaning | values / notes |
|---|---|---|---|
| usgs_site | text | Catchment/site identifier | USGS site code used across firearea/covariates tables |
| year | integer | Fire period year | Year associated with the largest-fire period for the row's grouping |
| start_date | date | Fire period start | Start date of grouped fire period |
| end_date | date | Fire period end | End date of grouped fire period |
| segment | text | Relative timing segment | `before` or `after` relative to the fire period |
| date | date | Water-quality sample date | Observation date for analyte/discharge record |
| value_std | numeric | Standardized analyte concentration | Unitless standardized value used in modeling/export workflow |
| flow | numeric | Discharge at observation date | From firearea.discharge Flow |
| quartile | integer | Flow quartile at observation date | Expected 1-4; lower to higher flow conditions |
| na_l3name | text | Dominant Level-3 ecoregion name | Selected as highest overlap catchment ecoregion |
| previous_end_date | date | Previous fire period end date | NULL when no prior fire period is available |
| next_start_date | date | Next fire period start date | NULL when no subsequent fire period is available |
| days_since | integer | Days since previous fire period | NULL when previous_end_date is NULL |
| days_until | integer | Days until next fire period | NULL when next_start_date is NULL |
| cum_fire_area | numeric | Cumulative burned area in grouped period | km2 |
| catch_area | numeric | Catchment area | km2 |
| cum_per_cent_burned | numeric | Percent of catchment burned in grouped period | Percent scale (0-100) |
| all_fire_area | numeric | Total burned area through period end | km2 |
| all_per_cent_burned | numeric | Total percent burned through period end | Percent scale (0-100) |
| latitude | numeric | Catchment centroid latitude | Decimal degrees |
| longitude | numeric | Catchment centroid longitude | Decimal degrees |
| elev_avg_m | numeric | Mean catchment elevation | meters |
| elev_median_m | numeric | Median catchment elevation | meters |
| elev_min_m | numeric | Minimum catchment elevation | meters |
| elev_max_m | numeric | Maximum catchment elevation | meters |
| slope_avg_deg | numeric | Mean catchment slope | degrees |
| slope_median_deg | numeric | Median catchment slope | degrees |
| slope_min_deg | numeric | Minimum catchment slope | degrees |
| slope_max_deg | numeric | Maximum catchment slope | degrees |
| lulc | text | Dominant NLCD class label for catchment | Based on largest `class_percent` per site from covariates.nlcd |
| return_interval | numeric | Fire regime return interval classification value | From `covariates.fire_classifications` where fire_classification = `return_interval` |
| regime_group | numeric | Fire regime group classification value | From `covariates.fire_classifications` where fire_classification = `regime_group` |
| veg_departure | numeric | Vegetation departure classification value | From `covariates.fire_classifications` where fire_classification = `veg_departure` |
| fire_risk | numeric | Fire risk classification value | From `covariates.fire_classifications` where fire_classification = `fire_risk` |
| avg_median_post_evi | numeric | Area-weighted post-fire EVI summary for grouped events | Weighted mean of event-level median post-fire EVI by burned area |
| evi_weight_coverage | numeric | EVI weighted coverage fraction | Weighted valid-area / total event-area used for EVI aggregation; expected 0-1 |
| avg_weighted_fsi | numeric | Area-weighted fire severity index summary for grouped events | Weighted by burned area across available severity event rows |
| fire_severity_weight_coverage | numeric | Fire severity weighted coverage fraction | Weighted valid-area / total event-area used for severity aggregation; expected 0-1 |
| events | text[] | Event IDs represented by the grouped fire period | PostgreSQL array in CSV (for example `{CA3448511986620080701}`) |
