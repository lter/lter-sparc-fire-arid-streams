

## overview

Calls to functions to generate analyte-specific views and materialized
views; and exports. Functions to build views can be called individually
for each analyte or, more likely, as part of a rebuild of standardied
water chemistry and related views for each analyte.

## view: analyte_counts (summer)

``` sql
SELECT firearea.create_analyte_counts_view('orthop');
```

## m.view: largest fire

``` sql
SELECT firearea.create_largest_analyte_valid_fire_per_site_mv('orthop');
```

## export: summary all sites

Catchment and summary statistics for all sites associated with this
analyte.

See full metadata
[here](https://github.com/lter/lter-sparc-fire-arid-streams/blob/main/wildfire_database/wildfire_database_query_summaries.md#fn-export-summary-all-analyte-sites).

The query result is saved as: `{analyte}_summary.csv`

``` sql
SELECT firearea.export_analyte_summary_all_sites('orthop');
```

## export: summary largest fire sites

Catchment and summary statistics for all sites associated with this
analyte for the largest fire.

See full metadata
[here](https://github.com/lter/lter-sparc-fire-arid-streams/blob/main/wildfire_database/wildfire_database_query_summaries.md#fn-export-summary-largest-fire-sites).

The query result is saved as: `{analyte}_sites_fires.csv`

``` sql
SELECT firearea.export_analyte_largest_fire_sites('orthop'::TEXT);
```

## export: q+c all

All analyte + discharge observations. Used for testing but not analyses.

``` sql
\COPY (
SELECT
  orthop.*,
  discharge."Flow",
  discharge.quartile
FROM firearea.orthop
JOIN firearea.discharge ON (
  discharge."Date" = orthop.date
  AND discharge.usgs_site = orthop.usgs_site
)
ORDER BY
  orthop.usgs_site,
  orthop.date
) TO '/tmp/orthop_q_chem.csv' WITH CSV HEADER
;
```

## export: q+c pre-post-quartiles

synopsis:

This function generates a query (`{analyte}_q_upper_quartiles`) that
extracts paired analyte and discharge observations from USGS and
non-USGS monitoring sites focusing on the periods before and after fire.

For each site/fire window, requires: - analyte–discharge observations in
both the 3-year pre- and post-fire windows - analyte–discharge
observations must include flow quartiles 2, 3, and 4 in both windows

Full metadata
[here](https://github.com/lter/lter-sparc-fire-arid-streams/blob/main/wildfire_database/wildfire_database_query_summaries.md#export-qc-pre-post-quartiles).

The query result is saved as:
`{analyte}_discharge_data_filtered_quartiles_234.csv`

``` sql
SELECT firearea.export_analyte_q_pre_post_quartiles('orthop'::TEXT);
```

## export: q+c pre-post-quartiles largest fire

purpose:

This query extracts analyte and discharge observations from USGS
watershed sites surrounding wildfires. It isolates data for only the
largest valid fire per watershed, where validity is defined by the
presence of adequate water quality monitoring data before and after the
fire.

filtering:

- Limit analysis to only the most impactful fire per watershed, based on
  fire area (`cum_fire_area`).
- ensure fire events are sufficiently monitored, with:
  - At least 3 years of data before and after the fire.
  - Presence of streamflow across flow quartiles 2, 3, and 4 in both
    windows.

Full metadata
[here](https://github.com/lter/lter-sparc-fire-arid-streams/blob/main/wildfire_database/wildfire_database_query_summaries.md#fn-export-qc-pre-post-quartiles-largest-fire).

The query result is saved as:
`{analyte}_discharge_quartiles_234_max_fire.csv`

``` sql
SELECT firearea.export_analyte_q_pre_post_quartiles_largest_fire('orthop'::TEXT);
```

## export: q+c pre-quartiles largest fire

purpose:

This query retrieves orthop and discharge records for USGS watershed
sites (`usgs_site`) surrounding wildfires. It ensures strong sampling
coverage before the fire, requiring observations in flow quartiles 2, 3,
and 4, while placing no constraint on post-fire sampling coverage.

Full metadata
[here](https://github.com/lter/lter-sparc-fire-arid-streams/blob/main/wildfire_database/wildfire_database_query_summaries.md#export-qc-pre-quartiles-largest-fire).

The query result is saved as:
`{analyte}_discharge_before_quartiles_234_max_fire.csv`

``` sql
SELECT firearea.export_analyte_q_pre_quartiles_largest_fire('orthop'::TEXT);
```
