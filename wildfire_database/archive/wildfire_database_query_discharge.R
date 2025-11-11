# Tools to query daily discharge data from the wildfire database. Easily
# accessible data are stored as an rds file in the lter-sparc-fire-arid shared
# drive on Aurora. A workflow to refresh the rds file as needed is detailed.

# query the data from rds ------------------------------------------------------

discharge_daily <- readRDS("/home/shares/lter-sparc-fire-arid/discharge_daily.rds")


# refresh the rds --------------------------------------------------------------

base_query <- "
SELECT
  *
FROM firearea.discharge_daily
WHERE
  usgs_site IN (
    SELECT usgs_site FROM firearea.ecoregion_catchments
  )
;
"

discharge_daily <- DBI::dbGetQuery(pg, base_query)

saveRDS(discharge_daily, "/home/shares/lter-sparc-fire-arid/discharge_daily.rds")
