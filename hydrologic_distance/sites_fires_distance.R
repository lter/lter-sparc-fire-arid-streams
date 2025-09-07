source("~/Documents/localSettings/pg_local.R")
pg <- pg_local_connect("wildfire_dev")

largest_fire_queries <- purrr::map(
  .x = c("nitrate","ammonium","orthop","spcond"),
  .f = ~ glue::glue("
      SELECT
        '{.x}' AS analyte,
        usgs_site,
        year,
        unnest AS event,
        ordinality AS event_index,
        start_date,
        end_date,
        cum_fire_area
      FROM firearea.largest_{.x}_valid_fire_per_site
      CROSS JOIN LATERAL unnest(events) WITH ORDINALITY
    ")
)

result <- purrr::map_dfr(
  .x = largest_fire_queries,
  .f = ~ DBI::dbGetQuery(pg, .x)
)

distances <- readr::read_csv("combined_distances.csv")

join <- dplyr::left_join(
  result,
  distances,
  by = c(
    "usgs_site" = "usgs_site",
    "event" = "event_id"
  )
)

# only able to calculate distance for ~36% of fire*catchment
# distance=='NA'	count	percent	histogram
# True  558	63.12	■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
# False	326	36.88	■■■■■■■■■■■■■■■■■■■■■■