# helper functions -------------------------------------------------------------

#' Get Sites with Existing Flowlines
#'
#' Retrieves a list of site IDs that already have flowlines stored in the
#' firearea.flowlines database table. This function is useful for identifying
#' which sites already have flowline data processed and which sites still need
#' flowline data to be generated.
#'
#' @return A character vector of site IDs (with "usgs-" prefix removed) that
#'   have flowlines in the database, or NULL if no sites with flowlines are found.
#'   When sites are found, USGS site identifiers are cleaned by removing the
#'   "usgs-" prefix for consistency.
#'
#' @details The function queries the firearea.flowlines table to find distinct
#'   usgs_site values. If sites are found, the "usgs-" prefix is removed from
#'   the site identifiers to create standardized site_id values.
#'
#' @family flowline functions
#' @export
get_flowlines_sites <- function() {

  flowline_sites <- DBI::dbGetQuery(
    conn      = pg,
    statement = "
    SELECT DISTINCT usgs_site
    FROM firearea.flowlines
    ;
    "
  )

  if (nrow(flowline_sites > 0)) {

    flowline_sites <- flowline_sites |>
      dplyr::mutate(
        site_id = gsub(
          pattern     = "usgs-",
          replacement = "",
          x           = usgs_site,
          ignore.case = TRUE
        )
      ) |>
      dplyr::pull(site_id)

  } else {
    message("existing flowline sites not found")
    return(NULL)
  }

  return(flowline_sites)

}


#' Identify Sites Missing Flowlines
#'
#' Compares a set of chemistry sites against sites that already have flowlines
#' in the database, returning only those sites that are missing flowline data.
#' This function helps identify which sites need new flowlines to be generated.
#'
#' @param chem_sites A data frame containing chemistry sites. Must include a
#'   'site_id' column with site identifiers that can be matched against existing
#'   flowline sites.
#'
#' @return A data frame containing only the chemistry sites that do not have
#'   corresponding flowlines in the database. If all chemistry sites already
#'   have flowlines, returns a data frame with zero rows. Sites that are already
#'   present will generate an informational message listing the existing sites.
#'
#' @details This function first calls \code{\link{get_flowlines_sites}} to
#'   retrieve sites with existing flowlines, then filters the input chemistry
#'   sites to identify those missing from the flowlines database. Sites that
#'   already exist are reported via message for user information.
#'
#' @family flowline functions
#' @seealso \code{\link{get_flowlines_sites}} for retrieving existing flowline sites
#' @export
new_flowline_sites <- function(chem_sites) {

  flowline_sites <- get_flowlines_sites()

  present <- chem_sites |> dplyr::filter(site_id %in% flowline_sites)
  missing <- chem_sites |> dplyr::filter(!site_id %in% flowline_sites)

  if (nrow(present) > 0) {
    message(
      "chem sites already present in flowline_sites: ",
      paste(present$site_id, collapse = ", ")
    )
  }

  return(missing)

}


# flowlines: nitrate sites -----------------------------------------------------

nitrate_sites <- DBI::dbGetQuery(
  conn      = pg,
  statement = "
  SELECT DISTINCT usgs_site
  FROM firearea.largest_nitrate_valid_fire_per_site
  ;
  "
) |>
  dplyr::mutate(
    site_id = gsub(
      pattern     = "usgs-",
      replacement = "",
      x           = usgs_site,
      ignore.case = TRUE
    )
  )

## add usgs site flowlines

nitrate_sites_usgs <- new_flowline_sites(chem_sites = nitrate_sites) |>
  dplyr::filter(
    grepl(
      pattern     = "usgs",
      x           = usgs_site,
      ignore.case = TRUE
    )
  )

write_usgs_flowlines_safely <- purrr::safely(firearea::write_usgs_flowlines)

results <- purrr::map(
  .x = nitrate_sites_usgs$site_id,
  .f = ~ write_usgs_flowlines_safely(
    usgs_site = .x
  )
)

### usgs sites successes and errors

successes  <- purrr::map_lgl(results, ~ is.null(.x$error))
usgs_sites <- nitrate_sites_usgs$usgs_site
result_df  <- tibble::tibble(
  usgs_site = usgs_sites,
  success   = successes,
  error     = purrr::map_chr(
    results,
    ~ if (!is.null(.x$error)) as.character(.x$error) else NA_character_
  )
)

## add non-usgs site flowlines

nitrate_sites_other <- new_flowline_sites(chem_sites = nitrate_sites) |>
  dplyr::filter(
    !grepl(
      pattern     = "usgs",
      x           = usgs_site,
      ignore.case = TRUE
    )
  )

nitrate_sites_other_geom <- sf::st_read(
  dsn   = pg,
  query = glue::glue_sql(
    "
    SELECT *
    FROM firearea.non_usgs_pour_points
    WHERE usgs_site IN ({ nitrate_sites_other$usgs_site* })
    ;
    ",
    .con = DBI::ANSI()
  ),
  geometry_column = "geometry"
)

nitrate_sites_other_geom$longitude <- sf::st_coordinates(nitrate_sites_other_geom)[, 1]
nitrate_sites_other_geom$latitude  <- sf::st_coordinates(nitrate_sites_other_geom)[, 2]

results_other <- purrr::map2(
  .x = split(nitrate_sites_other_geom, seq_len(nrow(nitrate_sites_other_geom))),
  .y = nitrate_sites_other_geom$usgs_site,
  .f = ~ write_usgs_flowlines_safely(
    location_df         = .x,
    non_usgs_identifier = .y
  )
)

### non-usgs sites successes and errors

successes_other <- purrr::map_lgl(results_other, ~ is.null(.x$error))
result_other_df <- tibble::tibble(
  usgs_site = nitrate_sites_other_geom$usgs_site,
  success   = successes_other,
  error     = purrr::map_chr(
    results_other,
    ~ if (!is.null(.x$error)) as.character(.x$error) else NA_character_
  )
)
