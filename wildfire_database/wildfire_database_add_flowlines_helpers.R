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
#' Compares the set of firearea.fires_catchments sites against sites that
#' already have flowlines in the database, returning only those sites that are
#' missing flowline data. This function helps identify which sites need new
#' flowlines to be generated.
#'
#' @return A data frame containing only the sites that do not have
#' corresponding flowlines in the database. If all sites have flowlines,
#' returns a data frame with zero rows. Sites that are already present will
#' generate an informational message listing the existing sites.
#'
#' @details This function first calls \code{\link{get_flowlines_sites}} to
#' retrieve sites with existing flowlines, then filters the sites to identify
#' those missing from the flowlines table. Sites that already exist are
#' reported via message for user information.
#'
#' @family flowline functions
#' @seealso \code{\link{get_flowlines_sites}} for retrieving existing flowline
#' sites
#' @export
new_flowline_sites <- function() {

  all_sites <- DBI::dbGetQuery(
    conn      = pg,
    statement = "
    SELECT DISTINCT usgs_site
    FROM firearea.fires_catchments
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

  flowline_sites <- get_flowlines_sites()

  present <- all_sites |> dplyr::filter(site_id %in% flowline_sites)
  missing <- all_sites |> dplyr::filter(!site_id %in% flowline_sites)

  if (nrow(present) > 0) {
    message(
      "chem sites already present in flowline_sites: ",
      paste(present$site_id, collapse = ", ")
    )
  }

  return(missing)

}
