# HELPER FUNCTIONS THAT AID IN THE HYDROLOGIC DISTANCE CALCULATION WORKFLOW BUT
# ARE NOT QUITE WORTH ADDING TO THE FIREAREA PACKAGE

#' Export Spatial Data from PostgreSQL to GeoJSON Files
#'
#' Extracts spatial data (catchments, fire-catchment intersections, flowlines, 
#' and pour points) from PostgreSQL database tables for specified chemistry 
#' sites and exports them as separate GeoJSON files. This function combines 
#' both USGS and non-USGS site data from multiple database tables.
#'
#' @param chem_sites A character vector of site identifiers to filter the 
#'   spatial data. These identifiers are used to query the database tables 
#'   and extract only the spatial features associated with the specified sites.
#' @param path A character string specifying the directory path where GeoJSON 
#'   files will be written. The path should end with a trailing slash (e.g., 
#'   "/tmp/"). Four files will be created: catchments.geojson, 
#'   fires_catchments.geojson, flowlines.geojson, and pour_points.geojson.
#'
#' @return This function is called for its side effects (writing files) and 
#'   does not return a value. Files are written to the specified path with 
#'   \code{delete_dsn = TRUE} to overwrite existing files.
#'
#' @details The function queries multiple PostgreSQL tables in the firearea 
#'   schema:
#'   \itemize{
#'     \item \code{firearea.catchments} and \code{firearea.non_usgs_catchments} 
#'           for watershed boundaries
#'     \item \code{firearea.fires_catchments} for fire-catchment intersections
#'     \item \code{firearea.flowlines} for stream network data
#'     \item \code{firearea.pour_points} and \code{firearea.non_usgs_pour_points} 
#'           for sampling locations
#'   }
#'   
#'   The function combines USGS and non-USGS data for catchments and pour 
#'   points using \code{dplyr::bind_rows()}. All spatial data is filtered 
#'   using the provided site identifiers and exported as GeoJSON format 
#'   for use in spatial analysis workflows.
#'
#' @section Database Dependencies:
#' This function requires an active PostgreSQL connection object named 
#' \code{pg} to be available in the global environment. The connection should 
#' provide access to the firearea schema with the required spatial tables.
#'
#' @section Error Handling:
#' The function will stop with an error if any of the four required spatial 
#' datasets (catchments, fires_catchments, flowlines, pour_points) return 
#' zero rows after filtering by the provided site identifiers.
#'
#' @examples
#' \dontrun{
#'
#' >head(nitrate_sites, n = 14)
#' usgs_site         site_id
#' USGS-13092747     13092747
#' USGS-10396000     10396000
#' USGS-094196783    094196783
#' USGS-06635000     06635000
#' USGS-13184000     13184000
#' USGS-11060400     11060400
#' USGS-06279500     06279500
#' USGS-08406500     08406500
#' USGS-13108150     13108150
#' USGS-08396500     08396500
#' USGS-09502000     09502000
#' USGS-09466500     09466500
#' USGS-07311783     07311783
#' sbc_lter_rat      sbc_lter_rat
#'
#' postgres_to_geojson(
#'   chem_sites = nitrate_sites$usgs_site,
#'   path       = "/tmp/"
#' )
#' }
#'
#' @family spatial export functions
#' @seealso \code{\link[sf]{st_write}} for GeoJSON writing functionality
#' @export
#'
postgres_to_geojson <- function(
  chem_sites,
  path
) {

  catchments_usgs <- sf::st_read(
    dsn = pg,
    query = glue::glue_sql(
      "SELECT * FROM firearea.catchments WHERE usgs_site IN ({ chem_sites* }) ;",
      .con = DBI::ANSI()
    ),
    geometry_column = "geometry"
  )

  catchments_other <- sf::st_read(
    dsn = pg,
    query = glue::glue_sql(
      "SELECT * FROM firearea.non_usgs_catchments WHERE usgs_site IN ({ chem_sites* }) ;",
      .con = DBI::ANSI()
    ),
    geometry_column = "geometry"
  )

  catchments <- dplyr::bind_rows(
    catchments_usgs,
    catchments_other
  )

  fires_catchments <- sf::st_read(
    dsn = pg,
    query = glue::glue_sql(
      "SELECT * FROM firearea.fires_catchments WHERE usgs_site IN ({ chem_sites* }) ;",
      .con = DBI::ANSI()
    ),
    geometry_column = "geometry"
  )

  flowlines <- sf::st_read(
    dsn = pg,
    query = glue::glue_sql(
      "SELECT * FROM firearea.flowlines WHERE usgs_site IN ({ chem_sites* }) ;",
      .con = DBI::ANSI()
    ),
    geometry_column = "geometry"
  )

  pour_points_usgs <- sf::st_read(
    dsn = pg,
    query = glue::glue_sql(
      "SELECT * FROM firearea.pour_points WHERE usgs_site IN ({ chem_sites* }) ;",
      .con = DBI::ANSI()
    ),
    geometry_column = "geometry"
  )

  pour_points_other <- sf::st_read(
    dsn = pg,
    query = glue::glue_sql(
      "SELECT * FROM firearea.non_usgs_pour_points WHERE usgs_site IN ({ chem_sites* }) ;",
      .con = DBI::ANSI()
    ),
    geometry_column = "geometry"
  )

  pour_points <- dplyr::bind_rows(
    pour_points_usgs,
    pour_points_other
  )

  if (
    nrow(catchments) == 0 ||
    nrow(fires_catchments) == 0 ||
    nrow(flowlines) == 0 ||
    nrow(pour_points) == 0
  ) {
    stop("one or more required data frames has zero rows")
  }

  sf::st_write(
    obj        = catchments,
    dsn        = paste0(path, "catchments.geojson"),
    delete_dsn = TRUE
  )

  sf::st_write(
    obj        = fires_catchments,
    dsn        = paste0(path, "fires_catchments.geojson"),
    delete_dsn = TRUE
  )

  sf::st_write(
    obj        = flowlines,
    dsn        = paste0(path, "flowlines.geojson"),
    delete_dsn = TRUE
  )

  sf::st_write(
    obj        = pour_points,
    dsn        = paste0(path, "pour_points.geojson"),
    delete_dsn = TRUE
  )

}


#' Create Interactive Map or Export Spatial Data for a Chemistry Site
#'
#' Visualizes spatial data (catchments, fire intersections, flowlines, and pour 
#' points) for a specified chemistry site either as an interactive web map using 
#' mapview or by exporting the data as GeoJSON files. The function automatically 
#' searches both USGS and non-USGS spatial datasets.
#'
#' @param chem_site A character string specifying the chemistry site identifier.
#'   The function uses pattern matching to find sites, so partial matches are 
#'   supported (case-insensitive). Examples: "08406500", "sbc_lter_rat".
#' @param interactive A logical value indicating whether to create an interactive 
#'   map (\code{TRUE}, default) or export data as GeoJSON files (\code{FALSE}).
#'   When \code{TRUE}, returns a mapview object; when \code{FALSE}, writes 
#'   GeoJSON files to /tmp/ directory.
#' @param winnow_largest_fire A logical value indicating whether to filter 
#'   fire intersections to only those from the largest valid fire per site 
#'   (\code{FALSE}, default). When \code{TRUE}, restricts fire-catchment 
#'   intersections to event IDs contained in the 
#'   \code{firearea.largest_nitrate_valid_fire_per_site} view for the specified 
#'   site. This filtering applies to both interactive and non-interactive modes.
#'
#' @return When \code{interactive = TRUE}, returns a mapview object showing 
#'   layered spatial data, or \code{invisible(NULL)} if no data is found. 
#'   When \code{interactive = FALSE}, called for side effects (file writing) 
#'   and returns nothing. Files are named with the pattern: 
#'   \code{<data_type>_<site_id>.geojson}.
#'
#' @details The function queries multiple PostgreSQL tables in the firearea 
#'   schema to retrieve spatial data for the specified site:
#'   \itemize{
#'     \item Catchments: Searches \code{firearea.catchments} first, then 
#'           \code{firearea.non_usgs_catchments} if no matches found
#'     \item Fire intersections: From \code{firearea.fires_catchments}. When 
#'           \code{winnow_largest_fire = TRUE}, restricts results to fire events 
#'           matching the largest valid fire per site from 
#'           \code{firearea.largest_nitrate_valid_fire_per_site}.
#'     \item Flowlines: From \code{firearea.flowlines}
#'     \item Pour points: Searches \code{firearea.pour_points} first, then 
#'           \code{firearea.non_usgs_pour_points} if no matches found
#'   }
#'   
#'   For interactive maps, layers are displayed in order from bottom to top: 
#'   catchments (blue, transparent), fires (orange outline), flowlines (blue), 
#'   and pour points (black dots). Only layers with data are included.
#'   
#'   The function uses case-insensitive pattern matching with wildcards, so 
#'   partial site identifiers can be used to find matches.
#'
#' @section Database Dependencies:
#' Requires an active PostgreSQL connection object named \code{pg} in the 
#' global environment with access to the firearea schema.
#'
#' @section File Output:
#' When \code{interactive = FALSE}, creates four GeoJSON files in /tmp/:
#' \itemize{
#'   \item \code{catchment_<site_id>.geojson}
#'   \item \code{fires_catchments_<site_id>.geojson}
#'   \item \code{flowlines_<site_id>.geojson}
#'   \item \code{pour_points_<site_id>.geojson}
#' }
#' Existing files are overwritten (\code{delete_dsn = TRUE}).
#'
#' @examples
#' \dontrun{
#' simple_plot("sbc_lter_rat")
#' simple_plot("08406500", interactive = FALSE)
#' simple_plot("08406500", winnow_largest_fire = TRUE)
#' }
#'
#' @family spatial visualization functions
#' @seealso \code{\link[mapview]{mapview}} for interactive mapping,
#'   \code{\link[sf]{st_write}} for GeoJSON export
#' @export
#'
simple_plot <- function(
  chem_site,
  interactive = TRUE,
  winnow_largest_fire = FALSE
  ) {

  catchment <- sf::st_read(
    dsn = pg,
    query = glue::glue_sql(
      "SELECT * FROM firearea.catchments WHERE usgs_site ~~* { paste0('%', chem_site, '%') } ;",
      .con = DBI::ANSI()
    ),
    geometry_column = "geometry"
  )

  # if no rows, try non_usgs_catchments
  if (nrow(catchment) == 0) {
    catchment <- sf::st_read(
      dsn = pg,
      query = glue::glue_sql(
        "SELECT * FROM firearea.non_usgs_catchments WHERE usgs_site ~~* { paste0('%', chem_site, '%') } ;",
        .con = DBI::ANSI()
      ),
      geometry_column = "geometry"
    )
  }

  if (!winnow_largest_fire) {
    fires_catchment <- sf::st_read(
      dsn = pg,
      query = glue::glue_sql(
        "SELECT * FROM firearea.fires_catchments WHERE usgs_site ~~* { paste0('%', chem_site, '%') } ;",
        .con = DBI::ANSI()
      ),
      geometry_column = "geometry"
    )
  } else {
    fires_catchment <- sf::st_read(
      dsn = pg,
      query = glue::glue_sql(
        "SELECT firearea.fires_catchments.*
         FROM firearea.fires_catchments
         JOIN firearea.largest_nitrate_valid_fire_per_site
           ON firearea.fires_catchments.usgs_site = firearea.largest_nitrate_valid_fire_per_site.usgs_site
         WHERE firearea.fires_catchments.usgs_site ~~* {paste0('%', chem_site, '%')}
           AND firearea.fires_catchments.event_id = ANY(firearea.largest_nitrate_valid_fire_per_site.events);",
        .con = DBI::ANSI()
      ),
      geometry_column = "geometry"
    )
  }

  flowlines <- sf::st_read(
    dsn = pg,
    query = glue::glue_sql(
      "SELECT * FROM firearea.flowlines WHERE usgs_site ~~* { paste0('%', chem_site, '%') } ;",
      .con = DBI::ANSI()
    ),
    geometry_column = "geometry"
  )

  pour_point <- sf::st_read(
    dsn = pg,
    query = glue::glue_sql(
      "SELECT * FROM firearea.pour_points WHERE usgs_site ~~* { paste0('%', chem_site, '%') } ;",
      .con = DBI::ANSI()
    ),
    geometry_column = "geometry"
  )

  # if no rows, try non_usgs_pour_points
  if (nrow(pour_point) == 0) {
    pour_point <- sf::st_read(
      dsn = pg,
      query = glue::glue_sql(
        "SELECT * FROM firearea.non_usgs_pour_points WHERE usgs_site ~~* { paste0('%', chem_site, '%') } ;",
        .con = DBI::ANSI()
      ),
      geometry_column = "geometry"
    )
  }

if (interactive == TRUE) {

  m1 <- if (nrow(catchment) > 0) {
    mapview::mapview(
      catchment,
      col.regions = "blue",
      alpha.regions = 0.1,
      lwd = 0
    )
  } else NULL
  
  m2 <- if (nrow(fires_catchment) > 0) {
    mapview::mapview(
      fires_catchment,
      color = "orange",
      col.regions = "transparent",
      lwd = 3,
      alpha.regions = 0
    )
  } else NULL
  
  m3 <- if (nrow(flowlines) > 0) {
    mapview::mapview(flowlines, color = "blue", lwd = 2)
  } else NULL
  
  m4 <- if (nrow(pour_point) > 0) {
    mapview::mapview(pour_point, color = "black", cex = 5, pch = 19)
  } else NULL

  # Combine only non-NULL mapview objects, preserving order: pour_point,
  # flowlines, fires, catchment
  layers <- list(m4, m3, m2, m1)
  layers <- layers[!sapply(layers, is.null)]
  if (length(layers) == 0) {
    message("No data to plot.")
    return(invisible(NULL))
  }
  # use reduce to add all mapview layers together
  Reduce(`+`, layers)

} else {

  sf::st_write(
    obj = catchment,
    dsn = paste0("/tmp/", "catchment_", chem_site, ".geojson"),
    delete_dsn = TRUE
  )

  sf::st_write(
    obj = fires_catchment,
    dsn = paste0("/tmp/", "fires_catchments_", chem_site, ".geojson"),
    delete_dsn = TRUE
  )

  sf::st_write(
    obj = flowlines,
    dsn = paste0("/tmp/", "flowlines_", chem_site, ".geojson"),
    delete_dsn = TRUE
  )

  sf::st_write(
    obj = pour_point,
    dsn = paste0("/tmp/", "pour_points_", chem_site, ".geojson"),
    delete_dsn = TRUE
  )
}

}
