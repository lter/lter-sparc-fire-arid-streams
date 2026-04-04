#!/usr/bin/env Rscript

#' @title Build NSF Fire-Stream Figure
#' @description
#' End-to-end map builder for the NSF wildfire-stream figure with local MTBS
#' support, stream order >= 5 hydrology, major city overlays, major river
#' labels, and clipping north of a configurable latitude threshold.
invisible(NULL)

options(stringsAsFactors = FALSE)

required_pkgs <- c(
  "sf",
  "dplyr",
  "readr",
  "tibble",
  "purrr",
  "stringr",
  "lubridate",
  "ggplot2",
  "ggspatial",
  "scales",
  "terra",
  "cowplot",
  "maps",
  "ggrepel"
)

missing_pkgs <- required_pkgs[
  !vapply(required_pkgs, requireNamespace, logical(1), quietly = TRUE)
]
if (length(missing_pkgs) > 0) {
  install.packages(missing_pkgs, repos = "https://cloud.r-project.org")
}

sf::sf_use_s2(TRUE)

map_crs <- 4326
hydro_streamorder_min <- 5
map_west_lon <- suppressWarnings(as.numeric(Sys.getenv("MAP_WEST_LON", "-124.50")))
map_east_lon <- suppressWarnings(as.numeric(Sys.getenv("MAP_EAST_LON", "-101.75")))
map_south_lat <- suppressWarnings(as.numeric(Sys.getenv("MAP_SOUTH_LAT", "31.33")))
map_north_lat <- suppressWarnings(as.numeric(Sys.getenv("MAP_NORTH_LAT", "38.83")))
map_right_gutter_frac <- suppressWarnings(as.numeric(Sys.getenv("MAP_RIGHT_GUTTER_FRAC", "0.18")))
map_clip_lat <- suppressWarnings(as.numeric(Sys.getenv("MAP_CLIP_LAT", as.character(map_north_lat))))
if (is.na(map_clip_lat)) {
  map_clip_lat <- map_north_lat
}

if (any(is.na(c(map_west_lon, map_east_lon, map_south_lat, map_north_lat)))) {
  stop("MAP_* extent environment values must be numeric.")
}
if (is.na(map_right_gutter_frac) || map_right_gutter_frac < 0) {
  map_right_gutter_frac <- 0.18
}

project_root <- "."
cache_dir <- file.path(project_root, "data_cache")
outputs_dir <- file.path(project_root, "outputs")
if (!dir.exists(outputs_dir)) {
  dir.create(outputs_dir, recursive = TRUE)
}

catch_path <- file.path(project_root, "catch.geojson")
states_cache_path <- file.path(cache_dir, "conus_states_2024_epsg5070.gpkg")
states_cache_layer <- "states"
hillshade_cache_path <- file.path(cache_dir, "hillshade_bbox_epsg5070.tif")
hydro_cache_path <- file.path(
  cache_dir,
  paste0("nhdplus_flowlines_so", hydro_streamorder_min, "_bbox.gpkg")
)
hydro_cache_layer <- paste0("flowlines_so", hydro_streamorder_min)

resolve_hydro_source <- function(cache_dir) {
  candidates <- tibble::tibble(
    path = c(
      file.path(cache_dir, "nhdplus_flowlines_so5_bbox.gpkg"),
      file.path(cache_dir, "nhdplus_flowlines_so4_bbox.gpkg"),
      file.path(cache_dir, "nhdplus_flowlines_so7_bbox.gpkg")
    ),
    layer = c("flowlines_so5", "flowlines_so4", "flowlines_so7")
  )

  existing <- candidates |>
    dplyr::mutate(exists = purrr::map_lgl(.data$path, file.exists)) |>
    dplyr::filter(.data$exists)

  if (nrow(existing) == 0) {
    return(NULL)
  }

  existing |>
    dplyr::slice(1)
}

read_local_mtbs <- function() {
  explicit <- Sys.getenv("MTBS_SHP_PATH", unset = "")
  candidates <- c(
    file.path(project_root, "mtbs_perims_DD.shp"),
    "/home/srearl/Desktop/fire_proposal_map/mtbs_perims_DD.shp",
    explicit
  )

  mtbs_path <- candidates |>
    purrr::discard(~ is.na(.x) || !nzchar(.x)) |>
    purrr::keep(file.exists) |>
    purrr::pluck(1, .default = NA_character_)

  if (is.na(mtbs_path)) {
    stop(
      paste(
        "MTBS shapefile not found.",
        "Set MTBS_SHP_PATH or place mtbs_perims_DD.shp locally,",
        "for example under /home/srearl/Desktop/fire_proposal_map/."
      )
    )
  }

  sf::st_read(mtbs_path, quiet = TRUE)
}

ensure_caches <- function() {
  if (!file.exists(states_cache_path) || !file.exists(hillshade_cache_path)) {
    message("Basemap cache missing. Running download_basemap_cache.R ...")
    code <- system2("Rscript", "download_basemap_cache.R")
    if (!identical(code, 0L)) {
      stop("download_basemap_cache.R failed.")
    }
  }

  hydro_source <- resolve_hydro_source(cache_dir)
  if (is.null(hydro_source)) {
    message("Hydrology cache missing. Running download_hydrology_cache.R ...")
    code <- system2("Rscript", "download_hydrology_cache.R")
    if (!identical(code, 0L)) {
      stop("download_hydrology_cache.R failed.")
    }
  }
}

normalize_fire_dates <- function(mtbs) {
  mtbs |>
    dplyr::mutate(
      Ig_Date = suppressWarnings(lubridate::ymd(.data$Ig_Date)),
      year = lubridate::year(.data$Ig_Date),
      decade = floor(.data$year / 10) * 10,
      decade_label = dplyr::if_else(
        is.na(.data$decade),
        "Unknown",
        paste0(.data$decade, "s")
      )
    )
}

build_clip_bbox <- function(catch_union_wgs84, clip_lat) {
  catch_bbox <- sf::st_bbox(catch_union_wgs84)
  if (clip_lat <= catch_bbox[["ymin"]]) {
    stop("MAP_CLIP_LAT is below the catchment extent; no mappable area remains.")
  }

  clip_bbox <- catch_bbox
  clip_bbox[["ymax"]] <- min(catch_bbox[["ymax"]], clip_lat)
  sf::st_as_sfc(clip_bbox)
}

clip_and_project_layers <- function(catch, fires, streams, states, clip_lat) {
  catch_wgs84 <- catch |>
    sf::st_make_valid() |>
    sf::st_transform(4326)

  catch_union <- catch_wgs84 |>
    sf::st_union() |>
    sf::st_make_valid()

  clip_bbox <- build_clip_bbox(catch_union, clip_lat)

  clip_mask <- suppressWarnings(sf::st_intersection(catch_union, clip_bbox))
  clip_mask <- sf::st_make_valid(clip_mask)

  fires_wgs84 <- fires |>
    sf::st_make_valid() |>
    sf::st_transform(4326)

  streams_wgs84 <- streams |>
    sf::st_make_valid() |>
    sf::st_transform(4326)

  states_wgs84 <- states |>
    sf::st_make_valid() |>
    sf::st_transform(4326)

  fires_clip <- suppressWarnings(sf::st_intersection(fires_wgs84, clip_mask))
  # Keep streams fully unclipped; display clipping is handled by coord_sf limits.
  streams_clip <- streams_wgs84
  catch_clip <- suppressWarnings(sf::st_intersection(catch_union, clip_mask))

  list(
    clip_mask = sf::st_transform(clip_mask, map_crs),
    catch_clip = sf::st_transform(catch_clip, map_crs),
    fires_clip = sf::st_transform(fires_clip, map_crs),
    streams_clip = sf::st_transform(streams_clip, map_crs),
    states = sf::st_transform(states_wgs84, map_crs)
  )
}

build_major_cities <- function(clip_mask_5070) {
  clip_bbox <- sf::st_as_sfc(sf::st_bbox(clip_mask_5070))

  cities <- maps::us.cities |>
    dplyr::as_tibble() |>
    dplyr::filter(.data$pop >= 300000) |>
    dplyr::filter(!.data$name %in% c("Long Beach CA", "Mesa AZ", "Santa Ana CA", "Anaheim CA", "Riverside CA")) |>
    dplyr::filter(
      !stringr::str_detect(.data$name, "\\sCA$") |
        .data$name %in% c("Fresno CA", "Los Angeles CA", "San Diego CA")
    ) |>
    dplyr::transmute(city = .data$name, population = .data$pop, long = .data$long, lat = .data$lat)

  cities_sf <- sf::st_as_sf(cities, coords = c("long", "lat"), crs = 4326) |>
    sf::st_transform(map_crs)

  inside <- suppressWarnings(sf::st_intersection(cities_sf, clip_bbox))

  inside |>
    dplyr::arrange(dplyr::desc(.data$population)) |>
    dplyr::slice_head(n = 10)
}

build_major_river_labels <- function(streams_clip_5070) {
  named_streams <- streams_clip_5070 |>
    dplyr::mutate(
      gnis_name = stringr::str_trim(as.character(.data$gnis_name)),
      lengthkm = suppressWarnings(as.numeric(.data$lengthkm))
    ) |>
    dplyr::filter(
      !is.na(.data$gnis_name),
      .data$gnis_name != "",
      !stringr::str_detect(.data$gnis_name, "^Little\\s")
    )

  if (nrow(named_streams) == 0) {
    return(named_streams[0, ])
  }

  length_ranked_names <- named_streams |>
    sf::st_drop_geometry() |>
    dplyr::group_by(.data$gnis_name) |>
    dplyr::summarise(total_length_km = sum(.data$lengthkm, na.rm = TRUE), .groups = "drop") |>
    dplyr::arrange(dplyr::desc(.data$total_length_km)) |>
    dplyr::slice_head(n = 4)

  priority_names <- named_streams |>
    sf::st_drop_geometry() |>
    dplyr::group_by(.data$gnis_name) |>
    dplyr::summarise(total_length_km = sum(.data$lengthkm, na.rm = TRUE), .groups = "drop") |>
    dplyr::filter(
      stringr::str_detect(.data$gnis_name, stringr::regex("^Rio\\s+Grande$", ignore_case = TRUE)) |
        stringr::str_detect(.data$gnis_name, stringr::regex("^Green(\\s+River)?$", ignore_case = TRUE)) |
        stringr::str_detect(.data$gnis_name, stringr::regex("^Sacramento(\\s+River)?$", ignore_case = TRUE))
    )

  major_names <- dplyr::bind_rows(length_ranked_names, priority_names) |>
    dplyr::distinct(.data$gnis_name, .keep_all = TRUE)

  major_lines <- named_streams |>
    dplyr::semi_join(major_names, by = "gnis_name") |>
    dplyr::group_by(.data$gnis_name) |>
    dplyr::summarise(.groups = "drop")

  major_lines |>
    suppressWarnings(sf::st_centroid()) |>
    dplyr::select("gnis_name")
}

build_hillshade_df <- function(hillshade_path, clip_mask_5070, states_5070) {
  hill <- terra::rast(hillshade_path)
  us_outline <- states_5070 |>
    dplyr::summarise()
  hill_mask <- terra::vect(us_outline)
  clip_ext <- terra::ext(sf::st_bbox(clip_mask_5070))
  hill_crop <- terra::crop(hill, clip_ext)
  hill_masked <- terra::crop(hill_crop, hill_mask, mask = TRUE)

  hill_df <- as.data.frame(hill_masked, xy = TRUE, na.rm = TRUE)
  names(hill_df) <- c("x", "y", "hill")
  hill_df
}

build_fire_counts <- function(fires_clip_5070) {
  fires_clip_5070 |>
    sf::st_drop_geometry() |>
    dplyr::count(.data$decade_label, name = "n", sort = TRUE) |>
    dplyr::rename(decade = "decade_label")
}

write_diagnostics <- function(
  catch,
  fires_raw,
  fires_clip,
  streams_raw,
  streams_clip,
  clip_mask,
  mtbs_path_used
) {
  catch_bbox <- sf::st_bbox(sf::st_transform(sf::st_union(catch), 4326))

  diagnostics <- tibble::tibble(
    metric = c(
      "catch_features",
      "catch_bbox_xmin",
      "catch_bbox_ymin",
      "catch_bbox_xmax",
      "catch_bbox_ymax",
      "map_clip_lat",
      "mtbs_source_path",
      "mtbs_total_features",
      "mtbs_selected_features",
      "streams_downloaded_features",
      "streams_selected_features",
      "clip_area_sqkm"
    ),
    value = c(
      nrow(catch),
      catch_bbox[["xmin"]],
      catch_bbox[["ymin"]],
      catch_bbox[["xmax"]],
      catch_bbox[["ymax"]],
      map_clip_lat,
      mtbs_path_used,
      nrow(fires_raw),
      nrow(fires_clip),
      nrow(streams_raw),
      nrow(streams_clip),
      round(as.numeric(sf::st_area(sf::st_union(clip_mask))) / 1e6, 2)
    )
  )

  readr::write_csv(diagnostics, file.path(outputs_dir, "nsf_fire_stream_diagnostics.csv"))
}

build_plots <- function(
  states,
  fires_clip,
  streams_clip,
  major_cities,
  river_labels,
  clip_mask
) {
  clip_bbox <- sf::st_bbox(clip_mask)
  x_span <- as.numeric(clip_bbox[["xmax"]] - clip_bbox[["xmin"]])
  y_span <- as.numeric(clip_bbox[["ymax"]] - clip_bbox[["ymin"]])

  city_label_jigger <- suppressWarnings(as.numeric(Sys.getenv("CITY_LABEL_JIGGER", "1.75")))
  if (is.na(city_label_jigger) || city_label_jigger <= 0) {
    city_label_jigger <- 1.75
  }

  # fonts
  city_font_size <- suppressWarnings(as.numeric(Sys.getenv("CITY_FONT_SIZE", "4.0")))
  if (is.na(city_font_size) || city_font_size <= 0) {
    city_font_size <- 3.0
  }

  river_font_size <- suppressWarnings(as.numeric(Sys.getenv("RIVER_FONT_SIZE", "4.0")))
  if (is.na(river_font_size) || river_font_size <= 0) {
    river_font_size <- 3.0
  }

  legend_title_size <- suppressWarnings(as.numeric(Sys.getenv("LEGEND_TITLE_SIZE", "16")))
  if (is.na(legend_title_size) || legend_title_size <= 0) {
    legend_title_size <- 16
  }

  legend_text_size <- suppressWarnings(as.numeric(Sys.getenv("LEGEND_TEXT_SIZE", "14")))
  if (is.na(legend_text_size) || legend_text_size <= 0) {
    legend_text_size <- 14
  }

  city_offsets <- tibble::tribble(
    ~city, ~label_dx, ~label_dy,
    "Los Angeles CA", 0.30, 0.12,
    "San Diego CA", 0.29, 0.13,
    "Phoenix AZ", -0.25, -0.20,
    "Tucson AZ", -0.05, -0.01,
    "Las Vegas NV", -0.45, -0.22,
    "El Paso TX", 0.24, 0.17,
    "Albuquerque NM", -0.70, 0.18,
    "Denver CO", 0.18, 0.16,
    "Colorado Springs CO", 0.18, 0.13,
    "Fresno CA", -0.30, 0.18
  )

  city_labels <- major_cities |>
    dplyr::mutate(
      x = sf::st_coordinates(.data$geometry)[, 1],
      y = sf::st_coordinates(.data$geometry)[, 2]
    ) |>
    sf::st_drop_geometry() |>
    dplyr::arrange(dplyr::desc(.data$population)) |>
    dplyr::mutate(city_rank = dplyr::row_number()) |>
    dplyr::left_join(city_offsets, by = "city") |>
    dplyr::mutate(
      symbol_jx = ((.data$city_rank - 1) %% 3 - 1) * (x_span * 0.012) * city_label_jigger,
      symbol_jy = (((.data$city_rank - 1) %% 4) - 1.5) * (y_span * 0.010) * city_label_jigger,
      symbol_x = .data$x,
      symbol_y = .data$y,
      label_dx = dplyr::coalesce(.data$label_dx, 0.30) * city_label_jigger,
      label_dy = dplyr::coalesce(.data$label_dy, 0.32) * city_label_jigger
    )

  # Lock the map to requested geographic extents while retaining right-side gutter.
  x_core_limits <- c(map_west_lon, map_east_lon)
  y_limits <- c(map_south_lat, map_north_lat)
  x_right_pad <- as.numeric(diff(x_core_limits) * map_right_gutter_frac)
  x_limits <- c(x_core_limits[[1]], x_core_limits[[2]] + x_right_pad)
  core_bbox <- sf::st_as_sfc(sf::st_bbox(c(
    xmin = x_core_limits[[1]],
    ymin = y_limits[[1]],
    xmax = x_core_limits[[2]],
    ymax = y_limits[[2]]
  ), crs = sf::st_crs(clip_mask)))

  states_plot <- suppressWarnings(sf::st_intersection(states, core_bbox))
  fires_plot <- suppressWarnings(sf::st_intersection(fires_clip, core_bbox))
  streams_plot <- suppressWarnings(sf::st_intersection(streams_clip, core_bbox))
  river_labels_plot <- suppressWarnings(sf::st_intersection(river_labels, core_bbox))

  city_labels <- city_labels |>
    dplyr::filter(.data$symbol_x <= x_core_limits[[2]], .data$symbol_x >= x_core_limits[[1]])

  decade_breaks <- fires_clip |>
    sf::st_drop_geometry() |>
    dplyr::pull(.data$decade) |>
    as.numeric() |>
    unique() |>
    sort()

  decade_breaks <- decade_breaks[!is.na(decade_breaks)]
  if (length(decade_breaks) == 0) {
    decade_breaks <- NULL
  }

  main_plot <- ggplot2::ggplot() +
    ggplot2::geom_sf(
      data = states_plot,
      fill = "grey95",
      color = "grey70",
      linewidth = 0.2
    ) +
    ggplot2::geom_sf(
      data = fires_plot,
      ggplot2::aes(fill = .data$decade),
      color = NA,
      alpha = 0.75
    ) +
    ggplot2::scale_fill_gradient(
      low = "#ffd92f",
      high = "#d7191c",
      na.value = "grey70",
      name = "Fire Decade",
      breaks = decade_breaks,
      labels = if (is.null(decade_breaks)) {
        ggplot2::waiver()
      } else {
        paste0(decade_breaks, "s")
      },
      guide = ggplot2::guide_colorbar(
        title.position = "top",
        title.hjust = 0.5,
        barheight = grid::unit(70, "pt")
      )
    ) +
    ggplot2::geom_sf(
      data = streams_plot,
      color = "#2b8cbe",
      linewidth = 0.26,
      alpha = 0.9
    ) +
    ggplot2::geom_point(
      data = city_labels,
      ggplot2::aes(
        x = .data$symbol_x,
        y = .data$symbol_y
      ),
      color = "#000000",
      fill = "#000000",
      shape = 21,
      stroke = 0.2,
      size = 2
    ) +
    ggrepel::geom_text_repel(
      data = city_labels,
      ggplot2::aes(
        x = .data$symbol_x,
        y = .data$symbol_y,
        label = .data$city
      ),
      nudge_x = city_labels$label_dx,
      nudge_y = city_labels$label_dy,
      seed = 123,
      force = 2.2 * city_label_jigger,
      box.padding = 0.45,
      point.padding = 0.35,
      max.overlaps = Inf,
      min.segment.length = 0,
      segment.color = "#666666",
      segment.alpha = 0.75,
      segment.size = 0.25,
      color = "#222222",
      size = city_font_size,
      family = "sans",
      fontface = "bold"
    ) +
    ggplot2::geom_sf_text(
      data = river_labels_plot,
      ggplot2::aes(label = .data$gnis_name),
      color = "#08589e",
      size = river_font_size,
      fontface = "bold",
      check_overlap = TRUE
    ) +
    ggspatial::annotation_north_arrow(
      location = "bl",
      which_north = "true",
      style = ggspatial::north_arrow_orienteering(
        line_col = "black",
        fill = c("white", "black")
      ),
      height = grid::unit(0.8, "cm"),
      width = grid::unit(0.8, "cm"),
      pad_x = grid::unit(0.35, "cm"),
      pad_y = grid::unit(1.05, "cm")
    ) +
    ggspatial::annotation_scale(
      location = "bl",
      text_col = "black",
      line_col = "black",
      bar_cols = c("grey15", "white"),
      unit_category = "metric",
      pad_x = grid::unit(0.35, "cm"),
      pad_y = grid::unit(0.25, "cm")
    ) +
    ggplot2::coord_sf(xlim = x_limits, ylim = y_limits, expand = FALSE) +
    ggplot2::labs(
      title = NULL,
      subtitle = NULL,
      x = NULL,
      y = NULL,
      caption = NULL
    ) +
    ggplot2::theme_minimal(base_size = 10) +
    ggplot2::theme(
      panel.grid.major = ggplot2::element_line(
        color = "grey92",
        linewidth = 0.2
      ),
      panel.grid.minor = ggplot2::element_blank(),
      # put legend inside panel near right edge
      # legend.position.inside = c(0.90, 0.30),
      legend.position = c(0.975, 0.30),
      legend.justification = c(1, 0.5),
      # Optional: tighten internal legend spacing
      # legend.margin = ggplot2::margin(2, 2, 2, 2),
      # legend.box.margin = ggplot2::margin(0, 0, 0, 0),
      # legend.position = "right",
      # legend.title = ggplot2::element_text(size = legend_title_size),
      legend.title = ggplot2::element_text(
        size = legend_title_size,
        margin = ggplot2::margin(b = 8)
      ),
      legend.text = ggplot2::element_text(size = legend_text_size),
      axis.title = ggplot2::element_blank(),
      axis.text = ggplot2::element_blank(),
      axis.ticks = ggplot2::element_blank(),
      plot.title = ggplot2::element_text(face = "bold"),
      plot.background = ggplot2::element_rect(fill = "white", color = NA)
    )

  us_outline <- states |>
    dplyr::summarise()

  inset_bbox <- sf::st_as_sfc(sf::st_bbox(c(
    xmin = x_core_limits[[1]],
    ymin = y_limits[[1]],
    xmax = x_core_limits[[2]],
    ymax = y_limits[[2]]
  ), crs = sf::st_crs(clip_mask)))

  inset_plot <- ggplot2::ggplot() +
    ggplot2::geom_sf(data = us_outline, fill = "grey92", color = "grey50", linewidth = 0.35) +
    ggplot2::geom_sf(data = inset_bbox, fill = NA, color = "#d95f0e", linewidth = 0.7) +
    ggplot2::coord_sf(expand = FALSE) +
    ggplot2::theme_void() +
    ggplot2::theme(
      panel.background = ggplot2::element_rect(fill = "white", color = "grey60", linewidth = 0.3),
      plot.background = ggplot2::element_rect(fill = "white", color = NA)
    )

  cowplot::ggdraw(main_plot) +
    # increase x to move right, increase y to move up
    cowplot::draw_plot(inset_plot, x = 0.78, y = 0.50, width = 0.20, height = 0.20)
}

ensure_caches()

hydro_source <- resolve_hydro_source(cache_dir)
if (is.null(hydro_source)) {
  stop("No hydrology cache found after cache setup.")
}

catch <- sf::st_read(catch_path, quiet = TRUE)
states <- sf::st_read(states_cache_path, layer = states_cache_layer, quiet = TRUE)
streams_raw <- sf::st_read(hydro_source$path[[1]], layer = hydro_source$layer[[1]], quiet = TRUE) |>
  dplyr::mutate(streamorde = suppressWarnings(as.numeric(.data$streamorde))) |>
  dplyr::filter(.data$streamorde >= hydro_streamorder_min)

mtbs_candidates <- c(
  file.path(project_root, "mtbs_perims_DD.shp"),
  "/home/srearl/Desktop/fire_proposal_map/mtbs_perims_DD.shp",
  Sys.getenv("MTBS_SHP_PATH", unset = "")
) |>
  purrr::discard(~ is.na(.x) || !nzchar(.x))

mtbs_path_used <- mtbs_candidates |>
  purrr::keep(file.exists) |>
  purrr::pluck(1, .default = NA_character_)

fires_raw <- read_local_mtbs() |> normalize_fire_dates()

clipped <- clip_and_project_layers(
  catch = catch,
  fires = fires_raw,
  streams = streams_raw,
  states = states,
  clip_lat = map_clip_lat
)

fires_clip <- clipped$fires_clip
streams_clip <- clipped$streams_clip
catch_clip <- clipped$catch_clip
clip_mask <- clipped$clip_mask
states_5070 <- clipped$states

major_cities <- build_major_cities(clip_mask)
river_labels <- build_major_river_labels(streams_clip)

fire_counts <- build_fire_counts(fires_clip)
readr::write_csv(fire_counts, file.path(outputs_dir, "fire_counts_by_decade.csv"))

write_diagnostics(
  catch = catch,
  fires_raw = fires_raw,
  fires_clip = fires_clip,
  streams_raw = streams_raw,
  streams_clip = streams_clip,
  clip_mask = clip_mask,
  mtbs_path_used = mtbs_path_used
)

figure <- build_plots(
  states = states_5070,
  fires_clip = fires_clip,
  streams_clip = streams_clip,
  major_cities = major_cities,
  river_labels = river_labels,
  clip_mask = clip_mask
)

ggplot2::ggsave(
  filename = file.path(outputs_dir, "nsf_fire_stream_figure.png"),
  plot = figure,
  width = 11,
  height = 8.5,
  dpi = 300
)

message("Figure build complete.")
message("Figure: outputs/nsf_fire_stream_figure.png")
message("Diagnostics: outputs/nsf_fire_stream_diagnostics.csv")
message("Decade counts: outputs/fire_counts_by_decade.csv")
