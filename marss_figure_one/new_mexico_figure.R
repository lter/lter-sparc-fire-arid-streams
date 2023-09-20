#' @title Generate plot of Valles Caldera study sites, catchments, and fires
#'
#' @description Map of Valles Caldera study sites, and corresponding catchments
#' and fires within those catchments.
#'
#' @note the original idea was to have the plots of sites, catchments and fires
#' from CA and NM as two separate plots and within each would be an inset of
#' the relative location in maps of the CA and NM state boundaries,
#' respecitvely. ultimately, we opted to build the map as it came to fruition
#' with the two sites, catchment, fires maps relative to a national map of
#' precipitation. The code for the former approach was not omitted, and are
#' referenced as `inner` (sites, catchments, fires) and `outer` (state).
#'
#' @note Very cool Stevan! I also had to miss the meeting for similar reasons.
#' To your question, I'm not sure but I can see that there are 6 sites plotted
#' and it should be only these 5: EFJ, RED, RSA, SAW

# NM data ----------------------------------------------------------------------

nm_points <- sf::st_read("data/new_mexico_points.geojson") |>
  dplyr::filter(
    grepl("jemez|redondo|west", Stream, ignore.case = TRUE)
  ) |> 
  dplyr::mutate(
    Stream = dplyr::case_when(
      grepl("redondo", Stream, ignore.case = TRUE) ~ "Redondo",
      grepl("jemez", Stream, ignore.case = TRUE) ~ "Jemez",
      grepl("west", Stream, ignore.case = TRUE) ~ "San Antonio (West)",
      grepl("toledo", Stream, ignore.case = TRUE) ~ "San Antonio (Toledo)",
      TRUE ~ Stream
    ),
    sitecode = dplyr::case_when(
      grepl("redondo", Stream, ignore.case = TRUE) ~ "RED",
      grepl("jemez", Stream, ignore.case = TRUE) ~ "EFJ",
      grepl("west", Stream, ignore.case = TRUE) ~ "RSAW",
      grepl("toledo", Stream, ignore.case = TRUE) ~ "RSA",
      TRUE ~ Stream
    )
  )

nm_fires <- sf::st_read("data/new_mexico_fires.geojson") |>
  sf::st_transform(crs = 4326) |>
  dplyr::filter(
    grepl("jemez|redondo|west", location_id, ignore.case = TRUE)
  )

nm_catchments <- sf::st_read("data/new_mexico_catchments.geojson") |>
  sf::st_transform(crs = 4326) |>
  dplyr::filter(
    grepl("jemez|redondo|west", location_id, ignore.case = TRUE)
  )


# new mexico -------------------------------------------------------------------

new_mexico <- tigris::states() |> 
  dplyr::filter(grepl(pattern = "mexi", x = NAME, ignore.case = TRUE))


# new mexico bounds ------------------------------------------------------------

nm_north <- +36.1
nm_south <- +35.8
nm_west  <- -106.9
nm_east  <- -106.1


# bkgd for inner ---------------------------------------------------------------

nm_map <- ggmap::get_stamenmap(
  bbox = c(
    left   = nm_west,
    bottom = nm_south,
    right  = nm_east,
    top    = nm_north
    ),
  zoom = 10,
  maptype = "terrain-background"
)


# cities for outer -------------------------------------------------------------

nm_cities <- sf::st_read("data/USA_Major_Cities.geojson") |>
  dplyr::filter(grepl("nm", ST, ignore.case = TRUE)) |>
  dplyr::filter(grepl("^albuquerque", NAME, ignore.case = TRUE)) |>
  sf::st_transform(crs = 4326)


# new mexico outer -------------------------------------------------------------

nm_outer <- ggplot2::ggplot() + 
  ggplot2::theme_minimal(base_family = "sans") +
  ggplot2::geom_sf(data = new_mexico) +
  ggplot2::geom_sf(
    data = nm_cities,
    size = 0.5
    ) +
  ggplot2::geom_sf_text(
    data = nm_cities[nm_cities$NAME == "Albuquerque", ],
    ggplot2::aes(label = NAME),
    nudge_x = +2.5,
    nudge_y = -0.5,
    size    = +1
    ) +
  ggplot2::geom_rect(
    ggplot2::aes( 
      ymin = nm_south,
      ymax = nm_north,
      xmin = nm_west,
      xmax = nm_east
      ),
    color = "red",
    fill  = NA
    ) +
  ggplot2::labs(
    x = NULL,
    y = NULL
    ) +
  ggplot2::theme_test() + 
  ggplot2::theme(
    axis.text         = ggplot2::element_blank(),
    axis.ticks        = ggplot2::element_blank(),
    axis.ticks.length = ggplot2::unit(0, "pt"),
    axis.title        = ggplot2::element_blank()
  )


# new mexico inner -------------------------------------------------------------

nm_x_breaks <- seq(nm_west + 0.1, nm_east - 0.1, by = 0.3)
nm_y_breaks <- seq(nm_south + 0.05, nm_north - 0.05, by = 0.1)

(
  nm_inner <- ggmap::ggmap(nm_map) + 
  ggplot2::theme_minimal(base_family = "sans") +
  ggplot2::geom_sf(
    mapping     = ggplot2::aes(fill = "catchment"),
    data        = nm_catchments,
    inherit.aes = FALSE,
    show.legend = "polygon",
    color       = "black",
    linewidth   = catchment_line_width,
    alpha = 0.1
  ) +
  ggplot2::geom_sf(
    mapping     = ggplot2::aes(fill = "fire"),
    data        = nm_fires,
    inherit.aes = FALSE,
    show.legend = "polygon",
    alpha       = fires_alpha
  ) +
  ggplot2::geom_sf(
    mapping     = ggplot2::aes(fill = "outlet"),
    data        = nm_points,
    inherit.aes = FALSE,
    show.legend = "point",
    size        = outlet_size,
    colour      = outlet_color
  ) +
  ggplot2::geom_sf_text(
    mapping     = ggplot2::aes(label = sitecode),
    data        = nm_points,
    inherit.aes = FALSE,
    size        = 3,
    fontface = "bold",
    # each vectorized nudge must have a value
    nudge_x = c(
      -0.05, # RED  redondo
      -0.02, # EFJ  jemez
      -0.06  # RSAW san antonio west
    ),
    nudge_y = c(
      +0.01, # RED  redondo
      -0.02, # EFJ  jemez
      +0.01  # RSAW san antonio west
    )
  ) +
  ggplot2::labs(
    x = "Longitude",
    y = ""
  ) +
  ggplot2::scale_fill_manual(
    name   = NULL,
    values = c(
      "catchment" = NA,
      "fire"      = fires_color,
      "outlet"    = NA
    ),
    guide  = ggplot2::guide_legend(
      override.aes = list(
        shape    = c(NA, NA, 16),
        linetype = c(1, 1, 0),
        fill     = c(NA, fires_color, NA),
        colour   = c("black", fires_color, outlet_color),
        size     = c(NA, NA, 2)
      )
    )
  ) +
  ggplot2::scale_x_continuous(breaks = nm_x_breaks, expand = c(0, 0)) +
  ggplot2::scale_y_continuous(breaks = nm_y_breaks, expand = c(0, 0)) +
  ggplot2::theme(
    axis.title.x      = ggplot2::element_text(
      family = "sans",
      size   = 10
    ),
    legend.position = c(0.82, 0.20),
    legend.key.size = grid::unit(0.5, "cm"),
    axis.text.x     = ggplot2::element_text(size = 8),
    axis.text.y     = ggplot2::element_text(size = 8),
    plot.margin     = ggplot2::unit(c(0, 0, 0, 0), "cm"),
    panel.border    = ggplot2::element_rect(
      colour    = "grey",
      linewidth = 2,
      fill      = NA
    )
  )
)

# new mexico combined map ------------------------------------------------------

nm_full <- nm_inner +
patchwork::inset_element(
  p      = nm_outer,
  left   = -0.92,
  bottom = +0.4,
  right  = +1.255,
  top    = +1
) +
ggplot2::theme(
  plot.margin = ggplot2::unit(c(0, 0, 0, 0), "cm")
)
