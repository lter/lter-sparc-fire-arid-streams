#' @title Generate plot of Santa Barbara study sites, catchments, and fires
#'
#' @description Map of SBC study sites, and corresponding catchments and fires
#' within those catchments.
#'
#' @note the original idea was to have the plots of sites, catchments and fires
#' from CA and NM as two separate plots and within each would be an inset of
#' the relative location in maps of the CA and NM state boundaries,
#' respecitvely. Ultimately, we opted to build the map as it came to fruition
#' with the two sites, catchment, fires maps relative to a national map of
#' precipitation. The code for the former approach was not omitted, and are
#' referenced as `inner` (sites, catchments, fires) and `outer` (state).
#'
#' @note yes! this looks fantastic! just as a clarification - the only sites
#' we'll be using in santa barbara are the arroyo burro (ab00), gaviota (gv01),
#' arroyo hondo (ho00), mission creek (mc06), and rattlesnake canyon (rs02)
#' sites. i'm not sure how you have the data currently, but if you need their
#' coordinates to help filter your dataset lmk. thank you so much for pulling
#' this together!
#'
#' @note usmap seems not very intuitive
#' southwest  <- usmap::plot_usmap(include = c("ca", "nm", "az"))
#' california <- usmap::plot_usmap(include = c("ca"))

# SBC data ---------------------------------------------------------------------

sbc_sites <- readr::read_csv("data/sbc_sites_streams.csv") |>
  sf::st_as_sf(
    coords = c("Lon", "Lat"),
    crs    = 4326
  ) |>
  dplyr::filter(
    sitecode %in% c("AB00", "GV01", "HO00", "MC06", "RS02")
  ) |> 
  dplyr::mutate(
    Site = stringr::str_extract(
      string = Site,
      pattern = ".+?(?=\\sCreek)"
    )
  )

sbc_fires <- sf::st_read("data/santa_barbara_fires.geojson") |>
  dplyr::filter(location_id %in% c("AB00", "GV01", "HO00", "MC06", "RS02"))

sbc_catchments <- sf::st_read("data/santa_barbara_catchments.geojson") |>
  dplyr::filter(location_id %in% c("AB00", "GV01", "HO00", "MC06", "RS02"))

mc06_fire      <- sf::st_read("data/MC06_fires.geojson")
mc06_catchment <- sf::st_read("data/MC06.geojson")

sbc_fires <- dplyr::bind_rows(
  sbc_fires,
  mc06_fire
)

sbc_catchments <- dplyr::bind_rows(
  sbc_catchments,
  mc06_catchment
)


# california -------------------------------------------------------------------

california <- tigris::states() |> 
  dplyr::filter(
    grepl(
      pattern     = "cali",
      x           = NAME,
      ignore.case = TRUE
    )
  )


# california bounds ------------------------------------------------------------

ca_north <- +34.6
ca_south <- +34.3
ca_west  <- -120.4
ca_east  <- -119.6


# bkgd for inner ---------------------------------------------------------------

ca_map <- ggmap::get_stamenmap(
  bbox = c(
    left   = ca_west,
    bottom = ca_south,
    right  = ca_east,
    top    = ca_north
    ),
  zoom = 10,
  maptype = "terrain-background"
)


# cities for outer -------------------------------------------------------------

ca_cities <- sf::st_read("data/USA_Major_Cities.geojson") |>
  dplyr::filter(grepl("CA", ST, ignore.case = TRUE)) |>
  dplyr::filter(grepl("^los angeles|santa barbara", NAME, ignore.case = TRUE)) |>
  sf::st_transform(crs = 4326)


# california outer -------------------------------------------------------------

ca_outer <- ggplot2::ggplot() + 
  ggplot2::theme_minimal(base_family = "sans") +
  ggplot2::geom_sf(data = california) +
  ggplot2::geom_sf(
    data = ca_cities,
    size = 0.5
    ) +
  ggplot2::geom_sf_text(
    data = ca_cities[ca_cities$NAME == "Los Angeles", ],
    ggplot2::aes(label = NAME),
    nudge_x = +2.0,
    nudge_y = -1.0,
    size    = +1
    ) +
  ggplot2::geom_sf_text(
    data = ca_cities[ca_cities$NAME == "Santa Barbara", ],
    ggplot2::aes(label = NAME),
    nudge_x = -1,
    nudge_y = +0.8,
    size    = +1
    ) +
  ggplot2::geom_rect(
    ggplot2::aes( 
      ymin = ca_south,
      ymax = ca_north,
      xmin = ca_west,
      xmax = ca_east
      ),
    color = "red",
    fill  = NA
    ) +
  ggplot2::labs(
    x = NULL,
    y = NULL
    ) +
  ggplot2::coord_sf(expand = FALSE) +
  ggplot2::theme_test() + 
  ggplot2::theme(
    axis.text         = ggplot2::element_blank(),
    axis.ticks        = ggplot2::element_blank(),
    axis.ticks.length = ggplot2::unit(0, "pt"),
    axis.title        = ggplot2::element_blank(),
    plot.margin       = ggplot2::margin(0, 0, 0, 0, "pt")
  )


# california inner -------------------------------------------------------------

# remove call to `inherit.aes = FALSE` and uncomment lines for ggplot instead
# of ggmap

## ggplot approach would start like...
# ca_inner <- ggplot2::ggplot() + 
#   ggplot2::geom_sf(data = california) + ...

ca_x_breaks <- seq(ca_west + 0.1,   ca_east - 0.1,   by = 0.3)
ca_y_breaks <- seq(ca_south + 0.05, ca_north - 0.05, by = 0.1)

(
  ca_inner <- ggmap::ggmap(ca_map) + 
  ggplot2::theme_minimal(base_family = "sans") +
  ggplot2::geom_sf(
    mapping     = ggplot2::aes(fill = "catchment"),
    data        = sbc_catchments,
    inherit.aes = FALSE,
    show.legend = "polygon",
    color       = "black",
    fill        = NA,
    linewidth   = catchment_line_width,
  ) +
  ggplot2::geom_sf(
    mapping     = ggplot2::aes(fill = "fire"),
    data        = sbc_fires,
    inherit.aes = FALSE,
    show.legend = "polygon",
    alpha       = fires_alpha
  ) +
  ggplot2::geom_sf(
    mapping     = ggplot2::aes(fill = "outlet"),
    data        = sbc_sites,
    inherit.aes = FALSE,
    show.legend = "point",
    size        = outlet_size,
    colour      = outlet_color
  ) +
  ggplot2::geom_sf_text(
    mapping     = ggplot2::aes(label = sitecode),
    data        = sbc_sites,
    inherit.aes = FALSE,
    size        = 3,
    fontface    = "bold",
    # each vectorized nudge must have a value
    nudge_x = c(
      -0.02, # AB00 arroyo burro
      -0.03, # GV01 gaviota
      +0.03, # HO00 arroyo hondo
      +0.05, # RS02 rattlesnake
      +0.05  # MC06 mission
    ),
    nudge_y = c(
      -0.02, # AB00 arroyo burro
      -0.02, # GV01 gaviota
      -0.02, # HO00 arroyo hondo
      -0.01, # RS02 rattlesnake
      -0.02  # MC06 mission
    )
  ) +
  ggplot2::annotate(
    geom     = "text",
    x        = -120.00,
    y        = +34.35,
    label    = "Pacific Ocean",
    fontface = "italic",
    color    = "grey22",
    size     = 4,
    family   = "serif"
  ) +
  ggplot2::labs(
    x = "Longitude",
    y = "Latitude"
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
        size     = c(NA, NA, NA),
        shape    = c(NA, NA, 16),
        linetype = c(1, 1, 0),
        fill     = c(NA, "red", NA)
      )
    )
  ) +
  ggplot2::scale_x_continuous(breaks = ca_x_breaks, expand = c(0, 0)) +
  ggplot2::scale_y_continuous(breaks = ca_y_breaks, expand = c(0, 0)) +
  ggplot2::theme(
    # setting when CA is on the left
    axis.title = ggplot2::element_blank(),
    # axis.title.x = ggplot2::element_blank(),
    # axis.title      = ggplot2::element_text(
    #   family = "sans",
    #   size   = 10
    # ),
    legend.position = "none",
    axis.text.x     = ggplot2::element_text(size = 8),
    axis.text.y     = ggplot2::element_text(size = 8),
    plot.margin     = ggplot2::unit(c(0, 0, 0, 0), "cm"),
    panel.border    = ggplot2::element_rect(
      colour    = "grey",
      linewidth = 2,
      fill      = NA
    )
    # setting when CA is on the right
    # legend.position = "none",
    # axis.title      = ggplot2::element_blank(),
    # # axis.title  = ggplot2::element_text(size = 8),
    # axis.text.x     = ggplot2::element_text(size = 6),
    # axis.text.y     = ggplot2::element_text(size = 6),
    # plot.margin     = ggplot2::unit(c(0, 0, 0, 0), "cm")
  )
)

# california combined map ------------------------------------------------------

ca_full <- ca_inner +
patchwork::inset_element(
  p      = ca_outer,
  left   = -1.03,
  bottom = +0.01,
  right  = +1.30,
  top    = +0.55
) +
ggplot2::theme(
  plot.margin = ggplot2::unit(c(0, 0, 0, 0), "cm")
)
