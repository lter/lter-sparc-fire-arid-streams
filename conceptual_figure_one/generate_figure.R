#' @title Generate maps of ecoregions in north America relative to global mean
#' annual precipitation in context for Fire influence on land-water
#' interactions in aridland catchments
#'
#' @description Resources to generate figure 1 for the conceptual manuscript

# settings

font_size <- 12


# DATA: ecoregions (here using north America level II)

# data in drive: https://drive.google.com/file/d/1mL0mwaCPSLnn1zQ286ZcGCn4kdEXQJZd/view?usp=drive_link
# data in repo: data/NA_CEC_Eco_Level2.*
# data source: https://www.epa.gov/eco-research/ecoregions-north-america

ecoregions <- sf::st_read(
  dsn   = "../data/",
  layer = "NA_CEC_Eco_Level2"
)

ecoregions <- ecoregions |> 
  sf::st_make_valid(ecoregions) |> 
  sf::st_transform(crs = 4326)


# DATA: global MAP

geodata::worldclim_global(
  var  = "bioc", # bio vs bioc?
  res  = 10,
  path = "/tmp/"
)

global_map <- terra::rast("/tmp/wc2.1_10m/wc2.1_10m_bio_12.tif")

global_map_df <- terra::as.data.frame(
  x  = global_map,
  xy = TRUE
  ) |>
dplyr::rename(mm = wc2.1_10m_bio_12)


# DATA: western USA

western_states <- tigris::states() |> 
  dplyr::filter(
    grepl(
      pattern     = "nev|ore|was|cal|mex|ariz|tex|uta|col|wyo|monta|idah",
      x           = NAME,
      ignore.case = TRUE
    )
  ) |> 
  sf::st_transform(crs = 4326) |>
  dplyr::summarise()


# MAPPING: ecoregions

western_ecos <- ecoregions[sf::st_intersects(x = ecoregions, y = western_states, sparse = FALSE), ] |> 
dplyr::filter(
  !grepl(
    pattern     = "louisiana|southeastern|marine",
    x           = NA_L2NAME,
    ignore.case = TRUE
  )
)

## ecoregions metadata

# NA_L2NAME                       NA_L1NAME
# SOUTH CENTRAL SEMIARID PRAIRIES GREAT PLAINS
# WEST-CENTRAL SEMIARID PRAIRIES  GREAT PLAINS
# TAMAULIPAS-TEXAS SEMIARID PLAIN GREAT PLAINS

# COLD DESERTS                    NORTH AMERICAN DESERTS
# WARM DESERTS                    NORTH AMERICAN DESERTS
# MEDITERRANEAN CALIFORNIA        MEDITERRANEAN CALIFORNIA

# WESTERN CORDILLERA              NORTHWESTERN FORESTED MOUNTAINS
# WESTERN SIERRA MADRE PIEDMONT   SOUTHERN SEMIARID HIGHLANDS
# UPPER GILA MOUNTAINS            TEMPERATE SIERRAS

(
  western_ecos_plot <- ggplot2::ggplot() +
    ggplot2::geom_sf(
      data = western_ecos,
      ggplot2::aes(
        fill = NA_L2NAME
      )
      ) +
    ggplot2::theme_minimal() +
    ggplot2::theme(
      axis.text        = ggplot2::element_blank(),
      axis.ticks       = ggplot2::element_blank(),
      axis.title       = ggplot2::element_blank(),
      panel.grid.major = ggplot2::element_blank(),
      panel.grid.minor = ggplot2::element_blank(),
      panel.background = ggplot2::element_blank(),
      plot.margin      = ggplot2::unit(c(0, 0, 0, 0), "cm"), # TRBL
      legend.title     = ggplot2::element_blank(),
      legend.text      = ggplot2::element_text(size = font_size),
      legend.key.size  = ggplot2::unit(0.5, "cm"),
      # legend.position  = c(-0.25, 0.5) # for single column
      legend.position  = "right"
      ) +
    ggplot2::scale_fill_manual(
      breaks = c(
        # plains
        "SOUTH CENTRAL SEMIARID PRAIRIES",
        "WEST-CENTRAL SEMIARID PRAIRIES",
        "TAMAULIPAS-TEXAS SEMIARID PLAIN",
        # deserts
        "COLD DESERTS",
        "WARM DESERTS",
        # mediterranean
        "MEDITERRANEAN CALIFORNIA",
        # mountains
        "WESTERN CORDILLERA",
        "WESTERN SIERRA MADRE PIEDMONT",
        "UPPER GILA MOUNTAINS"
        ),
      values = c(
        # plains
        "#9bc4e2",
        "#bcd4e6",
        "#e7feff",
        # deserts
        "#fcb071",
        "#ec8c54",
        # mediterranean
        "#89c662",
        # mountains
        "#e9ffdb",
        "#ecebbd",
        "#f5deb3"
      )
    )
    # ggplot2::guides(
    #   fill = ggplot2::guide_legend(ncol = 2)
    # )
)


# mapping: MAP

western_ecos_outline <- sf::st_union(western_ecos) |>
  sf::st_transform(crs = 4326)

qn <- quantile(
  x     = global_map_df$mm,
  probs = c(0.0, 0.99),
  na.rm = TRUE
)

qn01 <- scales::rescale(x = c(qn, range(global_map_df$mm)))

(
  ppt <- ggplot2::ggplot() +
    ggplot2::geom_raster(
      data    = global_map_df,
      mapping = ggplot2::aes(
        x    = x,
        y    = y,
        fill = mm
      )
      ) +
    ggplot2::geom_sf(
      data      = western_ecos_outline,
      color     = "black",
      fill      = NA,
      linewidth = 0.8
      ) +
    ggplot2::coord_sf(
      expand     = FALSE
      # label_axes = "-NE-" # y-axis on the right TRBL
      ) +
    ggplot2::scale_fill_gradientn (
      colours = colorRampPalette(
        colors = c(
          # red (5)
          "#ef1628",
          "#ec8c54",
          "#fcb071",
          "#fac687",
          "#f9e9d0",
          # green (8)
          "#e3f0c2",
          "#d0e5aa",
          "#b9d69b",
          "#9dd18e",
          "#89c662",
          "#71bf40",
          "#07b24a",
          "#00a64e",
          # blue (4)
          "#928bc1",
          "#7370b3",
          "#625eaa",
          "#2b3092"
          ),
        bias = 1.3
        )(20),
      values = c(0, seq(qn01[1], qn01[2], length.out = 18), 1)
      ) +
    # ggplot2::theme_minimal() +
    ggplot2::theme(
      axis.text        = ggplot2::element_text(size = font_size),
      panel.grid.major = ggplot2::element_blank(),
      panel.grid.minor = ggplot2::element_blank(),
      panel.background = ggplot2::element_blank(),
      plot.margin      = ggplot2::unit(c(0, 0, 0, 0), "cm"), # TRBL
      legend.text      = ggplot2::element_text(size = font_size)
      # legend.position  = "none"
      ) +
    ggplot2::labs(
      x    = NULL,
      y    = NULL,
      fill = "MAP (mm)"
    )
)


# MAPPING: layout

## horizontal

(
  ecos_map <- cowplot::plot_grid(
    western_ecos_plot,
    NULL,
    ppt,
    nrow       = 1,
    ncol       = 3,
    rel_widths = c(+1.2, -0.1, +1.0)
    # rel_heights = c(1, 4)
  )
)


## vertical

(
  ecos_map <- cowplot::plot_grid(
    ppt,
    western_ecos_plot,
    nrow       = 2,
    ncol       = 1,
    labels     = c("A", "B"),
    rel_widths = c(+4.2, +1.0)
  )
)


# ggsave

ggplot2::ggsave(
  filename = "/tmp/ecoregions_MAP.jpg",
  width    = 9,
  height   = 7,
  units    = c("in")
)
