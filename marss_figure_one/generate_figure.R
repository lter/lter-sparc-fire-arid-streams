#' @title Generate maps of Santa Barbara and Valles Caldera study sites in
#' context for Persistent and lagged effects of wildfire on stream
#' biogeochemistry due to intermittent precipitation in arid lands manuscript
#'
#' @description Resources to generate figure 1 for the MARSS manuscript
#'
#' @note In the time since what was expected to be the final figure was
#' developed and submitting the paper, stamenmaps, which provide the background
#' layer for this figure, are now served by Stadia Maps. At the time of this
#' writing, `ggmap` included functionality to pull similar layers from Stadia
#' Maps but that functionality had not yet been exported in either the CRAN or
#' developement (i.e., GitHub) versions of `ggmap`. As such when attempting to
#' explore potential modifications to the map, the unusual steps of having to
#' clone the `ggmap` repository and load the relevant functions was required.
#' Note also that unlike stamenmaps, Stadia Maps requires an API key.

# load tools to acquire background map layers from Stadia Maps  

## Stadia Maps does require an API key
## register_stadiamaps("mykey", write = TRUE) 

## load libraries used by the `ggmap` package
library(stringr)
library(glue)
library(tibble)
library(plyr)

## source required functions from a clone of `ggmap`
source("~/localRepos/ggmap/R/LonLat2XY.R")
source("~/localRepos/ggmap/R/XY2LonLat.R")
source("~/localRepos/ggmap/R/helpers.R")
source("~/localRepos/ggmap/R/register_stadiamaps.R")
source("~/localRepos/ggmap/R/ggmap_options.R")
source("~/localRepos/ggmap/R/attach.R")
source("~/localRepos/ggmap/R/file_drawer.R")
source("~/localRepos/ggmap/R/get_stadiamap.R")


# settings

catchment_line_width <- 0.3
outlet_size          <- 1.8
outlet_color         <- "blue"
fires_alpha          <- 0.6
fires_color          <- "orange"
inset_line_color     <- "#707070"

source("sbc_figure.R")
source("new_mexico_figure.R")
source("national_precipitation_map.R")
source("plot_legends.R")


# combine CA & NM into a single plot
(
  nm_ca_vert <- cowplot::plot_grid(
    ca_inner,
    NULL,
    nm_inner,
    align       = "vh",
    nrow        = 3,
    ncol        = 1,
    rel_heights = c(
      +1.00,
      -0.50,
      +1.00
    )
  )
)

# combine CA & NM with ppt
(
  plot_with_ppt <- cowplot::plot_grid(
    nm_ca_vert,
    ppt_with_legend,
    nrow = 1,
    ncol = 2
  )
)

# add y-axis label (*latitude*) spanning both CA & NM plots
(
  plot_with_y_axis <- cowplot::ggdraw(
    plot = cowplot::add_sub(
      plot       = plot_with_ppt,
      label      = "Latitude",
      vpadding   = grid::unit(0, "lines"),
      y          = 6.500,
      x          = 0.015,
      vjust      = 0.100,
      hjust      = 0.100,
      fontfamily = "sans",
      size       = 10,
      angle      = 90
    )
  )
)

# generate boxes to overlay on the ppt map
ca_bound <- grid::rectGrob(
  x      = 0.560,
  y      = 0.515,
  width  = 0.03,
  height = 0.02,
  gp     = grid::gpar(
    col = inset_line_color,
    lwd = 2
  )
)

nm_bound <- grid::rectGrob(
  x      = 0.645,
  y      = 0.520,
  width  = 0.03,
  height = 0.02,
  gp     = grid::gpar(
    col = inset_line_color,
    lwd = 2
  )
)

plot_with_y_axis +
# line: CA
cowplot::draw_line(
  x     = c(0.50, 0.545),
  y     = c(0.60, 0.525),
  color = inset_line_color,
  size  = 1
) +
# line: NM
cowplot::draw_line(
  x     = c(0.50, 0.630),
  y     = c(0.40, 0.512),
  color = inset_line_color,
  size  = 1
) +
cowplot::draw_text(
  text  = "Mediterranean",
  x     = 0.075,
  y     = 0.850,
  size  = 12,
  hjust = 0.0,
  vjust = 0.0
) +
cowplot::draw_text(
  text  = "monsoonal",
  x     = 0.075,
  y     = 0.545,
  size  = 12,
  hjust = 0.0,
  vjust = 0.0
) +
cowplot::draw_grob(
  ca_bound
) +
cowplot::draw_grob(
  nm_bound
)

ggplot2::ggsave(
  filename = "/tmp/stacked.jpg",
  width    = 9,
  height   = 7,
  units    = c("in")
)
