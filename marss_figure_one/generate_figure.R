#' @title Generate maps of Santa Barbara and Valles Caldera study sites in
#' context for Persistent and lagged effects of wildfire on stream
#' biogeochemistry due to intermittent precipitation in arid lands manuscript
#'
#' @description Resources to generate figure 1 for the MARSS manuscript

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
      -0.63,
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
      y          = 7.000,
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
  y     = c(0.60, 0.51),
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
  text  = "SB",
  x     = 0.545,
  y     = 0.530,
  size  = 12,
  hjust = 0.0,
  vjust = 0.0
) +
cowplot::draw_text(
  text  = "VC",
  x     = 0.632,
  y     = 0.535,
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

ggplot2::ggsave("/tmp/stacked.jpg")
