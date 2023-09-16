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

# combine CA & NA into a single plot
(
  nm_ca <- cowplot::plot_grid(
    ca_inner,
    nm_inner,
    align = "vh",
    nrow  = 1,
    ncol  = 2
  )
)

# combine ppt, and CA & NM maps now both of which have isolated but joined legends
source("plot_legends.R")

(
  plot_with_ppt <- cowplot::plot_grid(
    ppt_with_legend,
    NULL,
    plot_with_legend,
    nrow        = 3,
    rel_heights = c(0.5, -0.4, 1.0)
  )
)


# add x-axis label (*longitude*) spanning both CA & NM plots
(
  plot_with_x_axis <- cowplot::ggdraw(
    plot = cowplot::add_sub(
      plot       = plot_with_ppt,
      label      = "Longitude",
      vpadding   = grid::unit(0, "lines"),
      y          = 0.5,
      x          = 0.5,
      vjust      = -22,
      fontfamily = "sans",
      size       = 10
    )
  )
)


ca_bound <- grid::rectGrob(
  x      = 0.13,
  y      = 0.73,
  width  = 0.03,
  height = 0.02,
  gp     = grid::gpar(
    col = inset_line_color,
    lwd = 2
  )
)

nm_bound <- grid::rectGrob(
  x      = 0.28,
  y      = 0.76,
  width  = 0.03,
  height = 0.02,
  gp     = grid::gpar(
    col = inset_line_color,
    lwd = 2
  )
)

plot_with_x_axis +
# line: CA
cowplot::draw_line(
  x = c(0.10, 0.13),
  y = c(0.55, 0.720),
  color = inset_line_color,
  size = 1
) +
# line: NM
cowplot::draw_line(
  x = c(0.53, 0.295),
  y = c(0.55, 0.750),
  color = inset_line_color,
  size = 1
) +
cowplot::draw_text(
  text  = "SB",
  x     = 0.112,
  y     = 0.750,
  size  = 12,
  hjust = 0.0,
  vjust = 0.0
) +
cowplot::draw_text(
  text  = "VC",
  x     = 0.263,
  y     = 0.78,
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


## using patchwork instead of cowplot

# nm_ca <- ppt_map / (ca_inner + nm_inner) + patchwork::plot_layout(guides = "collect")
