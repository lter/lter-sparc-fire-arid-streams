#' @title Generate dummy plots to extract legends as isolated objects
#'
#' @description Generate near-identical plots to isolate the legend from each,
#' which gives us flexibility to align when combining in cowplot.

# legend for California and New Mexico map -------------------------------------

for_legend <- ggmap::ggmap(nm_map) + 
ggplot2::theme_minimal(base_family = "sans") +
ggplot2::geom_sf(
  mapping     = ggplot2::aes(fill = "catchment"),
  data        = nm_catchments,
  inherit.aes = FALSE
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
  show.legend = "point"
) +
ggplot2::labs(
  x = "longitude",
  y = "latitude"
) +
ggplot2::scale_fill_manual(
  name   = NULL,
  values = c(
    "catchment" = "black",
    "fire"      = fires_color,
    "outlet"    = NA
  ),
  guide  = ggplot2::guide_legend(
    override.aes = list(
      shape    = c(NA, NA, 16),
      linetype = c(1, 1, 0),
      fill     = c(NA, fires_color, NA),
      colour   = c("black", fires_color, outlet_color),
      size     = c(NA, NA, outlet_size)
    )
  )
) +
ggplot2::scale_x_continuous(breaks = nm_x_breaks, expand = c(0, 0)) +
ggplot2::scale_y_continuous(breaks = nm_y_breaks, expand = c(0, 0)) +
ggplot2::theme(
  # axis.title.x = ggplot2::element_blank(),
  # legend.position = "none",
  axis.title   = ggplot2::element_text(size = 8),
  axis.text.x  = ggplot2::element_text(size = 6),
  axis.text.y  = ggplot2::element_text(size = 6)
)


nm_ca_legend <- cowplot::get_legend(
  for_legend + ggplot2::theme(legend.box.margin = ggplot2::margin(0, 0, 0, 6)) # space to left of legend
)


# Add the legend. Give it one-third of the width of one plot (via rel_widths).
plot_with_legend <- cowplot::plot_grid(
  nm_ca,
  nm_ca_legend,
  rel_widths = c(3, 0.6)
)


# legend for national precipitation map ----------------------------------------

ppt_legend <- ggplot2::ggplot() +
ggplot2::geom_raster(
  data = bil_df,
  ggplot2::aes(
    x    = x,
    y    = y,
    fill = mm
  )
) +
ggplot2::scale_fill_gradientn(
  colours = c(
    # red
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
    # blue
    "#928bc1",
    "#7370b3",
    "#625eaa",
    "#2b3092"
  ),
  values = scales::rescale(
    c(0.01, 0.04, 0.06, 0.08, 0.11, 0.13, 0.15, 0.17, 0.22, 0.27, 0.31, 0.36, 0.46, 0.54, 0.63, 0.81, 0.90)
  )
) +
ggplot2::geom_sf(
  data = states,
  fill = NA
) +
ggplot2::theme_minimal() +
ggplot2::theme(
  axis.text        = ggplot2::element_blank(),
  axis.ticks       = ggplot2::element_blank(),
  axis.title       = ggplot2::element_blank(),
  panel.grid.major = ggplot2::element_blank(),
  panel.grid.minor = ggplot2::element_blank(),
  panel.background = ggplot2::element_blank(),
  plot.margin      = ggplot2::unit(c(0, 0, 0, 0), "cm")
) +
ggplot2::labs(
  x = NULL,
  y = NULL,
  fill = "MAP"
)

ppt_legend <- cowplot::get_legend(
  ppt_legend + ggplot2::theme(legend.box.margin = ggplot2::margin(0, 0, 0, -12)) # draw legend closer to plot
)

ppt_with_legend <- cowplot::plot_grid(
  ppt_map,
  ppt_legend,
  rel_widths = c(3, 0.6)
)
