#' @title Generate a national map of long-term, average precipitation.
#'
#' @description Generate a national map of long-term, average precipitation to
#' provide context for the study locations in the arid southwest.
#'
#' @note Data are 30-year normal precipitation from
#' [PRISM](https://prism.oregonstate.edu/normals/).
#'
#' @note The goal is to generate a map near to the GIS Geography map of US
#' Precipitation that Alex identified.
#' https://gisgeography.com/us-precipitation-map/

# PRISM data are in units of mm. We need to produce breaks at the same or
# similar intervals as the GIS Geography map (inches) to match the color
# profile.

# breaks <- readr::read_csv("data/breaks.csv") |> 
#   dplyr::arrange(mm)

#     `in`    mm
#    <dbl> <dbl>
#  1     0     0
#  2     5   127
#  3    10   254
#  4    15   381
#  5    20   508
#  6    25   635
#  7    30   762
#  8    35   889
#  9    40  1016
# 10    50  1270
# 11    60  1524
# 12    70  1778
# 13    80  2032
# 14   100  2540
# 15   120  3048
# 16   140  3556
# 17   180  4572
# 18   200  5080


bil_terra <- terra::rast("data/bil/PRISM_ppt_30yr_normal_4kmM4_annual_bil.bil")

bil_df <- as.data.frame(
  x  = bil_terra,
  xy = TRUE
) |>
  dplyr::rename(mm = PRISM_ppt_30yr_normal_4kmM4_annual_bil)

states <- tigris::states() |> 
  dplyr::filter(
    !grepl(
      pattern     = "alaska|hawaii|puerto|samoa|mariana|guam|island",
      x           = NAME,
      ignore.case = TRUE
    )
  )

ppt_map <- ggplot2::ggplot() +
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
  plot.margin      = ggplot2::unit(c(0, 0, 0, 0), "cm"),
  legend.position  = "none"
) +
ggplot2::labs(
  x = NULL,
  y = NULL
)
