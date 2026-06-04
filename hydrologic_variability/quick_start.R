path <- "~/Desktop/rds/"

asRDS <- readRDS(file.path(path, "usgs_merged_20260427.rds"))

asRDS <- asRDS |>
  dplyr::mutate(
    usgs_site = stringr::str_remove(usgs_site, "USGS-")
  ) |>
  dplyr::select(
    site = usgs_site,
    date = time,
    cfs  = value
  )

asRDS_gap_filled <-
  asRDS |>
  dplyr::mutate(
    site = base::as.character(site),
    date = base::as.Date(date),
    cfs  = base::as.numeric(cfs)
  ) |>
  dplyr::group_split(site, .keep = TRUE) |>
  purrr::map_dfr(
    \(x) {
      site_id <- dplyr::first(x$site)

      x |>
        tidyr::complete(
          site = site_id,
          date = base::seq.Date(
            from = base::min(x$date, na.rm = TRUE),
            to   = base::max(x$date, na.rm = TRUE),
            by   = "day"
          ),
          fill = list(cfs = NA_real_)
        ) |>
        dplyr::arrange(date)
    }
  ) |>
  dplyr::arrange(site, date)

# keep only sites with <= 100 missing cfs values
t.dat <-
  asRDS_gap_filled |>
  dplyr::semi_join(
    asRDS_gap_filled |>
      dplyr::group_by(site) |>
      dplyr::summarise(
        na_cfs_count = sum(is.na(cfs)),
        .groups = "drop"
      ) |>
      dplyr::filter(na_cfs_count <= 100) |>
      dplyr::select(site),
    by = "site"
  )

tdat.lst <- split(t.dat, t.dat$site)
tdat.lst <- lapply(tdat.lst, dplyr::select, -"site")

flow.list <-
  tdat.lst |>
  purrr::map(
    \(x) {
      x |>
        base::as.data.frame() |>
        discharge::asStreamflow(max.na = 100)
    }
  )

head(flow.list[["06259000"]]$data)
head(flow.list[["06279500"]]$data) # omitted NAs == 110

seas.list <- lapply(flow.list, discharge::fourierAnalysis)