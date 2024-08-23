# Tools to query water chemistry data from the wildfire database. Easily
# accessible data are stored as an rds file in the lter-sparc-fire-arid shared
# drive on Aurora. A workflow to refresh the rds file as needed is detailed.

# query the data from rds ------------------------------------------------------

usgs_chemistry <- readRDS("/home/shares/lter-sparc-fire-arid/usgs_chemistry.rds")


# refresh the rds --------------------------------------------------------------

base_query <- "
SELECT
  *,
  CASE
    WHEN \"USGSPCode\" = '71851' THEN \"ResultMeasureValue\" * (14.0/62.0) -- no3_d
    WHEN \"USGSPCode\" = '71856' THEN \"ResultMeasureValue\" * (14.0/46.0) -- no2_d
    WHEN \"USGSPCode\" = '71846' THEN \"ResultMeasureValue\" * (14.0/18.0) -- nh4_d
    WHEN \"USGSPCode\" = '71845' THEN \"ResultMeasureValue\" * (14.0/18.0) -- nh4_t
    ELSE \"ResultMeasureValue\"
  END value_std,
  CASE
    WHEN \"USGSPCode\" = '71851' THEN 'mg/l as N'
    WHEN \"USGSPCode\" = '71856' THEN 'mg/l as N'
    WHEN \"USGSPCode\" = '71846' THEN 'mg/l as N'
    WHEN \"USGSPCode\" = '71845' THEN 'mg/l as N'
    ELSE \"ResultMeasure.MeasureUnitCode\"
  END units_std,
  CASE
    WHEN \"USGSPCode\" = '71851' THEN '00618'
    WHEN \"USGSPCode\" = '71856' THEN '00613'
    WHEN \"USGSPCode\" = '71846' THEN '00608'
    WHEN \"USGSPCode\" = '71845' THEN '00610'
    WHEN \"USGSPCode\" = '90095' THEN '00095' -- sp cond
    ELSE \"USGSPCode\"
  END usgspcode_std
FROM firearea.water_chem
WHERE
  \"ActivityMediaName\" ~~* 'Water' AND             -- water samples only
  \"ActivityMediaSubdivisionName\" ~* 'Surface' AND -- surface water only
  \"ActivityTypeCode\" !~* 'quality' AND            -- omit QC (blanks, spikes, etc.)
  -- omit bed sediments and null values
  \"ResultSampleFractionText\" IN (
    'Dissolved',
    'Non-filterable',
    'Recoverable',
    'Suspended',
    'Total'
    ) AND
  \"USGSPCode\" NOT IN (
    '00402', -- spcond not at 25 c
    -- '91003', -- nitrate ug/L n=1
    '00070'  -- turbidity as JTU
  ) AND
  \"HydrologicEvent\" IN (
    'Affected by fire',
    'Backwater',
    -- 'Dambreak',
    'Drought',
    -- 'Earthquake',
    'Flood',
    -- 'Hurricane',
    -- 'Mudflow',
    'Not applicable',
    'Not Determined (historical)',
    'Regulated flow',
    'Routine sample',
    'Snowmelt',
    -- 'Spill',
    'Spring breakup' --,
    -- 'Storm',
    -- 'Under ice cover',
    -- 'Volcanic action'
  ) AND
  usgs_site IN (
    SELECT usgs_site FROM firearea.ecoregion_catchments
  )
;
"

usgs_chemistry <- DBI::dbGetQuery(
  conn      = pg,
  statement = base_query
)

saveRDS(
  object = usgs_chemistry,
  file   = "/home/shares/lter-sparc-fire-arid/usgs_chemistry.rds"
)

# nitrate ----------------------------------------------------------------------

# Build a dataset of usgs and non-usgs nitrate data

non_usgs_nitrate <- DBI::dbGetQuery(
  conn      = pg,
  statement = "
  SELECT *
  FROM firearea.non_usgs_water_chem
  WHERE
    analyte ~~* '%nitrate%' OR
    analyte ~~* '%no3%'
  ;
  "
)

nitrate_dataset <- usgs_chemistry |> 
  dplyr::filter(usgspcode_std %in% c("00618", "00631")) |> 
  dplyr::select(
    usgs_site,
    date    = ActivityStartDate,
    analyte = CharacteristicName,
    value_std
  ) |> 
  dplyr::bind_rows(
    non_usgs_nitrate |> 
      dplyr::mutate(
        value_std = dplyr::case_when(
          grepl("micromoles", unit, ignore.case = TRUE) ~ mean * (14 / 1000),
          TRUE ~ mean
        )
      ) |> 
      dplyr::select(
        usgs_site,
        date,
        analyte,
        value_std
      )
  )

saveRDS(
  object = nitrate_dataset,
  # file   = "/home/shares/lter-sparc-fire-arid/nitrate_chemistry.rds"
  file   = "/tmp/nitrate_chemistry.rds"
)
