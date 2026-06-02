Where we left off

- Canonical editing source is `water_chem_functions.qmd`.
- EVI MV builder is defined at `water_chem_functions.qmd`:433.
- Fire-severity MV builder is defined at `water_chem_functions.qmd`:794.
- Export function that includes EVI covariates is at
  water_chem_functions.qmd:2442, with example calls near
  water_chem_functions.qmd:2677.
- Generated SQL artifact is water_chem_functions.sql, including EVI function at
  water_chem_functions.sql:311.
- Generated docs artifact is water_chem_functions.md.

Workflow status reminder

- Preferred sync path in this repo is qmd -> sql -> md via just sync in
  justfile:28.
- One-step load path is just run in justfile:16.
- Database default in justfile is wildfire_dev, so set PGDATABASE=wildfire when
  you want production wildfire behavior.

How to regenerate and run the EVI outputs

From repo root:

Regenerate sql and md from qmd
PGDATABASE=wildfire just sync water_chem_functions.qmd

Load updated SQL functions into DB
PGDATABASE=wildfire just execute-sql water_chem_functions.sql

Build EVI MVs for all four analytes
psql -d wildfire -c "SELECT firearea.create_evi_join_largest_analyte_mv('nitrate');"
psql -d wildfire -c "SELECT firearea.create_evi_join_largest_analyte_mv('ammonium');"
psql -d wildfire -c "SELECT firearea.create_evi_join_largest_analyte_mv('spcond');"
psql -d wildfire -c "SELECT firearea.create_evi_join_largest_analyte_mv('orthop');"

Build severity MVs for all four analytes
psql -d wildfire -c "SELECT firearea.create_fire_severity_join_largest_analyte_mv('nitrate');"
psql -d wildfire -c "SELECT firearea.create_fire_severity_join_largest_analyte_mv('ammonium');"
psql -d wildfire -c "SELECT firearea.create_fire_severity_join_largest_analyte_mv('spcond');"
psql -d wildfire -c "SELECT firearea.create_fire_severity_join_largest_analyte_mv('orthop');"

Export analyte outputs
psql -d wildfire -c "SELECT firearea.export_analyte_covariates_pre_post('nitrate','/tmp/nitrate_largest_pre_post_covariates.csv');"
psql -d wildfire -c "SELECT firearea.export_analyte_covariates_pre_post('ammonium','/tmp/ammonium_largest_pre_post_covariates.csv');"
psql -d wildfire -c "SELECT firearea.export_analyte_covariates_pre_post('orthop','/tmp/orthop_largest_pre_post_covariates.csv');"
psql -d wildfire -c "SELECT firearea.export_analyte_covariates_pre_post('spcond','/tmp/spcond_largest_pre_post_covariates.csv');"
