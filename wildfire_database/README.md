## quarto

We can call quarto to render files sensu `# quarto render --profile chem -P
user:me`. The project was structured initially to be able to run {sql} chunks
by rendering the Quarto document. However, neither the knitr or jupyter engines
will run native SQL that does not return something (e.g., we could not execute
the SQL statements to build functions). Hence the code such as `params` in the
yml that is just legacy as we are now addressing all SQL execution via PSQL
either manually or with the help of just recipes (see below).

## just

Use just recipes to extract sql chunks from qmd files with option to execute
the sql statements in the sql file as well. There is also a recipe to render
qmd.

**Work with different .qmd files**
- just run water_chem_functions.qmd
- just run wildfire_database_query_nitrate.qmd
- just run wildfire_database_query_orthop.qmd

**Extract only (no execution)**
- just extract water_chem_functions.qmd
- just extract wildfire_database_query_nitrate.qmd

**Generate docs only**
- just docs water_chem_functions.qmd
- just docs wildfire_database_query_nitrate.qmd

**Complete workflow**
- just all water_chem_functions.qmd
- just all wildfire_database_query_nitrate.qmd

**Custom output filename**
- python3 extract_sql.py wildfire_database_query_nitrate.qmd -o nitrate_queries.sql
- just execute-sql nitrate_queries.sql

**No transaction wrapper**
- python3 extract_sql.py wildfire_database_query_nitrate.qmd --no-transaction

**List available .qmd files**
- just list-qmd

**Custom database connection**
- PGHOST=remote.server.com just nitrate
- dbname=test_db user=testuser just run water_chem_functions.qmd

**Uses wildfire_dev (default)**
- just test

**Uses production (from environment)**
- PGDATABASE=production just test

**Uses test_db (from environment)**
- export PGDATABASE=test_db
- just test

**If USER=me and PGUSER not set → uses "me"**
- just test

**Uses admin (from PGUSER environment)**
- PGUSER=admin just test

**Uses postgres (from PGUSER environment)**
- export PGUSER=postgres
- just test

**Set for entire session**
- export PGHOST=staging.db.com
- export PGDATABASE=staging
- just test
- just run water_chem_functions.qmd

**Check what environment variables you currently have set**
- echo $PGDATABASE    *Probably empty/unset*
- echo $PGUSER        *Probably empty/unset*  
- echo $PGHOST        *Probably empty/unset*
- echo $USER          *Shows your username (e.g., "me")*

**misc**
- just manual water_chem_functions.sql -h remote.server.com -d production -U admin -p 5433
- just manual water_chem_functions.sql -d wildfire

## water chemistry

See
[here](https://github.com/lter/lter-sparc-fire-arid-streams/blob/main/wildfire_database/water_chem_functions.md#water-chemistry-alignment-and-products)
for the in-project overview but generally we will:

1. ensure all of the functions exist `just manual water_chem_functions.sql -d
   wildfire`
2. build or rebuild the standardized water chem and aggregated analyte views,
   and analyte counts and largest fire materialized views. This can be done
piece-meal or with a scrip that performs all steps: `SELECT
firearea.rebuild_usgs_water_chem_std_and_dependencies();`
3. call the export functions to write to file
