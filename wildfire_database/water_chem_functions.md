

## overview

Functions for building and extracting water chemistry resources. Note
that the functions are built here but called elsewhere in the workflow.

Queries generate either a view, materialized view, or export. Views
should be reconstructed as needed based on database updates.

### water chemistry alignment and products

stepwise:

– Step 1: Rebuild base view only (destroys all dependencies) SELECT
firearea.rebuild_usgs_water_chem_std();

– Step 2: Rebuild analyte views SELECT firearea.create_nitrate_view();
SELECT firearea.create_spcond_view(); SELECT
firearea.create_ammonium_view(); SELECT firearea.create_orthop_view();

– Step 3: Rebuild counts views SELECT
firearea.create_analyte_counts_view(‘nitrate’); SELECT
firearea.create_analyte_counts_view(‘spcond’); SELECT
firearea.create_analyte_counts_view(‘ammonium’); SELECT
firearea.create_analyte_counts_view(‘orthop’);

– Step 4: Rebuild largest fire materialized views SELECT
firearea.create_largest_analyte_valid_fire_per_site_mv(‘nitrate’);
SELECT firearea.create_largest_analyte_valid_fire_per_site_mv(‘spcond’);
SELECT
firearea.create_largest_analyte_valid_fire_per_site_mv(‘ammonium’);
SELECT firearea.create_largest_analyte_valid_fire_per_site_mv(‘orthop’);

single site:

– Just rebuild one analyte’s full stack (e.g., nitrate) SELECT
firearea.create_nitrate_view(); SELECT
firearea.create_analyte_counts_view(‘nitrate’);  
SELECT
firearea.create_largest_analyte_valid_fire_per_site_mv(‘nitrate’);

the whole game:

builds standarized chem; and views of aggregated values and counts, and
the largest fire materialized view for each analyte in a single call
(i.e., all of stepwise steps 1-4 from above in a single function)

SELECT firearea.rebuild_usgs_water_chem_std_and_dependencies();

## fn. view: usgs_water_chem_std (std chem)

``` sql
CREATE OR REPLACE FUNCTION firearea.rebuild_usgs_water_chem_std()
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    result_msg TEXT;
BEGIN
    RAISE NOTICE 'Rebuilding firearea.usgs_water_chem_std (this will CASCADE DROP all dependent views)...';
    
    -- Drop the base view with CASCADE (destroys all dependent objects)
    DROP VIEW IF EXISTS firearea.usgs_water_chem_std CASCADE;
    
    -- Recreate the base view
    CREATE VIEW firearea.usgs_water_chem_std AS
    SELECT
      *,
      CASE
        WHEN "USGSPCode" = '71851' THEN "ResultMeasureValue" * (14.0/62.0) -- no3_d
        WHEN "USGSPCode" = '71856' THEN "ResultMeasureValue" * (14.0/46.0) -- no2_d
        WHEN "USGSPCode" = '71846' THEN "ResultMeasureValue" * (14.0/18.0) -- nh4_d
        WHEN "USGSPCode" = '71845' THEN "ResultMeasureValue" * (14.0/18.0) -- nh4_t
        WHEN "USGSPCode" = '00660' THEN "ResultMeasureValue" * 0.3261      -- po4_d
        ELSE "ResultMeasureValue"
      END value_std,
      CASE
        WHEN "USGSPCode" = '71851' THEN 'mg/l as N'
        WHEN "USGSPCode" = '71856' THEN 'mg/l as N'
        WHEN "USGSPCode" = '71846' THEN 'mg/l as N'
        WHEN "USGSPCode" = '71845' THEN 'mg/l as N'
        WHEN "USGSPCode" = '00660' THEN 'mg/l as P'
        ELSE "ResultMeasure.MeasureUnitCode"
      END units_std
    FROM firearea.water_chem
    WHERE
      "ActivityMediaName" ~~* 'Water' AND
      "ActivityMediaSubdivisionName" ~* 'Surface' AND
      "ActivityTypeCode" !~* 'quality' AND
      "ResultSampleFractionText" !~* 'bed sediment' AND
      usgs_site IN (
        SELECT usgs_site FROM firearea.ecoregion_catchments
      );
    
    result_msg := 'SUCCESS: Rebuilt firearea.usgs_water_chem_std. All dependent views were CASCADE dropped and need to be recreated manually.';
    RAISE NOTICE '%', result_msg;
    
    RETURN result_msg;
    
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'FAILED: Error rebuilding usgs_water_chem_std: %', SQLERRM;
END;
$$;

-- Step 1: Rebuild base view only (destroys all dependencies)
-- SELECT firearea.rebuild_usgs_water_chem_std();
```

## aggregated nutrient views

### fn. view: combined nitrate

Create a view of standardized (forms, units) nitrate that reflects data
from from both the USGS and non-USGS data sources.

Convert NEON and SBC data (micromoles) to `mg NO3-N / L`.

``` sql
CREATE OR REPLACE FUNCTION firearea.create_nitrate_view()
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    result_msg TEXT;
BEGIN
    -- Drop existing view
    DROP VIEW IF EXISTS firearea.nitrate;

    -- Create nitrate view (directly from your .qmd file)
    CREATE VIEW firearea.nitrate AS 
    SELECT
      usgs_site,
      "ActivityStartDate" AS date,
      'nitrate' as analyte,
      AVG (value_std) AS value_std,
      'mg/L as N' AS units_std
    FROM firearea.usgs_water_chem_std
    WHERE
      "USGSPCode" IN (
        '00618',
        '00631',
        '71851'
      )
    GROUP BY
      usgs_site,
      date
    UNION
    SELECT
      usgs_site,
      date,
      analyte,
      CASE
        WHEN unit ~~* 'micromoles%' THEN mean * (14.0 / 1000.0)
        WHEN unit ~~* '%uM%' THEN mean * (14.0 / 1000.0) 
        ELSE mean
      END value_std,
      CASE
        WHEN unit ~~* 'micromoles%' THEN 'mg/L as N'
        WHEN unit ~~* '%uM%' THEN 'mg/L as N' 
        ELSE unit
      END units_std
    FROM firearea.non_usgs_water_chem
    WHERE
    (
      analyte ~~* '%nitrate%' OR
      analyte ~~* '%no3%'
    ) AND
      usgs_site !~~* '%bell%';

    result_msg := 'SUCCESS: Created firearea.nitrate view';
    RAISE NOTICE '%', result_msg;
    
    RETURN result_msg;

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'FAILED: Error creating nitrate view: %', SQLERRM;
END;
$$;

-- SELECT firearea.create_nitrate_view();
```

### fn. view: combined ammonium

Create a view of standardized (forms, units) ammonium that reflects data
from from both the USGS and non-USGS data sources.

Convert NEON and SBC data (micromoles) to `mg NH4-N / L`.

``` sql
CREATE OR REPLACE FUNCTION firearea.create_ammonium_view()
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    result_msg TEXT;
BEGIN
    -- Drop existing view
    DROP VIEW IF EXISTS firearea.ammonium;

    -- Create ammonium view (directly from your .qmd file)
    CREATE VIEW firearea.ammonium AS
    SELECT
      usgs_site,
      "ActivityStartDate" AS date,
      'ammonium' as analyte,
      AVG (value_std) AS value_std,
      'mg/L as N' AS units_std
    FROM firearea.usgs_water_chem_std
    WHERE
      "USGSPCode" IN (
        '71846',
        '00608'
      )
    GROUP BY
      usgs_site,
      date
    UNION
    SELECT
      usgs_site,
      date,
      analyte,
      CASE
        WHEN unit ~~* 'micromoles%' THEN mean * 0.014007
        WHEN unit ~~* '%uM%' THEN mean * 0.014007
        ELSE mean
      END value_std,
      CASE
        WHEN unit ~~* 'micromoles%' THEN 'mg/L as N'
        WHEN unit ~~* '%uM%' THEN 'mg/L as N' 
        ELSE unit
      END units_std
    FROM firearea.non_usgs_water_chem
    WHERE
    (
      analyte ~~* '%nh4%'
    ) AND
      usgs_site !~~* '%bell%';

    result_msg := 'SUCCESS: Created firearea.ammonium view';
    RAISE NOTICE '%', result_msg;
    
    RETURN result_msg;

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'FAILED: Error creating ammonium view: %', SQLERRM;
END;
$$;

-- SELECT firearea.create_ammonium_view();
```

### fn. view: combined specific conductance

Create a view of standardized (forms, units) specific conductance that
reflects data from from both the USGS and non-USGS data sources.

``` sql
CREATE OR REPLACE FUNCTION firearea.create_spcond_view()
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    result_msg TEXT;
BEGIN
    -- Drop existing view
    DROP VIEW IF EXISTS firearea.spcond;

    -- Create spcond view (directly from your .qmd file)
    CREATE VIEW firearea.spcond AS 
    SELECT
      usgs_site,
      "ActivityStartDate" AS date,
      'spcond' as analyte,
      AVG (value_std) AS value_std,
      'microsiemensPerCentimeter' AS units_std
    FROM firearea.usgs_water_chem_std
    WHERE
      "USGSPCode" IN (
        '00094',
        '00095',
        '90095'
      )
    GROUP BY
      usgs_site,
      date
    UNION
    SELECT
      usgs_site,
      date,
      analyte,
      mean AS value_std,
      'microsiemensPerCentimeter' AS units_std
    FROM firearea.non_usgs_water_chem
    WHERE
    (
      analyte ~~* '%spec_cond_uSpercm%' OR
      analyte ~~* '%specificConductance%'
    ) AND
      usgs_site !~~* '%bell%';

    result_msg := 'SUCCESS: Created firearea.spcond view';
    RAISE NOTICE '%', result_msg;
    
    RETURN result_msg;

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'FAILED: Error creating spcond view: %', SQLERRM;
END;
$$;

-- SELECT firearea.create_spcond_view();
```

### fn. view: combined orthop

Create a view of standardized (forms, units) of orthophosphate that
reflects data from from both the USGS and non-USGS data sources.

Convert SBC data (micromoles) to `mg PO4-P / L`.

``` sql
CREATE OR REPLACE FUNCTION firearea.create_orthop_view()
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    result_msg TEXT;
BEGIN
    -- Drop existing view
    DROP VIEW IF EXISTS firearea.orthop;

    -- Create orthop view (directly from your .qmd file)
    CREATE VIEW firearea.orthop AS
    SELECT
      usgs_site,
      "ActivityStartDate" AS date,
      'orthop' as analyte,
      AVG (value_std) AS value_std,
      'mg/L as P' AS units_std
    FROM firearea.usgs_water_chem_std
    WHERE
      "USGSPCode" IN (
        '00660', -- PO4
        '00671'  -- PO4-P
      )
    GROUP BY
      usgs_site,
      date
    UNION
    SELECT
      usgs_site,
      date,
      analyte,
      CASE
        WHEN unit ~~* '%uM%' THEN mean * 0.030974 -- mg PO₄–P/L=µmol PO₄/L×0.030974
        ELSE mean
      END value_std,
      CASE
        WHEN unit ~~* '%uM%' THEN 'mg/L as P' 
        ELSE unit
      END units_std
    FROM firearea.non_usgs_water_chem
    WHERE
    (
      analyte ~~* '%po4%' OR
      analyte ~~* '%ortho%'
    ) AND
      usgs_site !~~* '%bell%';

    result_msg := 'SUCCESS: Created firearea.orthop view';
    RAISE NOTICE '%', result_msg;
    
    RETURN result_msg;

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'FAILED: Error creating orthop view: %', SQLERRM;
END;
$$;

-- SELECT firearea.create_orthop_view();
```

## fn. view: usgs_water_chem_std AND dependencies

``` sql
CREATE OR REPLACE FUNCTION firearea.rebuild_usgs_water_chem_std_and_dependencies()
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    analyte_name TEXT;
    analyte_list TEXT[] := ARRAY['nitrate', 'spcond', 'ammonium', 'orthop']; -- append new analytes
    result_msg TEXT;
    base_result TEXT;
    success_count INTEGER := 0;
BEGIN
    RAISE NOTICE 'Starting complete rebuild of firearea.usgs_water_chem_std and all dependencies...';
    
    -- Step 1: Call the base rebuild function
    RAISE NOTICE 'Calling firearea.rebuild_usgs_water_chem_std()...';
    SELECT firearea.rebuild_usgs_water_chem_std() INTO base_result;
    RAISE NOTICE '%', base_result;
    
    -- Check that base rebuild succeeded
    IF base_result NOT LIKE 'SUCCESS:%' THEN
        RAISE EXCEPTION 'Base view rebuild failed: %', base_result;
    END IF;
    
    -- Step 2: Recreate analyte views using the specific functions
    FOREACH analyte_name IN ARRAY analyte_list
    LOOP
        BEGIN
            RAISE NOTICE 'Creating analyte view: %', analyte_name;
            CASE analyte_name
                WHEN 'nitrate' THEN
                    PERFORM firearea.create_nitrate_view();
                WHEN 'spcond' THEN  
                    PERFORM firearea.create_spcond_view();
                WHEN 'ammonium' THEN
                    PERFORM firearea.create_ammonium_view();
                WHEN 'orthop' THEN
                    PERFORM firearea.create_orthop_view();
                ELSE
                    RAISE WARNING 'No create function found for analyte: %', analyte_name;
            END CASE;
            success_count := success_count + 1;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE WARNING 'Failed to create analyte view for %: %', analyte_name, SQLERRM;
        END;
    END LOOP;
    
    -- Step 3: Recreate counts views
    FOREACH analyte_name IN ARRAY analyte_list
    LOOP
        BEGIN
            RAISE NOTICE 'Creating counts view: %_counts', analyte_name;
            PERFORM firearea.create_analyte_counts_view(analyte_name);
        EXCEPTION
            WHEN OTHERS THEN
                RAISE WARNING 'Failed to create counts view for %: %', analyte_name, SQLERRM;
        END;
    END LOOP;
    
    -- Step 4: Recreate largest fire materialized views
    FOREACH analyte_name IN ARRAY analyte_list
    LOOP
        BEGIN
            RAISE NOTICE 'Creating largest fire materialized view: largest_%_valid_fire_per_site', analyte_name;
            PERFORM firearea.create_largest_analyte_valid_fire_per_site_mv(analyte_name);
        EXCEPTION
            WHEN OTHERS THEN
                RAISE WARNING 'Failed to create largest fire matview for %: %', analyte_name, SQLERRM;
        END;
    END LOOP;
    
    -- Step 5: Final verification
    RAISE NOTICE 'Verifying all objects were created successfully...';
    
    -- Check base view exists (should always pass since we called the function)
    IF NOT EXISTS (
        SELECT 1 FROM pg_views 
        WHERE schemaname = 'firearea' AND viewname = 'usgs_water_chem_std'
    ) THEN
        RAISE EXCEPTION 'Failed to create firearea.usgs_water_chem_std';
    END IF;
    
    -- Check analyte views exist
    FOREACH analyte_name IN ARRAY analyte_list
    LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_views 
            WHERE schemaname = 'firearea' AND viewname = analyte_name
        ) THEN
            RAISE WARNING 'Missing analyte view: firearea.%', analyte_name;
        END IF;
        
        IF NOT EXISTS (
            SELECT 1 FROM pg_views 
            WHERE schemaname = 'firearea' AND viewname = analyte_name || '_counts'
        ) THEN
            RAISE WARNING 'Missing counts view: firearea.%_counts', analyte_name;
        END IF;
        
        IF NOT EXISTS (
            SELECT 1 FROM pg_matviews 
            WHERE schemaname = 'firearea' AND matviewname = 'largest_' || analyte_name || '_valid_fire_per_site'
        ) THEN
            RAISE WARNING 'Missing largest fire matview: firearea.largest_%_valid_fire_per_site', analyte_name;
        END IF;
    END LOOP;
    
    result_msg := FORMAT('SUCCESS: Rebuilt firearea.usgs_water_chem_std and all dependencies for %s analytes: %s', 
                         array_length(analyte_list, 1),
                         array_to_string(analyte_list, ', '));
    RAISE NOTICE '%', result_msg;
    
    RETURN result_msg;
    
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'FAILED: Error rebuilding dependencies: %', SQLERRM;
        -- Transaction will automatically roll back on exception
END;
$$;

-- Complete rebuild of everything
-- SELECT firearea.rebuild_usgs_water_chem_std_and_dependencies();
```

## fn. view: analyte_counts (summer)

Generate a view of summary statistics surrounding analyte data
availability (number of samples pre, post fire).

The `num_pre_fire` and `num_post_fire` reflect the number of
observations in the period prior to and after fire or aggregated fires
of interest.

The number of observations reflects observations for which there is also
a discharge measurement.

``` sql
CREATE OR REPLACE FUNCTION firearea.create_analyte_counts_view(analyte_name TEXT)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    sql_query TEXT;
    table_exists BOOLEAN;
    table_has_data BOOLEAN;
    result_text TEXT;
BEGIN
    -- Input validation
    IF analyte_name !~ '^[a-zA-Z_][a-zA-Z0-9_]*$' THEN
        RETURN FORMAT('ERROR: Invalid analyte name: %s', analyte_name);
    END IF;
    
    -- Check if the table exists
    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'firearea' AND table_name = analyte_name
    ) INTO table_exists;
    
    IF NOT table_exists THEN
        RETURN FORMAT('ERROR: Table firearea.%s does not exist', analyte_name);
    END IF;
    
    -- Check if the table has data
    EXECUTE FORMAT('SELECT EXISTS(SELECT 1 FROM firearea.%I LIMIT 1)', analyte_name)
    INTO table_has_data;
    
    IF NOT table_has_data THEN
        RETURN FORMAT('WARNING: Table firearea.%s exists but contains no data. View not created.', analyte_name);
    END IF;
    
    -- Check if the resulting view would have data
    EXECUTE FORMAT($check$
        SELECT EXISTS(
            SELECT 1 FROM firearea.%I 
            JOIN firearea.discharge ON (
                discharge."Date" = %I.date
                AND discharge.usgs_site = %I.usgs_site
            )
            JOIN firearea.ranges_agg ON (ranges_agg.usgs_site = %I.usgs_site)
            LIMIT 1
        )
    $check$, analyte_name, analyte_name, analyte_name, analyte_name)
    INTO table_has_data;
    
    IF NOT table_has_data THEN
        RETURN FORMAT('WARNING: No matching data found for %s with discharge and ranges_agg. View not created.', analyte_name);
    END IF;
    
    -- Create the view if all checks pass
    sql_query := replace($template$
    DROP VIEW IF EXISTS firearea.ANALYTE_counts CASCADE;
    
    CREATE VIEW firearea.ANALYTE_counts AS 
    WITH ANALYTE_discharge AS (
      SELECT
        ANALYTE.usgs_site,
        ANALYTE.date
      FROM firearea.ANALYTE
      JOIN firearea.discharge ON (
        discharge."Date" = ANALYTE.date
        AND discharge.usgs_site = ANALYTE.usgs_site
      )
    )
    SELECT
      ANALYTE_discharge.usgs_site,
      ranges_agg.events,
      ranges_agg.start_date,
      ranges_agg.end_date,
      SUM(CASE WHEN ANALYTE_discharge.date < ranges_agg.start_date THEN 1 ELSE 0 END) AS count_before_start,
      SUM(CASE WHEN ANALYTE_discharge.date > ranges_agg.end_date THEN 1 ELSE 0 END) AS count_after_end
    FROM firearea.ranges_agg
    JOIN ANALYTE_discharge ON (ranges_agg.usgs_site = ANALYTE_discharge.usgs_site)
    GROUP BY
      ANALYTE_discharge.usgs_site,
      ranges_agg.start_date,
      ranges_agg.end_date,
      ranges_agg.events;
    $template$, 'ANALYTE', analyte_name);
    
    EXECUTE sql_query;
    
    RETURN FORMAT('SUCCESS: View firearea.%s_counts created successfully', analyte_name);
    
EXCEPTION
    WHEN OTHERS THEN
        RETURN FORMAT('ERROR: Failed to create view for %s: %s', analyte_name, SQLERRM);
END;
$$;
```

## fn. m.view: analyte largest fire

purpose:

Function to create a materialized view that identifies the largest
wildfire per watershed (`usgs_site`) that meets strict pre- and
post-fire data quality criteria for water quality analysis. It enables
fast, repeatable queries for analyzing wildfire impacts on analyte and
discharge patterns.

use case:

- Supports analysis of analyte and streamflow responses to wildfire.
- Used to restrict focus to watersheds with both extensive monitoring
  coverage and a single, largest impactful fire.
- Reused in downstream workflows for filtering, reporting, or dashboard
  visualizations.

logic summary:

1.  Join analyte and discharge records by site and date.
2.  Match those records to fire windows from `ranges_agg`.
3.  Apply Data Sufficiency Filters:

- Must have analyte + discharge data in the 3 years before and after
  each fire.
- Must include observations in flow quartiles 2, 3, and 4 in both
  windows.

4.  Group results by fire event (site, year, start/end date).
5.  Select only the largest valid fire per watershed, ranked by
    `cum_fire_area`.

fields included:

| Column Name | Description |
|----|----|
| `usgs_site` | Watershed site identifier |
| `year` | Fire year (based on ignition date) |
| `start_date` | Start date of fire event group |
| `end_date` | End date of fire event group |
| `cum_fire_area` | Cumulative fire-affected area (km²) for the grouped event |
| `before_count` | Count of analyte observations in 3-year window before fire |
| `after_count` | Count of analyte observations in 3-year window after fire |
| `bq2`, `bq3`, `bq4` | Count of pre-fire flow quartile 2, 3, and 4 observations |
| `aq2`, `aq3`, `aq4` | Count of post-fire flow quartile 2, 3, and 4 observations |

technical notes:

- Source tables:
  - `firearea.{analyte}`
  - `firearea.discharge`
  - `firearea.ranges_agg`
- Computation-intensive: uses joins, date filters, aggregation, and
  conditional `HAVING` clauses.
- Materialized to avoid recomputation and accelerate downstream query
  performance.
- Refresh as needed to reflect updates to analyte, discharge, or fire
  data.

``` sql
CREATE OR REPLACE FUNCTION firearea.create_largest_analyte_valid_fire_per_site_mv(analyte_name TEXT)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    sql_query TEXT;
    table_exists BOOLEAN;
    table_has_data BOOLEAN;
    mv_name TEXT;
    index_name_site TEXT;
    index_name_date TEXT;
BEGIN
    -- Input validation
    IF analyte_name !~ '^[a-zA-Z_][a-zA-Z0-9_]*$' THEN
        RETURN FORMAT('ERROR: Invalid analyte name: %s', analyte_name);
    END IF;
    
    -- Check if the analyte table exists
    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'firearea' AND table_name = analyte_name
    ) INTO table_exists;
    
    IF NOT table_exists THEN
        RETURN FORMAT('ERROR: Table firearea.%s does not exist', analyte_name);
    END IF;
    
    -- Check if the table has data
    EXECUTE FORMAT('SELECT EXISTS(SELECT 1 FROM firearea.%I LIMIT 1)', analyte_name)
    INTO table_has_data;
    
    IF NOT table_has_data THEN
        RETURN FORMAT('WARNING: Table firearea.%s exists but contains no data. Materialized view not created.', analyte_name);
    END IF;
    
    -- Set materialized view and index names
    mv_name := FORMAT('firearea.largest_%s_valid_fire_per_site', analyte_name);
    index_name_site := FORMAT('idx_%s_fire_usgs_site', analyte_name);
    index_name_date := FORMAT('idx_%s_fire_start_date', analyte_name);
    
    -- Create the materialized view
    sql_query := replace($template$
    DROP MATERIALIZED VIEW IF EXISTS MV_NAME CASCADE;
    
    CREATE MATERIALIZED VIEW MV_NAME AS
    WITH fire_with_data AS (
      SELECT
        firearea.ANALYTE.usgs_site,
        firearea.ranges_agg.year,
        firearea.ranges_agg.events,
        firearea.ranges_agg.start_date,
        firearea.ranges_agg.end_date,
        firearea.ranges_agg.cum_fire_area,
        COUNT(CASE WHEN firearea.ANALYTE.date >= (firearea.ranges_agg.start_date - INTERVAL '3 years')
                   AND firearea.ANALYTE.date < firearea.ranges_agg.start_date THEN 1 END) AS before_count,
        COUNT(CASE WHEN firearea.ANALYTE.date > firearea.ranges_agg.end_date
                   AND firearea.ANALYTE.date <= (firearea.ranges_agg.end_date + INTERVAL '3 years') THEN 1 END) AS after_count,
        SUM(CASE WHEN firearea.ANALYTE.date >= (firearea.ranges_agg.start_date - INTERVAL '3 years')
                  AND firearea.ANALYTE.date < firearea.ranges_agg.start_date AND firearea.discharge.quartile = 2 THEN 1 ELSE 0 END) AS bq2,
        SUM(CASE WHEN firearea.ANALYTE.date >= (firearea.ranges_agg.start_date - INTERVAL '3 years')
                  AND firearea.ANALYTE.date < firearea.ranges_agg.start_date AND firearea.discharge.quartile = 3 THEN 1 ELSE 0 END) AS bq3,
        SUM(CASE WHEN firearea.ANALYTE.date >= (firearea.ranges_agg.start_date - INTERVAL '3 years')
                  AND firearea.ANALYTE.date < firearea.ranges_agg.start_date AND firearea.discharge.quartile = 4 THEN 1 ELSE 0 END) AS bq4,
        SUM(CASE WHEN firearea.ANALYTE.date > firearea.ranges_agg.end_date
                  AND firearea.ANALYTE.date <= (firearea.ranges_agg.end_date + INTERVAL '3 years') AND firearea.discharge.quartile = 2 THEN 1 ELSE 0 END) AS aq2,
        SUM(CASE WHEN firearea.ANALYTE.date > firearea.ranges_agg.end_date
                  AND firearea.ANALYTE.date <= (firearea.ranges_agg.end_date + INTERVAL '3 years') AND firearea.discharge.quartile = 3 THEN 1 ELSE 0 END) AS aq3,
        SUM(CASE WHEN firearea.ANALYTE.date > firearea.ranges_agg.end_date
                  AND firearea.ANALYTE.date <= (firearea.ranges_agg.end_date + INTERVAL '3 years') AND firearea.discharge.quartile = 4 THEN 1 ELSE 0 END) AS aq4
      FROM firearea.ANALYTE
      JOIN firearea.discharge
        ON firearea.ANALYTE.usgs_site = firearea.discharge.usgs_site
        AND firearea.ANALYTE.date = firearea.discharge."Date"
      JOIN firearea.ranges_agg
        ON firearea.ANALYTE.usgs_site = firearea.ranges_agg.usgs_site
      WHERE firearea.ANALYTE.value_std IS NOT NULL
        AND firearea.discharge."Flow" IS NOT NULL
        AND firearea.discharge.quartile IS NOT NULL
      GROUP BY
        firearea.ANALYTE.usgs_site,
        firearea.ranges_agg.year,
        firearea.ranges_agg.events,
        firearea.ranges_agg.start_date,
        firearea.ranges_agg.end_date,
        firearea.ranges_agg.cum_fire_area
      HAVING
        SUM(CASE WHEN firearea.ANALYTE.date >= (firearea.ranges_agg.start_date - INTERVAL '3 years') AND firearea.ANALYTE.date < firearea.ranges_agg.start_date AND firearea.discharge.quartile = 2 THEN 1 ELSE 0 END) > 0 AND
        SUM(CASE WHEN firearea.ANALYTE.date >= (firearea.ranges_agg.start_date - INTERVAL '3 years') AND firearea.ANALYTE.date < firearea.ranges_agg.start_date AND firearea.discharge.quartile = 3 THEN 1 ELSE 0 END) > 0 AND
        SUM(CASE WHEN firearea.ANALYTE.date >= (firearea.ranges_agg.start_date - INTERVAL '3 years') AND firearea.ANALYTE.date < firearea.ranges_agg.start_date AND firearea.discharge.quartile = 4 THEN 1 ELSE 0 END) > 0 AND
        SUM(CASE WHEN firearea.ANALYTE.date > firearea.ranges_agg.end_date AND firearea.ANALYTE.date <= (firearea.ranges_agg.end_date + INTERVAL '3 years') AND firearea.discharge.quartile = 2 THEN 1 ELSE 0 END) > 0 AND
        SUM(CASE WHEN firearea.ANALYTE.date > firearea.ranges_agg.end_date AND firearea.ANALYTE.date <= (firearea.ranges_agg.end_date + INTERVAL '3 years') AND firearea.discharge.quartile = 3 THEN 1 ELSE 0 END) > 0 AND
        SUM(CASE WHEN firearea.ANALYTE.date > firearea.ranges_agg.end_date AND firearea.ANALYTE.date <= (firearea.ranges_agg.end_date + INTERVAL '3 years') AND firearea.discharge.quartile = 4 THEN 1 ELSE 0 END) > 0
    )
    SELECT DISTINCT ON (usgs_site) *
    FROM fire_with_data
    ORDER BY usgs_site, cum_fire_area DESC
    ;
    
    CREATE INDEX INDEX_NAME_SITE ON MV_NAME(usgs_site);
    CREATE INDEX INDEX_NAME_DATE ON MV_NAME(start_date);
    $template$, 'ANALYTE', analyte_name);
    
    -- Replace placeholders
    sql_query := replace(sql_query, 'MV_NAME', mv_name);
    sql_query := replace(sql_query, 'INDEX_NAME_SITE', index_name_site);
    sql_query := replace(sql_query, 'INDEX_NAME_DATE', index_name_date);
    
    EXECUTE sql_query;
    
    RETURN FORMAT('SUCCESS: Materialized view %s created successfully with indexes', mv_name);
    
EXCEPTION
    WHEN OTHERS THEN
        RETURN FORMAT('ERROR: Failed to create materialized view for %s: %s', analyte_name, SQLERRM);
END;
$$;
```

## fn. export: summary all analyte sites

output columns:

| Column              | Description                                           |
|---------------------|-------------------------------------------------------|
| usgs_site           | Catchment/site identifier                             |
| year                | Fire year                                             |
| start_date          | Start date of fire period                             |
| end_date            | End date of fire period                               |
| previous_end_date   | End date of previous fire period                      |
| next_start_date     | Start date of next fire period                        |
| days_since          | Days since previous fire                              |
| days_until          | Days until next fire                                  |
| events              | Array of event IDs in this fire period                |
| cum_fire_area       | Cumulative burned area (km²) for the fire period      |
| catch_area          | Catchment area (km²)                                  |
| cum_per_cent_burned | Cumulative percent of catchment burned                |
| all_fire_area       | Total burned area (km²) in catchment through end_date |
| all_per_cent_burned | Percent of total catchment burned through end_date    |
| latitude            | Catchment centroid latitude                           |
| longitude           | Catchment centroid longitude                          |
| count_before_start  | Number of analyte observations before fire window     |
| count_after_end     | Number of analyte observations after fire window      |

``` sql
-- DROP FUNCTION IF EXISTS firearea.export_analyte_summary_all_sites(TEXT);
-- DROP FUNCTION IF EXISTS firearea.export_analyte_summary_all_sites(TEXT, TEXT);

CREATE OR REPLACE FUNCTION firearea.export_analyte_summary_all_sites(analyte_name TEXT, file_path TEXT DEFAULT NULL)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    sql_query TEXT;
    output_file TEXT;
    counts_table TEXT;
BEGIN
    -- Input validation
    IF analyte_name !~ '^[a-zA-Z_][a-zA-Z0-9_]*$' THEN
        RETURN FORMAT('ERROR: Invalid analyte name: %s', analyte_name);
    END IF;
    
    -- Set table and file names
    counts_table := FORMAT('firearea.%s_counts', analyte_name);
    output_file := COALESCE(file_path, FORMAT('/tmp/%s_summary.csv', analyte_name));
    
    -- Check if the counts table exists
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'firearea' AND table_name = FORMAT('%s_counts', analyte_name)
    ) THEN
        RETURN FORMAT('ERROR: Table %s does not exist', counts_table);
    END IF;
    
    -- Build and execute the query
    sql_query := FORMAT($template$
    COPY (
    SELECT
      ranges_agg.*,
      %s.count_before_start,
      %s.count_after_end
    FROM firearea.ranges_agg 
    JOIN %s ON 
      ranges_agg.usgs_site = %s.usgs_site AND
      (
        ARRAY(SELECT UNNEST(ranges_agg.events) ORDER BY 1) = 
        ARRAY(SELECT UNNEST(%s.events) ORDER BY 1)
      )
    ORDER BY
      ranges_agg.usgs_site,
      ranges_agg.start_date
    ) TO '%s' WITH CSV HEADER
    $template$, 
    counts_table, counts_table, counts_table, counts_table, counts_table, output_file);
    
    EXECUTE sql_query;
    
    RETURN FORMAT('SUCCESS: Exported %s summary to %s', analyte_name, output_file);
    
EXCEPTION
    WHEN OTHERS THEN
        RETURN FORMAT('ERROR: Failed to export %s summary: %s', analyte_name, SQLERRM);
END;
$$;

-- examples (default path is '/tmp')
-- SELECT firearea.export_analyte_summary_all_sites('nitrate'::TEXT);
-- SELECT firearea.export_analyte_summary_all_sites('nitrate'::TEXT, '/tmp/nitrate_summary.csv'::TEXT);
```

## fn. export: summary largest fire sites

purpose:

This function generates a query that retrieves comprehensive fire event
information for each watershed (`usgs_site`) based on the largest
wildfire that also meets analyte-discharge data sufficiency criteria. It
ensures that only those events selected for water quality analysis are
described in detail using the `ranges_agg` view.

context:

- The `ranges_agg` view aggregates fires by watershed and year,
  summarizing burn area, duration, spatial extent, and event IDs.
- The companion materialized view
  `firearea.largest_{analyte}_valid_fire_per_site` filters these to a
  single, largest valid fire per watershed with adequate analyte and
  discharge monitoring.
- This query joins the two views on `usgs_site`, `start_date`, and
  `end_date` to return only the selected fires.

query logic:

1.  Pull all rows from `firearea.ranges_agg`, which includes fire
    grouping metadata per site.
2.  Filter the results to only the fire windows (start + end dates)
    identified in the analyte validation materialized view.
3.  Return the entire record for each matched fire.

input tables:

- `firearea.ranges_agg`: View of grouped and summarized fire events by
  site.
- `firearea.largest_{analyte}_valid_fire_per_site`: Materialized view of
  sites with validated analyte+discharge data coverage.

output columns:

| Column              | Description                                           |
|---------------------|-------------------------------------------------------|
| usgs_site           | Catchment/site identifier                             |
| year                | Fire year                                             |
| start_date          | Start date of fire period                             |
| end_date            | End date of fire period                               |
| previous_end_date   | End date of previous fire period                      |
| next_start_date     | Start date of next fire period                        |
| days_since          | Days since previous fire                              |
| days_until          | Days until next fire                                  |
| events              | Array of event IDs in this fire period                |
| cum_fire_area       | Cumulative burned area (km²) for the fire period      |
| catch_area          | Catchment area (km²)                                  |
| cum_per_cent_burned | Cumulative percent of catchment burned                |
| all_fire_area       | Total burned area (km²) in catchment through end_date |
| all_per_cent_burned | Percent of total catchment burned through end_date    |
| latitude            | Catchment centroid latitude                           |
| longitude           | Catchment centroid longitude                          |

``` sql
CREATE OR REPLACE FUNCTION firearea.export_analyte_largest_fire_sites(analyte_name TEXT, file_path TEXT DEFAULT NULL)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    sql_query TEXT;
    output_file TEXT;
    largest_fire_table TEXT;
    table_exists BOOLEAN;
BEGIN
    -- Input validation
    IF analyte_name !~ '^[a-zA-Z_][a-zA-Z0-9_]*$' THEN
        RETURN FORMAT('ERROR: Invalid analyte name: %s', analyte_name);
    END IF;
    
    -- Set table and file names
    largest_fire_table := FORMAT('firearea.largest_%s_valid_fire_per_site', analyte_name);
    output_file := COALESCE(file_path, FORMAT('/tmp/%s_sites_fires.csv', analyte_name));
    
    -- Check if the largest fire materialized view exists
    SELECT EXISTS (
        SELECT 1 FROM pg_matviews 
        WHERE schemaname = 'firearea' AND matviewname = FORMAT('largest_%s_valid_fire_per_site', analyte_name)
    ) INTO table_exists;
    
    IF NOT table_exists THEN
        RETURN FORMAT('ERROR: Materialized view %s does not exist', largest_fire_table);
    END IF;
    
    -- Build and execute the query
    sql_query := FORMAT($template$
    COPY (
    SELECT
      ranges_agg.*
    FROM firearea.ranges_agg
    JOIN %s
      ON ranges_agg.usgs_site   = %s.usgs_site
      AND ranges_agg.start_date = %s.start_date
      AND ranges_agg.end_date   = %s.end_date
    ORDER BY
      ranges_agg.usgs_site,
      ranges_agg.start_date
    ) TO '%s' WITH CSV HEADER
    $template$, 
    largest_fire_table, largest_fire_table, largest_fire_table, largest_fire_table, output_file);
    
    EXECUTE sql_query;
    
    RETURN FORMAT('SUCCESS: Exported %s largest fire sites to %s', analyte_name, output_file);
    
EXCEPTION
    WHEN OTHERS THEN
        RETURN FORMAT('ERROR: Failed to export %s largest fire sites: %s', analyte_name, SQLERRM);
END;
$$;

-- Use default path
-- SELECT firearea.export_analyte_largest_fire_sites('nitrate'::TEXT);

-- Use custom path  
-- SELECT firearea.export_analyte_largest_fire_sites('nitrate'::TEXT, '/home/user/data/nitrate_largest_fires.csv'::TEXT);

-- For spcond
-- SELECT firearea.export_analyte_largest_fire_sites('spcond'::TEXT);

-- For any future analyte
-- SELECT firearea.export_analyte_largest_fire_sites('phosphate'::TEXT, '/data/phosphate_fires.csv'::TEXT);
```

## fn. export: q+c pre-post-quartiles

synopsis:

This function generates a query (`{analyte}_q_upper_quartiles`) that
extracts paired analyte and discharge observations from USGS and
non-USGS monitoring sites focusing on the periods before and after fire.
It applies **strict filtering**.

- **Joins**: Combines standardized analyte data (`firearea.{analyte}`),
  combined discharge data (`firearea.discharge`), and fire metadata
  (`firearea.ranges_agg`).
- **Time Windows**: Selects observations within 3 years before each fire
  window’s `start_date` and 3 years after each `end_date`.
- **Segment Labeling**: Labels each observation as `'before'` or
  `'after'` the fire window.
- **Strict Inclusion Criteria**: For each site/fire window, requires:
  - analyte–discharge observations in both the 3-year pre- and post-fire
    windows
  - analyte–discharge observations must include flow quartiles 2, 3, and
    4 in both windows
- **Output**: Returns all qualifying observations, along with counts and
  quartile information.

processing note:

Filtering the observations to include flow spanning quartiles 2-3 in
each of the windows before and after a fire is a heavy lift
computationally. This lift is addressed by the materialized view of the
largest fire when we are filtering by both quartiles and selecting only
the largest fire in a catchment (e.g., see [here]()). As a result, the
query to select observations where we filter to the largest fire is much
faster. So, if there is interest to explore patterns other than only
those associated with the largest fire in a catchment, consider
offloading the filtering of flow quartiles to another table,
materialized view, or even make this query a table or materialized view.

output:

| Column | Description |
|----|----|
| `usgs_site` | USGS site ID |
| `year` | Fire year from `ranges_agg` |
| `start_date` | Fire window start date |
| `end_date` | Fire window end date |
| `segment` | `'before'` or `'after'` fire window, indicating observation timing |
| `date` | Observation date |
| `value_std` | Standardized analyte concentration |
| `"Flow"` | Daily discharge value |
| `quartile` | Discharge quartile (1–4) for the observation |
| `before_count` | Number of valid analyte–discharge observations in the 3 years before fire |
| `after_count` | Number of valid analyte–discharge observations in the 3 years after fire |

The query result is saved as:
`{analyte}_discharge_data_filtered_quartiles_234.csv`

``` sql
CREATE OR REPLACE FUNCTION firearea.export_analyte_q_pre_post_quartiles(analyte_name TEXT, file_path TEXT DEFAULT NULL)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    sql_query TEXT;
    output_file TEXT;
    analyte_table TEXT;
BEGIN
    -- Input validation
    IF analyte_name !~ '^[a-zA-Z_][a-zA-Z0-9_]*$' THEN
        RETURN FORMAT('ERROR: Invalid analyte name: %s', analyte_name);
    END IF;
    
    -- Set table and file names
    analyte_table := FORMAT('firearea.%s', analyte_name);
    output_file := COALESCE(file_path, FORMAT('/tmp/%s_discharge_data_filtered_quartiles_234.csv', analyte_name));
    
    -- Check if the analyte table exists
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'firearea' AND table_name = analyte_name
    ) THEN
        RETURN FORMAT('ERROR: Table %s does not exist', analyte_table);
    END IF;
    
    -- Build and execute the query
    sql_query := FORMAT($template$
    COPY (
    SELECT
      ANALYTE.usgs_site,
      ranges_agg.year,
      ranges_agg.start_date,
      ranges_agg.end_date,
      CASE
        WHEN ANALYTE.date < ranges_agg.start_date THEN 'before'
        WHEN ANALYTE.date > ranges_agg.end_date THEN 'after'
      END AS segment,
      ANALYTE.date,
      ANALYTE.value_std,
      discharge."Flow",
      discharge.quartile,
      counts.before_count,
      counts.after_count
    FROM %s
    JOIN firearea.discharge
      ON ANALYTE.usgs_site = discharge.usgs_site
      AND ANALYTE.date = discharge."Date"
    JOIN firearea.ranges_agg
      ON ANALYTE.usgs_site = ranges_agg.usgs_site
      AND (
        (ANALYTE.date >= (ranges_agg.start_date - INTERVAL '3 years') AND ANALYTE.date < ranges_agg.start_date)
        OR
        (ANALYTE.date > ranges_agg.end_date AND ANALYTE.date <= (ranges_agg.end_date + INTERVAL '3 years'))
      )
    JOIN (
      SELECT
        ANALYTE.usgs_site,
        ranges_agg.year,
        ranges_agg.start_date,
        ranges_agg.end_date,
        COUNT(CASE WHEN ANALYTE.date >= (ranges_agg.start_date - INTERVAL '3 years') AND ANALYTE.date < ranges_agg.start_date THEN 1 END) AS before_count,
        COUNT(CASE WHEN ANALYTE.date > ranges_agg.end_date AND ANALYTE.date <= (ranges_agg.end_date + INTERVAL '3 years') THEN 1 END) AS after_count
      FROM %s
      JOIN firearea.discharge
        ON ANALYTE.usgs_site = discharge.usgs_site
        AND ANALYTE.date = discharge."Date"
      JOIN firearea.ranges_agg
        ON ANALYTE.usgs_site = ranges_agg.usgs_site
      WHERE ANALYTE.value_std IS NOT NULL
        AND discharge."Flow" IS NOT NULL
        AND discharge.quartile IS NOT NULL
      GROUP BY
        ANALYTE.usgs_site,
        ranges_agg.year,
        ranges_agg.start_date,
        ranges_agg.end_date
      HAVING
        -- quartile 2, 3, and 4 must be present in before window
        SUM(CASE WHEN ANALYTE.date >= (ranges_agg.start_date - INTERVAL '3 years') AND ANALYTE.date < ranges_agg.start_date AND discharge.quartile = 2 THEN 1 ELSE 0 END) > 0
        AND
        SUM(CASE WHEN ANALYTE.date >= (ranges_agg.start_date - INTERVAL '3 years') AND ANALYTE.date < ranges_agg.start_date AND discharge.quartile = 3 THEN 1 ELSE 0 END) > 0
        AND
        SUM(CASE WHEN ANALYTE.date >= (ranges_agg.start_date - INTERVAL '3 years') AND ANALYTE.date < ranges_agg.start_date AND discharge.quartile = 4 THEN 1 ELSE 0 END) > 0
        -- quartile 2, 3, and 4 must be present in after window
        AND
        SUM(CASE WHEN ANALYTE.date > ranges_agg.end_date AND ANALYTE.date <= (ranges_agg.end_date + INTERVAL '3 years') AND discharge.quartile = 2 THEN 1 ELSE 0 END) > 0
        AND
        SUM(CASE WHEN ANALYTE.date > ranges_agg.end_date AND ANALYTE.date <= (ranges_agg.end_date + INTERVAL '3 years') AND discharge.quartile = 3 THEN 1 ELSE 0 END) > 0
        AND
        SUM(CASE WHEN ANALYTE.date > ranges_agg.end_date AND ANALYTE.date <= (ranges_agg.end_date + INTERVAL '3 years') AND discharge.quartile = 4 THEN 1 ELSE 0 END) > 0
    ) AS counts
      ON ANALYTE.usgs_site = counts.usgs_site
      AND ranges_agg.year = counts.year
      AND ranges_agg.start_date = counts.start_date
      AND ranges_agg.end_date = counts.end_date
    WHERE ANALYTE.value_std IS NOT NULL
      AND discharge."Flow" IS NOT NULL
      AND discharge.quartile IS NOT NULL
      AND (
        (ANALYTE.date >= (ranges_agg.start_date - INTERVAL '3 years') AND ANALYTE.date < ranges_agg.start_date)
        OR
        (ANALYTE.date > ranges_agg.end_date AND ANALYTE.date <= (ranges_agg.end_date + INTERVAL '3 years'))
      )
    ORDER BY
      ANALYTE.usgs_site,
      ranges_agg.year,
      segment DESC,
      ANALYTE.date
    ) TO '%s' WITH CSV HEADER
    $template$, 
    analyte_table, analyte_table, output_file);
    
    -- Replace ANALYTE placeholders with the actual analyte name
    sql_query := replace(sql_query, 'ANALYTE', analyte_name);
    
    EXECUTE sql_query;
    
    RETURN FORMAT('SUCCESS: Exported %s pre-post quartiles data to %s', analyte_name, output_file);
    
EXCEPTION
    WHEN OTHERS THEN
        RETURN FORMAT('ERROR: Failed to export %s pre-post quartiles data: %s', analyte_name, SQLERRM);
END;
$$;

-- Use default path
-- SELECT firearea.export_analyte_q_pre_post_quartiles('nitrate'::TEXT);

-- Use custom path  
-- SELECT firearea.export_analyte_q_pre_post_quartiles('nitrate'::TEXT, '/home/user/data/nitrate_q_quartiles.csv'::TEXT);

-- For spcond
-- SELECT firearea.export_analyte_q_pre_post_quartiles('spcond'::TEXT);

-- For any future analyte
-- SELECT firearea.export_analyte_q_pre_post_quartiles('phosphate'::TEXT, '/data/phosphate_quartiles.csv'::TEXT);
```

## fn. export: q+c pre-post-quartiles largest fire

purpose:

This query extracts analyte and discharge observations from USGS
watershed sites surrounding wildfires. It isolates data for only the
largest valid fire per watershed, where validity is defined by the
presence of adequate water quality monitoring data before and after the
fire.

key objectives:

- Assess hydrologic and water quality response (analyte + flow) to
  wildfire disturbances.
- Limit analysis to only the most impactful fire per watershed, based on
  fire area (`cum_fire_area`).
- ensure fire events are sufficiently monitored, with:
  - At least 3 years of data before and after the fire.
  - Presence of streamflow across flow quartiles 2, 3, and 4 in both
    windows.

core logic steps:

1.  Join analyte and discharge records by site and date.
2.  Filter for valid data windows:
    - Records must fall within 3 years before or after each fire.
    - Each fire must have discharge records in quartiles 2–4 in both
      windows.
3.  Select valid fires per watershed from `ranges_agg`, ensuring they
    meet all data coverage criteria.
4.  Identify the largest fire per `usgs_site` by selecting the maximum
    `cum_fire_area` among valid events.
5.  Return analyte + discharge records for the selected fire per
    watershed, with an additional field (`segment`) labeling records as
    “before” or “after” the fire.

processing note:

When export_analyte_q_pre_post_quartiles_largest_fire joins to
`largest_{analyte}_valid_fire_per_site`:

1.  The materialized view only contains fires that have already passed
    the quartile filtering
2.  The join on usgs_site will only return data for sites/fires that met
    the criteria
3.  The date filtering in the export function ensures only the 3-year
    windows around those validated fires are included

As a result of \#1 and \#2 above, this query is very fast relative to
only querying the quartiles alone (e.g, [here]()).

inputs:

- `firearea.{analyte}`: View of USGS and non-USGS analyte observations
- `firearea.discharge`: Daily streamflow and derived quartile
  classification
- `firearea.ranges_agg`: Pre-aggregated wildfire periods and fire area
  metrics per watershed

outputs:

| column name    | description                                      |
|----------------|--------------------------------------------------|
| `usgs_site`    | Site identifier                                  |
| `year`         | Fire event year                                  |
| `start_date`   | Fire event start date                            |
| `end_date`     | Fire event end date                              |
| `segment`      | Temporal label: `'before'` or `'after'` fire     |
| `date`         | analyte/discharge observation date               |
| `value_std`    | Standardized analyte concentration               |
| `"Flow"`       | Discharge (cfs)                                  |
| `quartile`     | Discharge flow quartile (1–4)                    |
| `before_count` | Count of observations in 3 years before the fire |
| `after_count`  | Count of observations in 3 years after the fire  |

The query result is saved as:
`{analyte}_discharge_quartiles_234_max_fire.csv`

``` sql
CREATE OR REPLACE FUNCTION firearea.export_analyte_q_pre_post_quartiles_largest_fire(analyte_name TEXT, file_path TEXT DEFAULT NULL)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    sql_query TEXT;
    output_file TEXT;
    analyte_table TEXT;
    largest_fire_table TEXT;
    query_template TEXT := $template$
    COPY (
    SELECT
      firearea.ANALYTE.usgs_site,
      firearea.LARGEST_FIRE_TABLE.year,
      firearea.LARGEST_FIRE_TABLE.start_date,
      firearea.LARGEST_FIRE_TABLE.end_date,
      CASE
        WHEN firearea.ANALYTE.date < firearea.LARGEST_FIRE_TABLE.start_date THEN 'before'
        WHEN firearea.ANALYTE.date > firearea.LARGEST_FIRE_TABLE.end_date THEN 'after'
      END AS segment,
      firearea.ANALYTE.date,
      firearea.ANALYTE.value_std,
      firearea.discharge."Flow",
      firearea.discharge.quartile,
      firearea.LARGEST_FIRE_TABLE.before_count,
      firearea.LARGEST_FIRE_TABLE.after_count
    FROM firearea.ANALYTE
    JOIN firearea.discharge
      ON firearea.ANALYTE.usgs_site = firearea.discharge.usgs_site
      AND firearea.ANALYTE.date = firearea.discharge."Date"
    JOIN firearea.LARGEST_FIRE_TABLE
      ON firearea.ANALYTE.usgs_site = firearea.LARGEST_FIRE_TABLE.usgs_site
    WHERE firearea.ANALYTE.value_std IS NOT NULL
      AND firearea.discharge."Flow" IS NOT NULL
      AND firearea.discharge.quartile IS NOT NULL
      AND (
        (firearea.ANALYTE.date >= (firearea.LARGEST_FIRE_TABLE.start_date - INTERVAL '3 years') AND firearea.ANALYTE.date < firearea.LARGEST_FIRE_TABLE.start_date)
        OR
        (firearea.ANALYTE.date > firearea.LARGEST_FIRE_TABLE.end_date AND firearea.ANALYTE.date <= (firearea.LARGEST_FIRE_TABLE.end_date + INTERVAL '3 years'))
      )
    ORDER BY
      ANALYTE.usgs_site,
      LARGEST_FIRE_TABLE.year,
      segment DESC,
      ANALYTE.date
    ) TO 'OUTPUT_FILE' WITH CSV HEADER
    $template$;
BEGIN
    -- Input validation
    IF analyte_name !~ '^[a-zA-Z_][a-zA-Z0-9_]*$' THEN
        RETURN FORMAT('ERROR: Invalid analyte name: %s', analyte_name);
    END IF;
    
    -- Set table and file names
    analyte_table := analyte_name;
    largest_fire_table := FORMAT('largest_%s_valid_fire_per_site', analyte_name);
    output_file := COALESCE(file_path, FORMAT('/tmp/%s_discharge_quartiles_234_max_fire.csv', analyte_name));
    
    -- Check if the analyte table exists
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'firearea' AND table_name = analyte_name
    ) THEN
        RETURN FORMAT('ERROR: Table firearea.%s does not exist', analyte_name);
    END IF;
    
    -- Check if the largest fire materialized view exists
    IF NOT EXISTS (
        SELECT 1 FROM pg_matviews 
        WHERE schemaname = 'firearea' AND matviewname = largest_fire_table
    ) THEN
        RETURN FORMAT('ERROR: Materialized view firearea.%s does not exist', largest_fire_table);
    END IF;
    
    -- Replace all placeholders in the template
    sql_query := replace(query_template, 'ANALYTE', analyte_table);
    sql_query := replace(sql_query, 'LARGEST_FIRE_TABLE', largest_fire_table);
    sql_query := replace(sql_query, 'OUTPUT_FILE', output_file);
    
    EXECUTE sql_query;
    
    RETURN FORMAT('SUCCESS: Exported %s pre-post quartiles largest fire data to %s', analyte_name, output_file);
    
EXCEPTION
    WHEN OTHERS THEN
        RETURN FORMAT('ERROR: Failed to export %s pre-post quartiles largest fire data: %s', analyte_name, SQLERRM);
END;
$$;

-- filepath output is hardcoded to /tmp
-- SELECT firearea.export_analyte_q_pre_post_quartiles_largest_fire('nitrate'::TEXT);
```

## fn. export: q+c pre-quartiles largest fire

purpose:

This query retrieves analyte and discharge records for USGS watershed
sites (`usgs_site`) surrounding wildfires. It ensures strong sampling
coverage before the fire, requiring observations in flow quartiles 2, 3,
and 4, while placing no constraint on post-fire sampling coverage.

\*\* It is important to note that the results of this query are NOT a
superset of the results of the query where we are also constraining the
post-fire values to having representative values from quartiles 2, 3,
and 4. The reason is that largest fire for a given site is not
necessarily the same across the two queries.

For example, the largest fire in catchment USGS-06259000 for *nitrate*
occurred on 2011-07-22 when both before and after fire windows must
contain quartiles 2, 3, and 4 but on 2021-08-19 when only the pre-fire
data needed to reflect values in quartiles 2, 3, and 4. \*\*

Note that we are not using these results for analyses but serves as the
base for subsequent queries to pull discharge and fire details for the
sites.

context:

- This approach enables inclusion of fires where post-fire data may be
  sparse, but pre-disturbance flow conditions are well characterized.
- It is ideal for analyses focused on establishing a robust pre-fire
  baseline for analyte concentrations.

query logic:

1.  Join `firearea.{analyte}`, `firearea.discharge`, and
    `firearea.ranges_agg`.
2.  Identify all fire windows per site (`usgs_site`) and evaluate 3-year
    sampling windows:
    - Pre-fire window must contain observations in flow quartiles 2, 3,
      and 4. Post-fire window is included regardless of flow
      distribution.
3.  From qualifying fire windows, select the largest fire event per site
    using `cum_fire_area DESC`.
4.  Return analyte–discharge samples from the 3-year pre- and post-fire
    windows, with labeled segment (`'before'` or `'after'`).

input tables:

- `firearea.{analyte}`: Standardized analyte observations by site and
  date.
- `firearea.discharge`: Discharge flow volume and quartile per site and
  date.
- `firearea.ranges_agg`: Aggregated fire events per site, with timing
  and spatial metrics.

output columns:

| Column Name    | Description                            |
|----------------|----------------------------------------|
| `usgs_site`    | Watershed site identifier              |
| `year`         | Year of the fire event group           |
| `start_date`   | Start of fire window                   |
| `end_date`     | End of fire window                     |
| `segment`      | `'before'` or `'after'` the fire event |
| `date`         | Observation date                       |
| `value_std`    | Standardized analyte concentration     |
| `"Flow"`       | Discharge volume (cfs)                 |
| `quartile`     | Flow quartile category (1–4)           |
| `before_count` | Total records in pre-fire window       |
| `after_count`  | Total records in post-fire window      |

The query result is saved as:
`{analyte}_discharge_before_quartiles_234_max_fire.csv`

``` sql
CREATE OR REPLACE FUNCTION firearea.export_analyte_q_pre_quartiles_largest_fire(analyte_name TEXT, file_path TEXT DEFAULT NULL)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    sql_query TEXT;
    output_file TEXT;
    query_template TEXT := $template$
    COPY (
    WITH fire_with_data AS (
      SELECT
        firearea.ANALYTE.usgs_site,
        firearea.ranges_agg.year,
        firearea.ranges_agg.start_date,
        firearea.ranges_agg.end_date,
        firearea.ranges_agg.cum_fire_area,
        COUNT(CASE WHEN firearea.ANALYTE.date >= (firearea.ranges_agg.start_date - INTERVAL '3 years')
                   AND firearea.ANALYTE.date < firearea.ranges_agg.start_date THEN 1 END) AS before_count,
        COUNT(CASE WHEN firearea.ANALYTE.date > firearea.ranges_agg.end_date
                   AND firearea.ANALYTE.date <= (firearea.ranges_agg.end_date + INTERVAL '3 years') THEN 1 END) AS after_count,
        SUM(CASE WHEN firearea.ANALYTE.date >= (firearea.ranges_agg.start_date - INTERVAL '3 years')
                  AND firearea.ANALYTE.date < firearea.ranges_agg.start_date AND firearea.discharge.quartile = 2 THEN 1 ELSE 0 END) AS bq2,
        SUM(CASE WHEN firearea.ANALYTE.date >= (firearea.ranges_agg.start_date - INTERVAL '3 years')
                  AND firearea.ANALYTE.date < firearea.ranges_agg.start_date AND firearea.discharge.quartile = 3 THEN 1 ELSE 0 END) AS bq3,
        SUM(CASE WHEN firearea.ANALYTE.date >= (firearea.ranges_agg.start_date - INTERVAL '3 years')
                  AND firearea.ANALYTE.date < firearea.ranges_agg.start_date AND firearea.discharge.quartile = 4 THEN 1 ELSE 0 END) AS bq4
      FROM firearea.ANALYTE
      JOIN firearea.discharge
        ON firearea.ANALYTE.usgs_site = firearea.discharge.usgs_site
        AND firearea.ANALYTE.date = firearea.discharge."Date"
      JOIN firearea.ranges_agg
        ON firearea.ANALYTE.usgs_site = firearea.ranges_agg.usgs_site
      WHERE firearea.ANALYTE.value_std IS NOT NULL
        AND firearea.discharge."Flow" IS NOT NULL
        AND firearea.discharge.quartile IS NOT NULL
      GROUP BY
        firearea.ANALYTE.usgs_site,
        firearea.ranges_agg.year,
        firearea.ranges_agg.start_date,
        firearea.ranges_agg.end_date,
        firearea.ranges_agg.cum_fire_area
      HAVING
        SUM(CASE WHEN firearea.ANALYTE.date >= (firearea.ranges_agg.start_date - INTERVAL '3 years')
                  AND firearea.ANALYTE.date < firearea.ranges_agg.start_date AND firearea.discharge.quartile = 2 THEN 1 ELSE 0 END) > 0 AND
        SUM(CASE WHEN firearea.ANALYTE.date >= (firearea.ranges_agg.start_date - INTERVAL '3 years')
                  AND firearea.ANALYTE.date < firearea.ranges_agg.start_date AND firearea.discharge.quartile = 3 THEN 1 ELSE 0 END) > 0 AND
        SUM(CASE WHEN firearea.ANALYTE.date >= (firearea.ranges_agg.start_date - INTERVAL '3 years')
                  AND firearea.ANALYTE.date < firearea.ranges_agg.start_date AND firearea.discharge.quartile = 4 THEN 1 ELSE 0 END) > 0
    )
    SELECT
      firearea.ANALYTE.usgs_site,
      fire_with_data.year,
      fire_with_data.start_date,
      fire_with_data.end_date,
      CASE
        WHEN firearea.ANALYTE.date < fire_with_data.start_date THEN 'before'
        WHEN firearea.ANALYTE.date > fire_with_data.end_date THEN 'after'
      END AS segment,
      firearea.ANALYTE.date,
      firearea.ANALYTE.value_std,
      firearea.discharge."Flow",
      firearea.discharge.quartile,
      fire_with_data.before_count,
      fire_with_data.after_count
    FROM firearea.ANALYTE
    JOIN firearea.discharge
      ON firearea.ANALYTE.usgs_site = firearea.discharge.usgs_site
      AND firearea.ANALYTE.date = firearea.discharge."Date"
    JOIN (
      SELECT DISTINCT ON (fire_with_data.usgs_site)
        fire_with_data.usgs_site,
        fire_with_data.year,
        fire_with_data.start_date,
        fire_with_data.end_date>,
        fire_with_data.before_count,
        fire_with_data.after_count
      FROM fire_with_data
      ORDER BY fire_with_data.usgs_site, fire_with_data.cum_fire_area DESC
    ) AS fire_with_data
      ON firearea.ANALYTE.usgs_site = fire_with_data.usgs_site
    WHERE firearea.ANALYTE.value_std IS NOT NULL
      AND firearea.discharge."Flow" IS NOT NULL
      AND firearea.discharge.quartile IS NOT NULL
      AND (
        (firearea.ANALYTE.date >= (fire_with_data.start_date - INTERVAL '3 years') AND firearea.ANALYTE.date < fire_with_data.start_date)
        OR
        (firearea.ANALYTE.date > fire_with_data.end_date AND firearea.ANALYTE.date <= (fire_with_data.end_date + INTERVAL '3 years'))
      )
    ORDER BY
      ANALYTE.usgs_site,
      fire_with_data.year,
      segment DESC,
      ANALYTE.date
    ) TO 'OUTPUT_FILE' WITH CSV HEADER
    $template$;
BEGIN
    -- Input validation
    IF analyte_name !~ '^[a-zA-Z_][a-zA-Z0-9_]*$' THEN
        RETURN FORMAT('ERROR: Invalid analyte name: %s', analyte_name);
    END IF;
    
    -- Set file name
    output_file := COALESCE(file_path, FORMAT('/tmp/%s_discharge_before_quartiles_234_max_fire.csv', analyte_name));
    
    -- Check if the analyte table exists
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'firearea' AND table_name = analyte_name
    ) THEN
        RETURN FORMAT('ERROR: Table firearea.%s does not exist', analyte_name);
    END IF;
    
    -- Replace all placeholders in the template
    sql_query := replace(query_template, 'ANALYTE', analyte_name);
    sql_query := replace(sql_query, 'OUTPUT_FILE', output_file);
    
    EXECUTE sql_query;
    
    RETURN FORMAT('SUCCESS: Exported %s pre-quartiles largest fire data to %s', analyte_name, output_file);
    
EXCEPTION
    WHEN OTHERS THEN
        RETURN FORMAT('ERROR: Failed to export %s pre-quartiles largest fire data: %s', analyte_name, SQLERRM);
END;
$$;


-- Use default path
-- SELECT firearea.export_analyte_q_pre_quartiles_largest_fire('nitrate'::TEXT);

-- Use custom path  
-- SELECT firearea.export_analyte_q_pre_quartiles_largest_fire('nitrate'::TEXT, '/home/user/data/nitrate_pre_quartiles_max_fire.csv'::TEXT);

-- For spcond
-- SELECT firearea.export_analyte_q_pre_quartiles_largest_fire('spcond'::TEXT);

-- For any future analyte
-- SELECT firearea.export_analyte_q_pre_quartiles_largest_fire('phosphate'::TEXT, '/data/phosphate_pre_quartiles.csv'::TEXT);
```
