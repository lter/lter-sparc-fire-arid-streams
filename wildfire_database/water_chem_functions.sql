-- Generated from water_chem_functions.qmd
-- Execute all functions in a single transaction

BEGIN;

-- chunk_1
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

-- chunk_2
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

-- chunk_3
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

-- chunk_4
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

-- chunk_5
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

-- chunk_6
CREATE OR REPLACE FUNCTION firearea.create_evi_join_largest_analyte_mv(analyte TEXT)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
  mv_name TEXT;
  lf_mv_name TEXT;
  result_msg TEXT;
BEGIN
  -- Resolve target MV and dependency names deterministically
  mv_name := format('evi_join_largest_%s', analyte);
  lf_mv_name := format('largest_%s_valid_fire_per_site', analyte);

  -- Validate dependency exists
  IF NOT EXISTS (
    SELECT 1
    FROM pg_matviews
    WHERE schemaname = 'firearea' AND matviewname = lf_mv_name
  ) THEN
    RAISE EXCEPTION 'Dependency firearea.% does not exist. Build it first via create_largest_analyte_valid_fire_per_site_mv(%).', lf_mv_name, analyte;
  END IF;

  -- Drop target MV and indexes if present
  EXECUTE format('DROP MATERIALIZED VIEW IF EXISTS firearea.%I CASCADE', mv_name);

  -- Create MV: join EVI to largest valid fire event sets for the analyte
  EXECUTE format($fmt$ 
    CREATE MATERIALIZED VIEW firearea.%I AS
    WITH lf AS (
      SELECT
        usgs_site,
        events,
        year,
        start_date,
        end_date
      FROM firearea.%I
    ),
    lf_events AS (
      SELECT
        usgs_site,
        year,
        start_date,
        end_date,
        ARRAY(SELECT unnest(events) ORDER BY 1) AS events_sorted,
        cardinality(events) AS event_count
      FROM lf
    ),
    evi_validated AS (
      SELECT
        evi.usgs_site,
        evi.event_id,
        btrim(evi.median_post_evi) AS median_post_evi_trim,
        CASE
          WHEN btrim(evi.median_post_evi) IS NULL OR btrim(evi.median_post_evi) = '' THEN FALSE
          WHEN btrim(evi.median_post_evi) ~ $re$^[+-]?([0-9]*\.?[0-9]+)([eE][+-]?[0-9]+)?$re$ THEN TRUE
          ELSE FALSE
        END AS is_numeric
      FROM firearea.evi AS evi
    ),
    joined AS (
      SELECT
        l.usgs_site,
        l.year,
        l.start_date,
        l.end_date,
        l.events_sorted,
        l.event_count,
        ev.event_id,
        ev.median_post_evi_trim,
        ev.is_numeric
      FROM lf_events AS l
      JOIN evi_validated AS ev
        ON ev.usgs_site = l.usgs_site
       AND ev.event_id = ANY(l.events_sorted)
    ),
    per_group AS (
      SELECT
        usgs_site,
        year,
        start_date,
        end_date,
        events_sorted AS events,
        event_count,
        COUNT(*) FILTER (WHERE is_numeric) AS numeric_count,
        COUNT(*) FILTER (WHERE NOT is_numeric OR median_post_evi_trim IS NULL OR median_post_evi_trim = '') AS bad_or_missing_count,
        AVG(median_post_evi_trim::DOUBLE PRECISION) FILTER (WHERE is_numeric) AS avg_median_post_evi
      FROM joined
      GROUP BY usgs_site, year, start_date, end_date, events_sorted, event_count
    )
    SELECT
      usgs_site,
      events,
      year,
      start_date,
      end_date,
      avg_median_post_evi,
      (bad_or_missing_count > 0 AND bad_or_missing_count <> event_count) OR (bad_or_missing_count = event_count) AS has_missing_evi,
      (bad_or_missing_count > 0 AND numeric_count > 0) AS has_bad_data
    FROM per_group
    ORDER BY usgs_site, start_date, end_date;
  $fmt$, mv_name, lf_mv_name);

  -- Indexes for common filters
  EXECUTE format('CREATE INDEX IF NOT EXISTS %I_usgs_site_idx ON firearea.%I (usgs_site)', mv_name, mv_name);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %I_start_date_idx ON firearea.%I (start_date)', mv_name, mv_name);

  result_msg := format('SUCCESS: Created firearea.%s and indexes', mv_name);
  RAISE NOTICE '%', result_msg;
  RETURN result_msg;

EXCEPTION
  WHEN OTHERS THEN
    RAISE EXCEPTION 'FAILED: Error creating EVI join MV for analyte %: %', analyte, SQLERRM;
END;
$$;

-- Examples:
-- SELECT firearea.create_evi_join_largest_analyte_mv('nitrate');
-- SELECT firearea.create_evi_join_largest_analyte_mv('ammonium');
-- SELECT firearea.create_evi_join_largest_analyte_mv('spcond');
-- SELECT firearea.create_evi_join_largest_analyte_mv('orthop');

-- chunk_7
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

-- chunk_8
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

-- chunk_9
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

-- chunk_10
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

-- chunk_11
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

-- chunk_12
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

-- chunk_13
CREATE OR REPLACE FUNCTION firearea.create_analyte_q_pre_post_quartiles_largest_fire_mv(analyte_name TEXT)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    mv_name TEXT;
    analyte_view TEXT;
    largest_fire_mv TEXT;
    idx_site TEXT;
    idx_start TEXT;
    sql TEXT;
    ok_analyte BOOLEAN;
    ok_largest BOOLEAN;
BEGIN
    IF analyte_name !~ '^[a-zA-Z_][a-zA-Z0-9_]*$' THEN
        RETURN FORMAT('ERROR: Invalid analyte name: %s', analyte_name);
    END IF;
    analyte_view := analyte_name; -- firearea.<analyte>
    largest_fire_mv := FORMAT('largest_%s_valid_fire_per_site', analyte_name);
    mv_name := FORMAT('%s_q_pre_post_quartiles_largest_fire', analyte_name);
    idx_site := FORMAT('idx_%s_qpp_lf_usgs_site', analyte_name);
    idx_start := FORMAT('idx_%s_qpp_lf_start_date', analyte_name);

    SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema='firearea' AND table_name=analyte_view) INTO ok_analyte;
    IF NOT ok_analyte THEN
        RETURN FORMAT('ERROR: Missing analyte view firearea.%s', analyte_view);
    END IF;
    SELECT EXISTS(SELECT 1 FROM pg_matviews WHERE schemaname='firearea' AND matviewname=largest_fire_mv) INTO ok_largest;
    IF NOT ok_largest THEN
        RETURN FORMAT('ERROR: Missing largest fire matview firearea.%s', largest_fire_mv);
    END IF;

  -- Build MV only; create indexes in separate EXECUTEs to avoid ambiguity
  sql := FORMAT($f$
    DROP MATERIALIZED VIEW IF EXISTS firearea.%I CASCADE;
    CREATE MATERIALIZED VIEW firearea.%I AS
    WITH counts AS (
      SELECT
        a.usgs_site,
        l.year,
        l.start_date,
        l.end_date,
        COUNT(CASE WHEN a.date >= (l.start_date - INTERVAL '3 years') AND a.date < l.start_date THEN 1 END) AS before_count,
        COUNT(CASE WHEN a.date > l.end_date AND a.date <= (l.end_date + INTERVAL '3 years') THEN 1 END) AS after_count,
        SUM(CASE WHEN a.date >= (l.start_date - INTERVAL '3 years') AND a.date < l.start_date AND d.quartile=2 THEN 1 ELSE 0 END) AS bq2,
        SUM(CASE WHEN a.date >= (l.start_date - INTERVAL '3 years') AND a.date < l.start_date AND d.quartile=3 THEN 1 ELSE 0 END) AS bq3,
        SUM(CASE WHEN a.date >= (l.start_date - INTERVAL '3 years') AND a.date < l.start_date AND d.quartile=4 THEN 1 ELSE 0 END) AS bq4,
        SUM(CASE WHEN a.date > l.end_date AND a.date <= (l.end_date + INTERVAL '3 years') AND d.quartile=2 THEN 1 ELSE 0 END) AS aq2,
        SUM(CASE WHEN a.date > l.end_date AND a.date <= (l.end_date + INTERVAL '3 years') AND d.quartile=3 THEN 1 ELSE 0 END) AS aq3,
        SUM(CASE WHEN a.date > l.end_date AND a.date <= (l.end_date + INTERVAL '3 years') AND d.quartile=4 THEN 1 ELSE 0 END) AS aq4
      FROM firearea.%I a
      JOIN firearea.discharge d ON (a.usgs_site=d.usgs_site AND a.date=d."Date")
      JOIN firearea.%I l ON (a.usgs_site=l.usgs_site)
      WHERE a.value_std IS NOT NULL AND d."Flow" IS NOT NULL AND d.quartile IS NOT NULL
      GROUP BY a.usgs_site,l.year,l.start_date,l.end_date
      HAVING
        SUM(CASE WHEN a.date >= (l.start_date - INTERVAL '3 years') AND a.date < l.start_date AND d.quartile=2 THEN 1 ELSE 0 END) > 0 AND
        SUM(CASE WHEN a.date >= (l.start_date - INTERVAL '3 years') AND a.date < l.start_date AND d.quartile=3 THEN 1 ELSE 0 END) > 0 AND
        SUM(CASE WHEN a.date >= (l.start_date - INTERVAL '3 years') AND a.date < l.start_date AND d.quartile=4 THEN 1 ELSE 0 END) > 0 AND
        SUM(CASE WHEN a.date > l.end_date AND a.date <= (l.end_date + INTERVAL '3 years') AND d.quartile=2 THEN 1 ELSE 0 END) > 0 AND
        SUM(CASE WHEN a.date > l.end_date AND a.date <= (l.end_date + INTERVAL '3 years') AND d.quartile=3 THEN 1 ELSE 0 END) > 0 AND
        SUM(CASE WHEN a.date > l.end_date AND a.date <= (l.end_date + INTERVAL '3 years') AND d.quartile=4 THEN 1 ELSE 0 END) > 0
    )
    SELECT
      a.usgs_site,
      l.year,
      l.start_date,
      l.end_date,
      CASE WHEN a.date < l.start_date THEN 'before' ELSE 'after' END AS segment,
      a.date,
      a.value_std,
      d."Flow" AS flow,
      d.quartile,
      c.before_count,
      c.after_count
    FROM firearea.%I a
    JOIN firearea.discharge d ON (a.usgs_site=d.usgs_site AND a.date=d."Date")
    JOIN firearea.%I l ON (a.usgs_site=l.usgs_site)
    JOIN counts c ON (a.usgs_site=c.usgs_site AND l.year=c.year AND l.start_date=c.start_date AND l.end_date=c.end_date)
    WHERE a.value_std IS NOT NULL AND d."Flow" IS NOT NULL AND d.quartile IS NOT NULL AND
      ((a.date >= (l.start_date - INTERVAL '3 years') AND a.date < l.start_date) OR (a.date > l.end_date AND a.date <= (l.end_date + INTERVAL '3 years')))
    ORDER BY a.usgs_site,l.year,segment DESC,a.date;
  -- FORMAT placeholders order: 1 drop MV name, 2 create MV name, 3 analyte view (counts), 4 largest fire MV (counts),
  -- 5 analyte view (select), 6 largest fire MV (select)
  $f$, mv_name, mv_name, analyte_view, largest_fire_mv, analyte_view, largest_fire_mv);

    EXECUTE sql;
    -- Create indexes separately for robustness
    EXECUTE FORMAT('CREATE INDEX IF NOT EXISTS %I ON firearea.%I(usgs_site);', idx_site, mv_name);
    EXECUTE FORMAT('CREATE INDEX IF NOT EXISTS %I ON firearea.%I(start_date);', idx_start, mv_name);
    RETURN FORMAT('SUCCESS: Created materialized view firearea.%s', mv_name);
EXCEPTION WHEN OTHERS THEN
    RETURN FORMAT('ERROR: Failed to create MV for %s: %s', analyte_name, SQLERRM);
END;
$$;

-- SELECT firearea.create_analyte_q_pre_post_quartiles_largest_fire_mv('nitrate');
-- SELECT firearea.create_analyte_q_pre_post_quartiles_largest_fire_mv('ammonium');
-- SELECT firearea.create_analyte_q_pre_post_quartiles_largest_fire_mv('orthop');
-- SELECT firearea.create_analyte_q_pre_post_quartiles_largest_fire_mv('spcond');

-- chunk_14
CREATE OR REPLACE FUNCTION firearea.create_analyte_q_pre_quartiles_largest_fire_mv(analyte_name TEXT)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    mv_name TEXT;
    analyte_view TEXT;
    idx_site TEXT;
    idx_start TEXT;
    sql TEXT;
    ok_analyte BOOLEAN;
BEGIN
    IF analyte_name !~ '^[a-zA-Z_][a-zA-Z0-9_]*$' THEN
        RETURN FORMAT('ERROR: Invalid analyte name: %s', analyte_name);
    END IF;

    analyte_view := analyte_name;                -- firearea.<analyte> view
    mv_name      := FORMAT('%s_q_pre_quartiles_largest_fire', analyte_name);
    idx_site     := FORMAT('idx_%s_qpre_lf_usgs_site', analyte_name);
    idx_start    := FORMAT('idx_%s_qpre_lf_start_date', analyte_name);

    -- Ensure analyte view exists
    SELECT EXISTS(
        SELECT 1 FROM information_schema.tables
        WHERE table_schema='firearea' AND table_name=analyte_view
    ) INTO ok_analyte;
    IF NOT ok_analyte THEN
        RETURN FORMAT('ERROR: Missing analyte view firearea.%s', analyte_view);
    END IF;

  -- Build MV: choose largest fire per site satisfying pre quartile presence (2–4), then emit both windows
  -- Note: create the MV and the indexes in separate EXECUTEs to avoid any parsing ambiguity.
  sql := FORMAT($f$
    DROP MATERIALIZED VIEW IF EXISTS firearea.%I CASCADE;
    CREATE MATERIALIZED VIEW firearea.%I AS
    WITH fire_candidates AS (
      SELECT
        r.usgs_site,
        r.year,
        r.start_date,
        r.end_date,
        r.cum_fire_area
      FROM firearea.ranges_agg r
      -- We join analyte + discharge only to test pre-window quartile presence
      JOIN firearea.%I a
        ON a.usgs_site = r.usgs_site
      JOIN firearea.discharge d
        ON a.usgs_site = d.usgs_site AND a.date = d."Date"
      WHERE a.value_std IS NOT NULL
        AND d."Flow" IS NOT NULL
        AND d.quartile IS NOT NULL
        AND a.date >= (r.start_date - INTERVAL '3 years')
        AND a.date <  r.start_date
      GROUP BY r.usgs_site, r.year, r.start_date, r.end_date, r.cum_fire_area
      HAVING
        SUM(CASE WHEN a.date >= (r.start_date - INTERVAL '3 years')
                  AND a.date < r.start_date AND d.quartile = 2 THEN 1 ELSE 0 END) > 0
        AND SUM(CASE WHEN a.date >= (r.start_date - INTERVAL '3 years')
                  AND a.date < r.start_date AND d.quartile = 3 THEN 1 ELSE 0 END) > 0
        AND SUM(CASE WHEN a.date >= (r.start_date - INTERVAL '3 years')
                  AND a.date < r.start_date AND d.quartile = 4 THEN 1 ELSE 0 END) > 0
    ),
    largest_fire AS (
      SELECT DISTINCT ON (usgs_site)
        usgs_site, year, start_date, end_date, cum_fire_area
      FROM fire_candidates
      ORDER BY usgs_site, cum_fire_area DESC
    ),
    counts AS (
      SELECT
        a.usgs_site,
        l.year,
        l.start_date,
        l.end_date,
        COUNT(CASE WHEN a.date >= (l.start_date - INTERVAL '3 years')
                   AND a.date <  l.start_date THEN 1 END) AS before_count,
        COUNT(CASE WHEN a.date > l.end_date
                   AND a.date <= (l.end_date + INTERVAL '3 years') THEN 1 END) AS after_count
      FROM firearea.%I a
      JOIN firearea.discharge d
        ON a.usgs_site = d.usgs_site AND a.date = d."Date"
      JOIN largest_fire l
        ON a.usgs_site = l.usgs_site
      WHERE a.value_std IS NOT NULL
        AND d."Flow" IS NOT NULL
        AND d.quartile IS NOT NULL
      GROUP BY a.usgs_site, l.year, l.start_date, l.end_date
    )
    SELECT
      a.usgs_site,
      l.year,
      l.start_date,
      l.end_date,
      CASE
        WHEN a.date < l.start_date THEN 'before'
        WHEN a.date > l.end_date  THEN 'after'
      END AS segment,
      a.date,
      a.value_std,
      d."Flow"      AS flow,
      d.quartile,
      c.before_count,
      c.after_count
    FROM firearea.%I a
    JOIN firearea.discharge d
      ON a.usgs_site = d.usgs_site AND a.date = d."Date"
    JOIN largest_fire l
      ON a.usgs_site = l.usgs_site
    JOIN counts c
      ON a.usgs_site = c.usgs_site AND l.year = c.year
       AND l.start_date = c.start_date AND l.end_date = c.end_date
    WHERE a.value_std IS NOT NULL
      AND d."Flow" IS NOT NULL
      AND d.quartile IS NOT NULL
      AND (
        (a.date >= (l.start_date - INTERVAL '3 years') AND a.date < l.start_date) OR
        (a.date > l.end_date AND a.date <= (l.end_date + INTERVAL '3 years'))
      )
    ORDER BY a.usgs_site, l.year, segment DESC, a.date;
    $f$, mv_name, mv_name, analyte_view, analyte_view, analyte_view, analyte_view);

    EXECUTE sql;
    -- Create indexes separately
    EXECUTE FORMAT('CREATE INDEX IF NOT EXISTS %I ON firearea.%I(usgs_site);', idx_site, mv_name);
    EXECUTE FORMAT('CREATE INDEX IF NOT EXISTS %I ON firearea.%I(start_date);', idx_start, mv_name);
    RETURN FORMAT('SUCCESS: Created materialized view firearea.%s', mv_name);

EXCEPTION
    WHEN OTHERS THEN
        RETURN FORMAT('ERROR: Failed to create MV for %s: %s', analyte_name, SQLERRM);
END;
$$;

-- SELECT firearea.create_analyte_q_pre_quartiles_largest_fire_mv('nitrate');


COMMIT;

-- If any errors occurred, the transaction will be rolled back automatically
