-- Compute hydro-fire distances for sites present in ANY largest_* materialized view (union)
-- Database: wildfire (schema firearea)
-- Function: firearea.compute_hydro_fire_distances_for_largest_sites
-- Purpose (minimal):
--   - Discover firearea.largest_*_valid_fire_per_site materialized views
--   - Build a site list as the UNION of unique usgs_site across all views
--   - Materialize that list in a temp table for inspection
--   - Optionally call firearea.compute_hydro_fire_distances(site) for each site (controlled by p_execute)
--   - Return the number of sites processed
-- Usage examples:
--   -- Run and execute the compute for all union sites
--   SELECT firearea.compute_hydro_fire_distances_for_largest_sites();
--   -- Dry-run: only populate _largest_sites, skip compute loop (preserve line spacing)
--   BEGIN;

--   SELECT firearea.compute_hydro_fire_distances_for_largest_sites(p_execute => false);

--   copy (SELECT * FROM _largest_sites) to '/tmp/largest_sites.csv' with csv header;

--   ROLLBACK;


DROP FUNCTION IF EXISTS firearea.compute_hydro_fire_distances_for_largest_sites(text, integer, boolean);
DROP FUNCTION IF EXISTS firearea.compute_hydro_fire_distances_for_largest_sites(boolean);
DROP FUNCTION IF EXISTS firearea.compute_hydro_fire_distances_for_largest_sites(boolean, boolean);
CREATE OR REPLACE FUNCTION firearea.compute_hydro_fire_distances_for_largest_sites(
  p_verbose boolean DEFAULT true,
  p_execute boolean DEFAULT true
) RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  views   text[];
  n       integer;       -- number of discovered views
  q       text;          -- dynamic SQL to get the site set
  site_ct integer;       -- number of sites to process
  r       record;
BEGIN
  -- Discover materialized views
  SELECT array_agg(format('%I.%I', schemaname, matviewname) ORDER BY matviewname)
  INTO views
  FROM pg_matviews
  WHERE schemaname = 'firearea'
    AND matviewname LIKE 'largest\_%\_valid\_fire\_per\_site';

  n := COALESCE(array_length(views, 1), 0);
  IF p_verbose THEN RAISE NOTICE 'Found % views: %', n, views; END IF;

  -- Guard: ensure per-site compute function exists
  IF to_regprocedure('firearea.compute_hydro_fire_distances(text)') IS NULL THEN
    RAISE EXCEPTION 'Missing function firearea.compute_hydro_fire_distances(text). Run hydrologic_distance/sql/pg_hydro_fire_distance.sql to create it.';
  END IF;
  -- Build UNION of usgs_site across all views (unique sites across any view)
  q := NULL;
  IF n > 0 THEN
    q := '';
    FOR i IN 1..n LOOP
      IF i > 1 THEN q := q || ' UNION '; END IF;
      q := q || format('SELECT usgs_site FROM %s', views[i]);
    END LOOP;
  END IF;

  -- Materialize site list and run compute
  EXECUTE 'CREATE TEMP TABLE IF NOT EXISTS _largest_sites (usgs_site text PRIMARY KEY) ON COMMIT DROP';
  EXECUTE 'TRUNCATE _largest_sites';
  IF q IS NOT NULL AND length(q) > 0 THEN
    EXECUTE 'INSERT INTO _largest_sites ' || q;
    GET DIAGNOSTICS site_ct = ROW_COUNT;
  ELSE
    site_ct := 0;
  END IF;
  IF p_verbose THEN RAISE NOTICE 'Sites to process: % (execute=%)', site_ct, p_execute; END IF;

  IF p_execute THEN
    FOR r IN SELECT usgs_site FROM _largest_sites LOOP
      PERFORM firearea.compute_hydro_fire_distances(r.usgs_site);
    END LOOP;
    IF p_verbose THEN RAISE NOTICE 'Completed compute_hydro_fire_distances() for % site(s).', site_ct; END IF;
  ELSE
    IF p_verbose THEN RAISE NOTICE 'Dry-run: compute loop skipped.'; END IF;
  END IF;
  RETURN site_ct;
END;
$$;