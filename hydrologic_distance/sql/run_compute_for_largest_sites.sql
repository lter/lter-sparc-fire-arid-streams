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

  -- Run and execute the compute for all union sites
     SELECT firearea.compute_hydro_fire_distances_for_largest_sites();

  -- Dry-run: only populate _largest_sites, skip compute loop (preserve line spacing)
     BEGIN;

     SELECT firearea.compute_hydro_fire_distances_for_largest_sites(p_execute => false);

     copy (SELECT * FROM _largest_sites) to '/tmp/largest_sites.csv' with csv header;

     ROLLBACK;


DROP FUNCTION IF EXISTS firearea.compute_hydro_fire_distances_for_largest_sites(text, integer, boolean);
DROP FUNCTION IF EXISTS firearea.compute_hydro_fire_distances_for_largest_sites(boolean);
DROP FUNCTION IF EXISTS firearea.compute_hydro_fire_distances_for_largest_sites(boolean, boolean);

-- Persistent log for diagnostics (visible even if NOTICEs are suppressed by client)
CREATE TABLE IF NOT EXISTS firearea.hydro_fire_compute_log (
  run_ts       timestamptz NOT NULL,
  site         text,
  message      text NOT NULL,
  views        text[],
  site_ct      integer,
  boundary_ct  integer,
  pour_ct      integer,
  upserts      integer,
  table_delta  integer,
  p_execute    boolean,
  p_verbose    boolean
);

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
  -- diagnostics
  before_total_ct integer;
  after_total_ct  integer;
  before_site_ct  integer;
  after_site_ct   integer;
  upserted_total  integer := 0;
  boundary_ct     integer;
  pour_ct         integer;
  run_ts          timestamptz;
BEGIN
  -- Ensure NOTICE messages are not suppressed by client settings when verbose
  IF p_verbose THEN
    PERFORM set_config('client_min_messages', 'notice', true);
  END IF;
  run_ts := clock_timestamp();
  -- Discover materialized views
  SELECT array_agg(format('%I.%I', schemaname, matviewname) ORDER BY matviewname)
  INTO views
  FROM pg_matviews
  WHERE schemaname = 'firearea'
    AND matviewname LIKE 'largest\_%\_valid\_fire\_per\_site';

  n := COALESCE(array_length(views, 1), 0);
  IF p_verbose THEN RAISE NOTICE 'Found % views: %', n, views; END IF;
  INSERT INTO firearea.hydro_fire_compute_log(run_ts, site, message, views, p_execute, p_verbose)
  VALUES (run_ts, NULL, format('Found %s views', n), views, p_execute, p_verbose);

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
  INSERT INTO firearea.hydro_fire_compute_log(run_ts, site, message, site_ct, p_execute, p_verbose)
  VALUES (run_ts, NULL, 'Sites to process', site_ct, p_execute, p_verbose);

  -- Prerequisite checks and early diagnostics
  IF to_regclass('firearea.fire_boundary_points') IS NULL THEN
    RAISE EXCEPTION 'Missing table firearea.fire_boundary_points. Run hydrologic_distance/sql/pg_hydro_fire_distance.sql first.';
  END IF;
  IF to_regclass('firearea.pour_point_nodes') IS NULL THEN
    RAISE EXCEPTION 'Missing table firearea.pour_point_nodes. Run hydrologic_distance/sql/pg_hydro_fire_distance.sql first.';
  END IF;

  SELECT COUNT(*) INTO boundary_ct
  FROM firearea.fire_boundary_points fbp
  WHERE fbp.usgs_site IN (SELECT usgs_site FROM _largest_sites);
  SELECT COUNT(*) INTO pour_ct
  FROM firearea.pour_point_nodes pp
  WHERE pp.usgs_site IN (SELECT usgs_site FROM _largest_sites);
  IF p_verbose THEN RAISE NOTICE 'Prereqs: boundary points=% , pour-point nodes=%', boundary_ct, pour_ct; END IF;
  INSERT INTO firearea.hydro_fire_compute_log(run_ts, site, message, boundary_ct, pour_ct, p_execute, p_verbose)
  VALUES (run_ts, NULL, 'Prereq counts', boundary_ct, pour_ct, p_execute, p_verbose);

  SELECT COUNT(*) INTO before_total_ct
  FROM firearea.hydro_fire_min_distance h
  WHERE h.usgs_site IN (SELECT usgs_site FROM _largest_sites);

  IF p_execute THEN
    FOR r IN SELECT usgs_site FROM _largest_sites LOOP
      SELECT COUNT(*) INTO before_site_ct FROM firearea.hydro_fire_min_distance WHERE usgs_site = r.usgs_site;
      PERFORM firearea.compute_hydro_fire_distances(r.usgs_site);
      SELECT COUNT(*) INTO after_site_ct FROM firearea.hydro_fire_min_distance WHERE usgs_site = r.usgs_site;
      upserted_total := upserted_total + GREATEST(0, after_site_ct - before_site_ct);
      IF p_verbose THEN RAISE NOTICE 'Site %: upserted % rows', r.usgs_site, (after_site_ct - before_site_ct); END IF;
      INSERT INTO firearea.hydro_fire_compute_log(run_ts, site, message, upserts, p_execute, p_verbose)
      VALUES (run_ts, r.usgs_site, 'Site upserts', (after_site_ct - before_site_ct), p_execute, p_verbose);
    END LOOP;
    SELECT COUNT(*) INTO after_total_ct
    FROM firearea.hydro_fire_min_distance h
    WHERE h.usgs_site IN (SELECT usgs_site FROM _largest_sites);
    IF p_verbose THEN RAISE NOTICE 'Completed compute for % site(s). Total upserts=% (table delta=%).', site_ct, upserted_total, (after_total_ct - before_total_ct); END IF;
    INSERT INTO firearea.hydro_fire_compute_log(run_ts, site, message, upserts, table_delta, site_ct, p_execute, p_verbose)
    VALUES (run_ts, NULL, 'Run summary', upserted_total, (after_total_ct - before_total_ct), site_ct, p_execute, p_verbose);
  ELSE
    IF p_verbose THEN RAISE NOTICE 'Dry-run: compute loop skipped.'; END IF;
    INSERT INTO firearea.hydro_fire_compute_log(run_ts, site, message, site_ct, p_execute, p_verbose)
    VALUES (run_ts, NULL, 'Dry-run: loop skipped', site_ct, p_execute, p_verbose);
  END IF;
  RETURN site_ct;
END;
$$;
