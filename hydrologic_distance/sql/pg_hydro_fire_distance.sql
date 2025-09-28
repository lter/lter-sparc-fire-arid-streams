-- Hydrologic shortest distance from catchment pour point to fire boundary intersections
-- Database: wildfire (schema firearea)
-- Requires: postgis, pgrouting

-- (0) Extensions
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pgrouting;

-- (1) Unified catchments and pour points (USGS + non-USGS)
DROP VIEW IF EXISTS firearea.catchments_all CASCADE;
CREATE VIEW firearea.catchments_all AS
SELECT usgs_site, geometry FROM firearea.catchments
UNION ALL
SELECT usgs_site, geometry FROM firearea.non_usgs_catchments;

DROP VIEW IF EXISTS firearea.pour_points_all CASCADE;
CREATE VIEW firearea.pour_points_all AS
SELECT usgs_site, geometry FROM firearea.pour_points
UNION ALL
SELECT usgs_site, geometry FROM firearea.non_usgs_pour_points;


-- (2) Build routing edges from flowlines (one edge per flowline)
DROP TABLE IF EXISTS firearea.flow_edges_base CASCADE;
CREATE TABLE firearea.flow_edges_base (
  id bigserial PRIMARY KEY,
  usgs_site text NOT NULL,
  geom geometry(LineString,4326) NOT NULL,
  length_m double precision NOT NULL,
  source bigint,
  target bigint,
  cost double precision,
  reverse_cost double precision
);

INSERT INTO firearea.flow_edges_base (usgs_site, geom, length_m, cost, reverse_cost)
SELECT fl.usgs_site,
       fl.geometry::geometry(LineString,4326) AS geom,
       ST_Length(fl.geometry::geography) AS length_m,
       ST_Length(fl.geometry::geography) AS cost,
       ST_Length(fl.geometry::geography) AS reverse_cost
FROM firearea.flowlines fl;

CREATE INDEX IF NOT EXISTS flow_edges_base_geom_idx ON firearea.flow_edges_base USING gist (geom);
CREATE INDEX IF NOT EXISTS flow_edges_base_site_idx ON firearea.flow_edges_base (usgs_site);

-- (3) Create topology (assign source/target nodes)
-- Tolerance ~ 1 meter in degrees
SELECT pgr_createTopology(
  'firearea.flow_edges_base',
  1e-5,
  'geom',
  'id',
  'source',
  'target',
  rows_where:='true'
);

-- (4) Pour-point to nearest vertex per site
DROP TABLE IF EXISTS firearea.pour_point_nodes CASCADE;
CREATE TABLE firearea.pour_point_nodes AS
SELECT p.usgs_site,
       n.id AS node_id,
       p.geometry AS geom
FROM firearea.pour_points_all p
JOIN LATERAL (
  SELECT nn.id, nn.the_geom
  FROM firearea.flow_edges_base_vertices_pgr nn
  JOIN firearea.flow_edges_base e ON nn.id IN (e.source, e.target) AND e.usgs_site = p.usgs_site
  ORDER BY nn.the_geom <-> p.geometry
  LIMIT 1
) n ON TRUE;

CREATE INDEX IF NOT EXISTS pour_point_nodes_site_idx ON firearea.pour_point_nodes (usgs_site);
CREATE INDEX IF NOT EXISTS pour_point_nodes_geom_idx ON firearea.pour_point_nodes USING gist (geom);

-- (5) Subgraph edges limited to catchment (contributing network proxy)
DROP VIEW IF EXISTS firearea.flow_edges_in_catchment CASCADE;
CREATE VIEW firearea.flow_edges_in_catchment AS
SELECT e.*
FROM firearea.flow_edges_base e
JOIN firearea.catchments_all c USING (usgs_site)
WHERE ST_Intersects(e.geom, c.geometry);

-- (6) (Refactored) Fire boundary intersections (on-demand)
-- Previous version materialized all (site,event,edge) intersection points in a global table
--   firearea.fire_boundary_points, which could become extremely large.
-- Strategy B: We now compute event distances per site on-demand inside the
-- per-site function without persisting individual intersection points.
-- This section is retained only as documentation; no table is created here.

-- (7) Results table
DROP TABLE IF EXISTS firearea.hydro_fire_min_distance CASCADE;
CREATE TABLE firearea.hydro_fire_min_distance (
  usgs_site text NOT NULL,
  event_id text NOT NULL,
  min_distance_m double precision NOT NULL,
  PRIMARY KEY (usgs_site, event_id)
);

-- Optional detailed record of which edge / orientation / fraction produced the minimum
DROP TABLE IF EXISTS firearea.hydro_fire_min_distance_detail CASCADE;
CREATE TABLE firearea.hydro_fire_min_distance_detail (
  usgs_site text NOT NULL,
  event_id text NOT NULL,
  edge_id bigint NOT NULL,
  orientation text NOT NULL,
  fraction double precision NOT NULL,
  boundary_point geometry(Point,4326) NOT NULL,
  from_cost_m double precision NOT NULL,
  partial_edge_m double precision NOT NULL,
  min_distance_m double precision NOT NULL,
  zero_reason text,                        -- NULL unless zero-distance classification applied
  pp_edge_length_m double precision,       -- length of nearest pour point edge (for zero-distance provenance)
  PRIMARY KEY (usgs_site, event_id)
);

-- (8) Compute function (per-site)
DROP FUNCTION IF EXISTS firearea.compute_hydro_fire_distances(site text);
CREATE OR REPLACE FUNCTION firearea.compute_hydro_fire_distances(site text)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  src_node bigint;
  edges_sql text;
  near_meters constant double precision := 20.0; -- tolerance for near-boundary (meters)
  pp_geom geometry(Point,4326);
  total_events integer;
  zero_events integer;
BEGIN
  SELECT node_id INTO src_node FROM firearea.pour_point_nodes WHERE usgs_site = site;
  IF src_node IS NULL THEN
    RAISE NOTICE 'No source node for %', site; RETURN;
  END IF;

  -- Pour point geometry
  SELECT geometry INTO pp_geom FROM firearea.pour_points_all WHERE usgs_site = site LIMIT 1;
  
  -- Prepare edge SQL and pre-compute directed vertex costs (for upstream determination and later routing)
  edges_sql := format('SELECT id, source, target, cost, reverse_cost FROM firearea.flow_edges_in_catchment WHERE usgs_site = %L', site);

  DROP TABLE IF EXISTS _tc;  -- vertex total costs from pour point (directed)
  CREATE TEMP TABLE _tc AS
  SELECT end_vid, agg_cost AS total_cost
  FROM pgr_dijkstraCost(
    edges_sql,
    src_node,
    ARRAY(SELECT DISTINCT v.id
          FROM firearea.flow_edges_base_vertices_pgr v
          JOIN firearea.flow_edges_in_catchment e ON v.id IN (e.source, e.target)
          WHERE e.usgs_site = site),
    directed := true);
  CREATE INDEX ON _tc(end_vid);

  -- Safeguard: any non-source vertex with distance 0 indicates a logic or data issue
  PERFORM 1 FROM _tc WHERE end_vid <> src_node AND total_cost = 0;
  IF FOUND THEN
    RAISE EXCEPTION 'Unexpected zero total_cost for non-source vertex(es) at site % (early cost build)', site;
  END IF;

  -- Identify fire events with zero hydrologic distance.
  -- Expanded Criteria (any satisfied):
  --   (a) Fire polygon covers the original pour point geometry (zero_reason='pour_point')
  --   (b) Fire polygon covers ANY upstream vertex among all edges in the catchment (zero_reason='upstream_vertex').
  --       Upstream vertex per edge is the endpoint with the higher network cost from pour point (directed),
  --       falling back to farther Euclidean endpoint only if one/both costs are NULL or equal.
  -- Pour point classification has priority; duplicates are collapsed by DISTINCT.
  DROP TABLE IF EXISTS _inside_zero;
  CREATE TEMP TABLE _inside_zero AS
  WITH fire_events AS (
    SELECT event_id, ST_MakeValid(geometry) AS geom
    FROM firearea.fires_catchments
    WHERE usgs_site = site
  ), nearest_edge_len AS (
    SELECT e.cost AS pp_edge_length_m
    FROM firearea.flow_edges_in_catchment e
    WHERE e.usgs_site = site
    ORDER BY e.geom <-> pp_geom
    LIMIT 1
  ), edge_endpoints AS (
    SELECT e.id AS edge_id,
           e.source,
           e.target,
           vs.the_geom AS source_geom,
           vt.the_geom AS target_geom,
           tcs.total_cost AS cost_source,
           tct.total_cost AS cost_target
    FROM firearea.flow_edges_in_catchment e
    JOIN firearea.flow_edges_base_vertices_pgr vs ON vs.id = e.source
    JOIN firearea.flow_edges_base_vertices_pgr vt ON vt.id = e.target
    LEFT JOIN _tc tcs ON tcs.end_vid = e.source
    LEFT JOIN _tc tct ON tct.end_vid = e.target
    WHERE e.usgs_site = site
  ), per_edge_upstream AS (
    SELECT edge_id,
           CASE WHEN cost_source IS NOT NULL AND cost_target IS NOT NULL AND cost_source < cost_target THEN target_geom
                WHEN cost_source IS NOT NULL AND cost_target IS NOT NULL AND cost_source > cost_target THEN source_geom
                ELSE CASE WHEN ST_Distance(source_geom::geography, pp_geom::geography) <= ST_Distance(target_geom::geography, pp_geom::geography)
                          THEN target_geom ELSE source_geom END
           END AS upstream_geom
    FROM edge_endpoints
  ), pour_point_zero AS (
    SELECT fe.event_id,
           'pour_point'::text AS zero_reason,
           (SELECT pp_edge_length_m FROM nearest_edge_len) AS pp_edge_length_m
    FROM fire_events fe
    WHERE ST_Covers(fe.geom, pp_geom)
  ), upstream_vertex_zero AS (
    SELECT DISTINCT fe.event_id,
           'upstream_vertex'::text AS zero_reason,
           (SELECT pp_edge_length_m FROM nearest_edge_len) AS pp_edge_length_m
    FROM fire_events fe
    JOIN per_edge_upstream pu ON pu.upstream_geom IS NOT NULL AND ST_Covers(fe.geom, pu.upstream_geom)
    WHERE NOT ST_Covers(fe.geom, pp_geom)  -- exclude those already covered by pour point classification
  )
  SELECT * FROM (
    SELECT * FROM pour_point_zero
    UNION ALL
    SELECT * FROM upstream_vertex_zero
  ) z;

  SELECT COUNT(*) INTO total_events FROM firearea.fires_catchments WHERE usgs_site = site;
  SELECT COUNT(*) INTO zero_events FROM _inside_zero;

  -- Upsert zero-distance events (summary)
  INSERT INTO firearea.hydro_fire_min_distance (usgs_site, event_id, min_distance_m)
  SELECT site, event_id, 0.0 FROM _inside_zero
  ON CONFLICT (usgs_site, event_id) DO UPDATE SET min_distance_m = 0.0;

  -- Upsert zero-distance events (detail)
  INSERT INTO firearea.hydro_fire_min_distance_detail (usgs_site, event_id, edge_id, orientation, fraction, boundary_point, from_cost_m, partial_edge_m, min_distance_m, zero_reason, pp_edge_length_m)
  SELECT site, event_id, 0::bigint, 'inside', 0.0, pp_geom, 0.0, 0.0, 0.0, zero_reason, pp_edge_length_m
  FROM _inside_zero
  ON CONFLICT (usgs_site, event_id) DO UPDATE SET
    edge_id = 0,
    orientation = 'inside',
    fraction = 0.0,
    boundary_point = EXCLUDED.boundary_point,
    from_cost_m = 0.0,
    partial_edge_m = 0.0,
    min_distance_m = 0.0,
    zero_reason = EXCLUDED.zero_reason,
    pp_edge_length_m = EXCLUDED.pp_edge_length_m;

  -- If ALL events are zero-distance, we can return early
  IF total_events = zero_events THEN
    RETURN;
  END IF;

  -- Ensure summary & detail tables exist (in case only function body was loaded without earlier DDL)
  PERFORM 1 FROM information_schema.tables WHERE table_schema='firearea' AND table_name='hydro_fire_min_distance';
  IF NOT FOUND THEN
    EXECUTE 'CREATE TABLE IF NOT EXISTS firearea.hydro_fire_min_distance (
        usgs_site text NOT NULL,
        event_id text NOT NULL,
        min_distance_m double precision NOT NULL,
        PRIMARY KEY (usgs_site, event_id)
      )';
  END IF;

  PERFORM 1 FROM information_schema.tables WHERE table_schema='firearea' AND table_name='hydro_fire_min_distance_detail';
  IF NOT FOUND THEN
    EXECUTE 'CREATE TABLE IF NOT EXISTS firearea.hydro_fire_min_distance_detail (
        usgs_site text NOT NULL,
        event_id text NOT NULL,
        edge_id bigint NOT NULL,
        orientation text NOT NULL,
        fraction double precision NOT NULL,
        boundary_point geometry(Point,4326) NOT NULL,
        from_cost_m double precision NOT NULL,
        partial_edge_m double precision NOT NULL,
        min_distance_m double precision NOT NULL,
        zero_reason text,
        pp_edge_length_m double precision,
        PRIMARY KEY (usgs_site, event_id)
      )';
  END IF;

  -- Migration: add provenance columns if missing
  PERFORM 1 FROM information_schema.columns
    WHERE table_schema='firearea' AND table_name='hydro_fire_min_distance_detail' AND column_name='zero_reason';
  IF NOT FOUND THEN
    EXECUTE 'ALTER TABLE firearea.hydro_fire_min_distance_detail ADD COLUMN zero_reason text';
  END IF;
  PERFORM 1 FROM information_schema.columns
    WHERE table_schema='firearea' AND table_name='hydro_fire_min_distance_detail' AND column_name='pp_edge_length_m';
  IF NOT FOUND THEN
    EXECUTE 'ALTER TABLE firearea.hydro_fire_min_distance_detail ADD COLUMN pp_edge_length_m double precision';
  END IF;

  -- (Vertex costs already computed above in _tc; reuse for downstream logic.)

  -- Costs to both endpoints per edge
  DROP TABLE IF EXISTS _edge_cost;
  CREATE TEMP TABLE _edge_cost AS
  SELECT e.id AS edge_id,
         e.source AS source_node,
         e.target AS target_node,
         e.cost   AS edge_length,
         tc_s.total_cost AS cost_source,
         tc_t.total_cost AS cost_target
  FROM firearea.flow_edges_in_catchment e
  JOIN _tc tc_s ON tc_s.end_vid = e.source
  JOIN _tc tc_t ON tc_t.end_vid = e.target
  WHERE e.usgs_site = site;

  -- Enumerate all intersection / near-boundary candidate points (multi-cross aware)
  DROP TABLE IF EXISTS _event_candidates;
  CREATE TEMP TABLE _event_candidates AS
  WITH site_fires AS (
    SELECT event_id, ST_Boundary(ST_MakeValid(geometry)) AS boundary
    FROM firearea.fires_catchments
    WHERE usgs_site = site
      AND event_id NOT IN (SELECT event_id FROM _inside_zero)
  ), base AS (
    SELECT sf.event_id,
           ec.edge_id,
           ec.cost_source,
           ec.cost_target,
           e.geom,
           sf.boundary
    FROM _edge_cost ec
    JOIN firearea.flow_edges_base e ON e.id = ec.edge_id
    JOIN site_fires sf ON TRUE
    WHERE (
        ST_Intersects(e.geom, sf.boundary)
        OR ST_DWithin(e.geom::geography, sf.boundary::geography, near_meters)
      )
      AND e.usgs_site = site
  ), exploded AS (
    -- Produce one candidate point per actual intersection point or, for overlapping segment(s), endpoints
    SELECT b.event_id,
           b.edge_id,
           b.cost_source,
           b.cost_target,
           b.geom,
           (ST_Dump(
              CASE
                WHEN ST_Intersects(b.geom, b.boundary) THEN
                  CASE
                    WHEN GeometryType(ST_Intersection(b.geom, b.boundary)) LIKE 'LINESTRING%' THEN
                      -- overlapping line(s): take both endpoints of each segment
                      ST_Collect(
                        ST_StartPoint(ST_Intersection(b.geom, b.boundary)),
                        ST_EndPoint(ST_Intersection(b.geom, b.boundary))
                      )
                    ELSE ST_Intersection(b.geom, b.boundary)
                  END
                ELSE ST_ClosestPoint(b.boundary, b.geom)
              END)).geom AS candidate_point
    FROM base b
  ), fracd AS (
    SELECT event_id,
           edge_id,
           cost_source,
           cost_target,
           geom,
           candidate_point,
           LEAST(1.0, GREATEST(0.0, ST_LineLocatePoint(geom, candidate_point))) AS frac
    FROM exploded
  ), metrics AS (
    SELECT event_id,
           edge_id,
           cost_source,
           cost_target,
           candidate_point,
           frac,
           ST_Length(ST_LineSubstring(geom, 0.0, frac)::geography) AS len_from_start,
           ST_Length(ST_LineSubstring(geom, frac, 1.0)::geography) AS len_from_end
    FROM fracd
  )
  SELECT event_id,
         edge_id,
         candidate_point,
         frac,
         cost_source,
         cost_target,
         len_from_start,
         len_from_end,
         cost_source + len_from_start AS total_from_source,
         cost_target + len_from_end   AS total_from_target,
         CASE WHEN (cost_source + len_from_start) <= (cost_target + len_from_end) THEN 'source' ELSE 'target' END AS orientation,
         LEAST(cost_source + len_from_start, cost_target + len_from_end) AS candidate_distance,
         CASE WHEN (cost_source + len_from_start) <= (cost_target + len_from_end) THEN len_from_start ELSE len_from_end END AS partial_edge_m,
         CASE WHEN (cost_source + len_from_start) <= (cost_target + len_from_end) THEN cost_source ELSE cost_target END AS from_cost_m
  FROM metrics;

  -- Minimum per event (store detail)
  DROP TABLE IF EXISTS _event_min;
  CREATE TEMP TABLE _event_min AS
  SELECT DISTINCT ON (event_id) event_id,
         edge_id,
         orientation,
         frac,
         candidate_point,
         from_cost_m,
         partial_edge_m,
         candidate_distance AS min_distance_m
  FROM _event_candidates
  ORDER BY event_id, candidate_distance;

  -- Upsert summary distances
  INSERT INTO firearea.hydro_fire_min_distance (usgs_site, event_id, min_distance_m)
  SELECT site, event_id, min_distance_m FROM _event_min
  ON CONFLICT (usgs_site, event_id) DO UPDATE SET min_distance_m = EXCLUDED.min_distance_m;

  -- Upsert detail
  INSERT INTO firearea.hydro_fire_min_distance_detail (usgs_site, event_id, edge_id, orientation, fraction, boundary_point, from_cost_m, partial_edge_m, min_distance_m)
  SELECT site, event_id, edge_id, orientation, frac, candidate_point, from_cost_m, partial_edge_m, min_distance_m
  FROM _event_min
  ON CONFLICT (usgs_site, event_id) DO UPDATE SET
    edge_id = EXCLUDED.edge_id,
    orientation = EXCLUDED.orientation,
    fraction = EXCLUDED.fraction,
    boundary_point = EXCLUDED.boundary_point,
    from_cost_m = EXCLUDED.from_cost_m,
    partial_edge_m = EXCLUDED.partial_edge_m,
    min_distance_m = EXCLUDED.min_distance_m;

  DROP TABLE IF EXISTS _tc; DROP TABLE IF EXISTS _edge_cost; DROP TABLE IF EXISTS _event_candidates; DROP TABLE IF EXISTS _event_min;
END;
$$;


-- Modernized multi-intersection + orientation-aware rows function (production parity)
-- This supersedes debug_hydro_fire_distance_rows (legacy single closest-point logic)
-- Differences vs legacy:
--   * Uses pgr_dijkstraCost instead of pgr_dijkstra + MIN aggregation
--   * Enumerates all intersection points AND overlapping segment endpoints per edge
--   * Chooses minimal orientation-aware hydrologic distance identical to compute_hydro_fire_distances
--   * Reconstructs path to the chosen endpoint (source/target) then appends only the used partial terminal segment
-- Usage (example ogr2ogr):
--   ogr2ogr debug2.geojson "PG:host=... dbname=wildfire user=..." \\
--     -sql "SELECT role, geom, distance_m, from_cost_m, partial_edge_m, fraction, edge_id, orientation
--            FROM firearea.debug_hydro_fire_distance_rows2('USGS-09404900','UT3743511264420120702')"
DROP FUNCTION IF EXISTS firearea.debug_hydro_fire_distance_rows2(p_site text, p_event_id text);
CREATE OR REPLACE FUNCTION firearea.debug_hydro_fire_distance_rows2(p_site text, p_event_id text)
RETURNS TABLE(
  role text,
  geom geometry,
  site text,
  event_id text,
  distance_m double precision,
  from_cost_m double precision,
  partial_edge_m double precision,
  fraction double precision,
  edge_id bigint,
  orientation text
) LANGUAGE plpgsql AS $$
DECLARE
  src_node bigint;
  edges_sql text;
  near_meters constant double precision := 20.0;
  v_event_id text := trim(p_event_id);
  -- Chosen candidate properties
  chosen_edge_id bigint;
  chosen_source_node bigint;
  chosen_target_node bigint;
  chosen_orientation text;
  chosen_frac double precision;
  chosen_boundary_point geometry(Point,4326);
  chosen_cost_source double precision;
  chosen_cost_target double precision;
  chosen_len_from_start double precision;
  chosen_len_from_end double precision;
  chosen_total_from_source double precision;
  chosen_total_from_target double precision;
  chosen_total double precision;
  chosen_from_cost_m double precision;
  chosen_partial_edge_m double precision;
  chosen_node bigint; -- endpoint node we route to (depends on orientation)
  term_edge_geom geometry(LineString,4326);
  -- Partial terminal segment: ensure LineString type even if degenerate
  term_edge_partial_geom geometry(LineString,4326);
  tmp_partial geometry; -- may be a POINT from ST_LineSubstring
  base_path_geo geometry; -- path to chosen_node
  full_path_geo geometry; -- base path + partial segment
  fire_poly geometry;
  fire_boundary geometry;
  catch_geom geometry;
BEGIN
  -- Validate event existence
  PERFORM 1 FROM firearea.fires_catchments f WHERE f.usgs_site = p_site AND f.event_id = v_event_id LIMIT 1;
  IF NOT FOUND THEN
    RAISE NOTICE 'No fire event % for site %', v_event_id, p_site; RETURN; END IF;

  SELECT node_id INTO src_node FROM firearea.pour_point_nodes WHERE usgs_site = p_site;
  IF src_node IS NULL THEN RAISE NOTICE 'No pour point node for site %', p_site; RETURN; END IF;

  edges_sql := format('SELECT id, source, target, cost, reverse_cost FROM firearea.flow_edges_in_catchment WHERE usgs_site = %L', p_site);
  -- Directional routing (see note in compute_hydro_fire_distances). Currently costs are
  -- symmetric; changing reverse_cost to NULL/penalty will constrain traversal.

  -- Vertex distances via pgr_dijkstraCost
  DROP TABLE IF EXISTS _mr_tc;
  CREATE TEMP TABLE _mr_tc AS
  SELECT end_vid, agg_cost AS total_cost
  FROM pgr_dijkstraCost(
    edges_sql,
    src_node,
    ARRAY(SELECT DISTINCT v.id
          FROM firearea.flow_edges_base_vertices_pgr v
          JOIN firearea.flow_edges_in_catchment e ON v.id IN (e.source, e.target)
          WHERE e.usgs_site = p_site),
    directed := true);
  CREATE INDEX ON _mr_tc(end_vid);

  -- Safeguard
  PERFORM 1 FROM _mr_tc WHERE end_vid <> src_node AND total_cost = 0;
  IF FOUND THEN
    RAISE EXCEPTION 'Unexpected zero total_cost for non-source vertex(es) at site % event %', p_site, v_event_id;
  END IF;

  -- Edge endpoint costs
  DROP TABLE IF EXISTS _mr_edge_cost;
  CREATE TEMP TABLE _mr_edge_cost AS
  SELECT e.id AS edge_id,
         e.source AS source_node,
         e.target AS target_node,
         tc_s.total_cost AS cost_source,
         tc_t.total_cost AS cost_target,
         e.geom
  FROM firearea.flow_edges_in_catchment e
  JOIN _mr_tc tc_s ON tc_s.end_vid = e.source
  JOIN _mr_tc tc_t ON tc_t.end_vid = e.target
  WHERE e.usgs_site = p_site;

  -- Event boundary
  SELECT ST_MakeValid(f.geometry) AS poly, ST_Boundary(ST_MakeValid(f.geometry)) AS boundary
  INTO fire_poly, fire_boundary
  FROM firearea.fires_catchments f
  WHERE f.usgs_site = p_site AND f.event_id = v_event_id;

  -- Enumerate candidates (multi-intersection + overlapping endpoints)
  DROP TABLE IF EXISTS _mr_candidates;
  CREATE TEMP TABLE _mr_candidates AS
  WITH base AS (
    SELECT ec.*, fire_boundary AS boundary
    FROM _mr_edge_cost ec
    WHERE ST_Intersects(ec.geom, fire_boundary)
       OR ST_DWithin(ec.geom::geography, fire_boundary::geography, near_meters)
  ), exploded AS (
    SELECT b.edge_id AS cand_edge_id,
           b.source_node,
           b.target_node,
           b.cost_source,
           b.cost_target,
           b.geom,
           (ST_Dump(
             CASE
               WHEN ST_Intersects(b.geom, b.boundary) THEN
                 CASE
                   WHEN GeometryType(ST_Intersection(b.geom, b.boundary)) LIKE 'LINESTRING%' THEN
                     ST_Collect(
                       ST_StartPoint(ST_Intersection(b.geom, b.boundary)),
                       ST_EndPoint(ST_Intersection(b.geom, b.boundary))
                     )
                   ELSE ST_Intersection(b.geom, b.boundary)
                 END
               ELSE ST_ClosestPoint(b.boundary, b.geom)
             END)).geom AS candidate_point
    FROM base b
  ), fracd AS (
    SELECT e.cand_edge_id,
           e.source_node,
           e.target_node,
           e.cost_source,
           e.cost_target,
           e.geom,
           e.candidate_point,
           LEAST(1.0, GREATEST(0.0, ST_LineLocatePoint(e.geom, e.candidate_point))) AS frac
    FROM exploded e
  ), metrics AS (
    SELECT f.cand_edge_id,
           f.source_node,
           f.target_node,
           f.cost_source,
           f.cost_target,
           f.geom,
           f.candidate_point AS boundary_point,
           f.frac,
           ST_Length(ST_LineSubstring(f.geom, 0.0, f.frac)::geography) AS len_from_start,
           ST_Length(ST_LineSubstring(f.geom, f.frac, 1.0)::geography) AS len_from_end
    FROM fracd f
  )
  SELECT m.cand_edge_id AS cand_edge_id,
         m.source_node,
         m.target_node,
         m.frac,
         m.cost_source,
         m.cost_target,
         m.len_from_start,
         m.len_from_end,
         m.cost_source + m.len_from_start AS total_from_source,
         m.cost_target + m.len_from_end   AS total_from_target,
         CASE WHEN (m.cost_source + m.len_from_start) <= (m.cost_target + m.len_from_end) THEN 'source' ELSE 'target' END AS chosen_orientation,
         LEAST(m.cost_source + m.len_from_start, m.cost_target + m.len_from_end) AS chosen_total,
         m.boundary_point,
         m.geom
  FROM metrics m;

  -- Pick best candidate
  SELECT c.cand_edge_id AS edge_id,
    c.source_node,
    c.target_node,
    c.frac,
    c.chosen_orientation,
    c.boundary_point,
    c.cost_source,
    c.cost_target,
    c.len_from_start,
    c.len_from_end,
    c.total_from_source,
    c.total_from_target,
    c.chosen_total,
    c.geom
  INTO chosen_edge_id,
       chosen_source_node,
       chosen_target_node,
       chosen_frac,
       chosen_orientation,
       chosen_boundary_point,
       chosen_cost_source,
       chosen_cost_target,
       chosen_len_from_start,
       chosen_len_from_end,
       chosen_total_from_source,
       chosen_total_from_target,
       chosen_total,
       term_edge_geom
  FROM _mr_candidates c
  ORDER BY c.chosen_total
  LIMIT 1;

  IF chosen_edge_id IS NULL THEN
    RAISE NOTICE 'No candidate edges for site %, event %', p_site, v_event_id; RETURN; END IF;

  IF chosen_orientation = 'source' THEN
    chosen_from_cost_m := chosen_cost_source;
    chosen_partial_edge_m := chosen_len_from_start;
    chosen_node := chosen_source_node;
    IF chosen_frac <= 0 THEN
      tmp_partial := NULL; -- boundary at source node
    ELSIF chosen_frac >= 1 THEN
      tmp_partial := term_edge_geom; -- whole edge
    ELSE
      tmp_partial := ST_LineSubstring(term_edge_geom, 0.0, chosen_frac);
    END IF;
  ELSE
    chosen_from_cost_m := chosen_cost_target;
    chosen_partial_edge_m := chosen_len_from_end;
    chosen_node := chosen_target_node;
    IF chosen_frac >= 1 THEN
      tmp_partial := NULL; -- boundary at target node
    ELSIF chosen_frac <= 0 THEN
      tmp_partial := term_edge_geom; -- whole edge (approached from target)
    ELSE
      tmp_partial := ST_LineSubstring(term_edge_geom, chosen_frac, 1.0);
    END IF;
  END IF;

  -- Normalize POINT → zero-length LineString, keep NULL as NULL
  IF tmp_partial IS NOT NULL THEN
    IF GeometryType(tmp_partial) = 'POINT' THEN
      term_edge_partial_geom := ST_MakeLine(tmp_partial, tmp_partial)::geometry(LineString,4326);
    ELSE
      term_edge_partial_geom := tmp_partial::geometry(LineString,4326);
    END IF;
  ELSE
    term_edge_partial_geom := NULL; -- no partial segment distinct from base path
  END IF;

  -- Reconstruct base path to chosen endpoint vertex
  DROP TABLE IF EXISTS _mr_path;
  CREATE TEMP TABLE _mr_path AS
  SELECT * FROM pgr_dijkstra(edges_sql, src_node, ARRAY[chosen_node], directed := true);

  SELECT ST_LineMerge(ST_Collect(e.geom)) INTO base_path_geo
  FROM _mr_path p
  JOIN firearea.flow_edges_base e ON e.id = p.edge
  WHERE p.edge <> -1;

  IF base_path_geo IS NULL THEN
    -- Pour point equals chosen node (distance is just partial edge?) Actually then partial edge likely zero; handle gracefully
    base_path_geo := NULL; -- keep NULL; flowpath_full will be just partial segment
  END IF;

  IF base_path_geo IS NOT NULL THEN
    SELECT ST_LineMerge(ST_Collect(ARRAY[base_path_geo, term_edge_partial_geom])) INTO full_path_geo;
  ELSE
    full_path_geo := term_edge_partial_geom; -- degenerate case
  END IF;

  -- Catchment
  SELECT geometry INTO catch_geom FROM firearea.catchments_all WHERE usgs_site = p_site LIMIT 1;

  -- Emit rows
  role := 'flowpath_full'; geom := full_path_geo; site := p_site; event_id := v_event_id;
  distance_m := chosen_total; from_cost_m := chosen_from_cost_m; partial_edge_m := chosen_partial_edge_m; fraction := chosen_frac; edge_id := chosen_edge_id; orientation := chosen_orientation; RETURN NEXT;

  role := 'flowpath_base'; geom := base_path_geo; site := p_site; event_id := v_event_id;
  distance_m := NULL; from_cost_m := NULL; partial_edge_m := NULL; fraction := NULL; edge_id := NULL; orientation := NULL; RETURN NEXT;

  role := 'terminal_edge'; geom := term_edge_geom; site := p_site; event_id := v_event_id;
  distance_m := NULL; from_cost_m := chosen_from_cost_m; partial_edge_m := chosen_partial_edge_m; fraction := chosen_frac; edge_id := chosen_edge_id; orientation := chosen_orientation; RETURN NEXT;

  role := 'terminal_edge_partial'; geom := term_edge_partial_geom; site := p_site; event_id := v_event_id;
  distance_m := NULL; from_cost_m := chosen_from_cost_m; partial_edge_m := chosen_partial_edge_m; fraction := chosen_frac; edge_id := chosen_edge_id; orientation := chosen_orientation; RETURN NEXT;

  role := 'boundary_point'; geom := chosen_boundary_point; site := p_site; event_id := v_event_id;
  distance_m := NULL; from_cost_m := NULL; partial_edge_m := NULL; fraction := NULL; edge_id := NULL; orientation := NULL; RETURN NEXT;

  role := 'pour_point'; geom := (SELECT geometry FROM firearea.pour_points_all WHERE usgs_site = p_site LIMIT 1); site := p_site; event_id := v_event_id;
  distance_m := NULL; from_cost_m := NULL; partial_edge_m := NULL; fraction := NULL; edge_id := NULL; orientation := NULL; RETURN NEXT;

  role := 'fire_boundary'; geom := fire_boundary; site := p_site; event_id := v_event_id;
  distance_m := NULL; from_cost_m := NULL; partial_edge_m := NULL; fraction := NULL; edge_id := NULL; orientation := NULL; RETURN NEXT;

  role := 'fire_polygon'; geom := fire_poly; site := p_site; event_id := v_event_id;
  distance_m := NULL; from_cost_m := NULL; partial_edge_m := NULL; fraction := NULL; edge_id := NULL; orientation := NULL; RETURN NEXT;

  role := 'catchment'; geom := catch_geom; site := p_site; event_id := v_event_id;
  distance_m := NULL; from_cost_m := NULL; partial_edge_m := NULL; fraction := NULL; edge_id := NULL; orientation := NULL; RETURN NEXT;

  RETURN;
END;
$$;

-- Debug / QA function: export hydrologic path components for a single site + event as GeoJSON
-- Returns a FeatureCollection containing:
--   - flowpath_full: full path from pour point to boundary point (including partial edge)
--   - flowpath_base: path to the start node of the terminal edge
--   - terminal_edge: full terminal edge geometry
--   - terminal_edge_partial: portion of terminal edge actually used
--   - boundary_point: point on network closest/intersecting fire boundary
--   - pour_point: original pour point
--   - catchment: catchment polygon
--   - fire_boundary: fire boundary (line) & fire_polygon (polygon)
-- Properties include distance components: from_cost_m, partial_edge_m, total_m
-- (Legacy debug functions removed: debug_hydro_fire_distance_candidates, _candidate_lines,
--  debug_hydro_fire_distance_rows (single closest point), and debug_hydro_fire_distance (GeoJSON)
--  in favor of the parity function debug_hydro_fire_distance_rows2.)
