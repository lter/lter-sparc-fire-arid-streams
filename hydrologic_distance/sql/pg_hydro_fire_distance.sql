-- Hydrologic shortest distance from catchment pour point to fire boundary intersections
-- Database: wildfire (schema firearea)
-- Requires: postgis, pgrouting

-- 0) Extensions
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pgrouting;

-- 1) Unified catchments (USGS + non-USGS)
DROP VIEW IF EXISTS firearea.catchments_all CASCADE;
CREATE VIEW firearea.catchments_all AS
SELECT usgs_site, geometry FROM firearea.catchments
UNION ALL
SELECT usgs_site, geometry FROM firearea.non_usgs_catchments;

-- 2) Build routing edges from flowlines (one edge per flowline)
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

-- 3) Create topology (assign source/target nodes)
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

-- 4) Pour-point to nearest vertex per site
DROP TABLE IF EXISTS firearea.pour_point_nodes CASCADE;
CREATE TABLE firearea.pour_point_nodes AS
SELECT p.usgs_site,
       n.id AS node_id,
       p.geometry AS geom
FROM firearea.pour_points p
JOIN LATERAL (
  SELECT nn.id, nn.the_geom
  FROM firearea.flow_edges_base_vertices_pgr nn
  JOIN firearea.flow_edges_base e ON nn.id IN (e.source, e.target) AND e.usgs_site = p.usgs_site
  ORDER BY nn.the_geom <-> p.geometry
  LIMIT 1
) n ON TRUE;

CREATE INDEX IF NOT EXISTS pour_point_nodes_site_idx ON firearea.pour_point_nodes (usgs_site);
CREATE INDEX IF NOT EXISTS pour_point_nodes_geom_idx ON firearea.pour_point_nodes USING gist (geom);

-- 5) Subgraph edges limited to catchment (contributing network proxy)
DROP VIEW IF EXISTS firearea.flow_edges_in_catchment CASCADE;
CREATE VIEW firearea.flow_edges_in_catchment AS
SELECT e.*
FROM firearea.flow_edges_base e
JOIN firearea.catchments_all c USING (usgs_site)
WHERE ST_Intersects(e.geom, c.geometry);

-- 6) Fire boundary intersection points per event and edge
DROP TABLE IF EXISTS firearea.fire_boundary_points CASCADE;
CREATE TABLE firearea.fire_boundary_points AS
SELECT f.usgs_site,
       f.event_id,
       e.id AS edge_id,
       (ST_Dump(
          ST_Intersection(e.geom, ST_Boundary(f.geometry))
       )).geom::geometry(Point,4326) AS pt
FROM firearea.fires_catchments f
JOIN firearea.flow_edges_in_catchment e ON e.usgs_site = f.usgs_site
WHERE ST_Intersects(e.geom, f.geometry);

CREATE INDEX IF NOT EXISTS fire_boundary_points_site_evt_idx ON firearea.fire_boundary_points(usgs_site,event_id);
CREATE INDEX IF NOT EXISTS fire_boundary_points_edge_idx ON firearea.fire_boundary_points(edge_id);
CREATE INDEX IF NOT EXISTS fire_boundary_points_geom_idx ON firearea.fire_boundary_points USING gist (pt);

-- 7) Results table
DROP TABLE IF EXISTS firearea.hydro_fire_min_distance CASCADE;
CREATE TABLE firearea.hydro_fire_min_distance (
  usgs_site text NOT NULL,
  event_id text NOT NULL,
  min_distance_m double precision NOT NULL,
  PRIMARY KEY (usgs_site, event_id)
);

-- 8) Compute function (per-site)
DROP FUNCTION IF EXISTS firearea.compute_hydro_fire_distances(site text);
CREATE OR REPLACE FUNCTION firearea.compute_hydro_fire_distances(site text)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  src_node bigint;
  edges_sql text;
BEGIN
  -- source node from pour points
  SELECT node_id INTO src_node FROM firearea.pour_point_nodes WHERE usgs_site = site;
  IF src_node IS NULL THEN
    RAISE NOTICE 'No source node for %', site; RETURN;
  END IF;

  -- edge SQL restricted to this site's catchment
  edges_sql := format('SELECT id, source, target, cost, reverse_cost FROM firearea.flow_edges_in_catchment WHERE usgs_site = %L', site);

  -- Route from source to all vertices within catchment
  DROP TABLE IF EXISTS _tree;
  CREATE TEMP TABLE _tree AS
  SELECT *
  FROM pgr_dijkstra(edges_sql, src_node,
                    ARRAY(SELECT DISTINCT v.id
                          FROM firearea.flow_edges_base_vertices_pgr v
                          JOIN firearea.flow_edges_in_catchment e ON v.id IN (e.source, e.target)
                          WHERE e.usgs_site = site),
                    directed := false);

  -- Cumulative cost per vertex
  DROP TABLE IF EXISTS _tc;
  CREATE TEMP TABLE _tc AS
  SELECT end_vid, MAX(agg_cost) AS total_cost
  FROM _tree
  GROUP BY end_vid;

  -- Edge-level from-node costs
  DROP TABLE IF EXISTS _edge_cost;
  CREATE TEMP TABLE _edge_cost AS
  SELECT e.id AS edge_id,
         e.source AS from_node,
         e.target AS to_node,
         e.cost   AS edge_cost,
         tc.total_cost AS from_cost
  FROM firearea.flow_edges_in_catchment e
  JOIN _tc tc ON tc.end_vid = e.source
  WHERE e.usgs_site = site;

  -- Per-event exact distance using boundary intersection offsets
  DROP TABLE IF EXISTS _event_dist;
  CREATE TEMP TABLE _event_dist AS
  SELECT fbp.event_id,
         MIN(
           from_cost + ST_Length(
             ST_LineSubstring(e.geom,
               0.0,
               LEAST(1.0, GREATEST(0.0, ST_LineLocatePoint(e.geom, fbp.pt)))
             )::geography
           )
         ) AS min_distance_m
  FROM firearea.fire_boundary_points fbp
  JOIN _edge_cost ec ON ec.edge_id = fbp.edge_id
  JOIN firearea.flow_edges_base e ON e.id = fbp.edge_id
  WHERE fbp.usgs_site = site
  GROUP BY fbp.event_id;

  -- Upsert results
  INSERT INTO firearea.hydro_fire_min_distance (usgs_site, event_id, min_distance_m)
  SELECT site, event_id, min_distance_m
  FROM _event_dist
  ON CONFLICT (usgs_site, event_id) DO UPDATE SET min_distance_m = EXCLUDED.min_distance_m;

  DROP TABLE IF EXISTS _tree;
  DROP TABLE IF EXISTS _tc;
  DROP TABLE IF EXISTS _edge_cost;
  DROP TABLE IF EXISTS _event_dist;
END;
$$;
