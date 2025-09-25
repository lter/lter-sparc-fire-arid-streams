# Hydrologic distance to fire boundary (PostGIS + pgRouting)

This SQL builds a routing graph from `firearea.flowlines`, restricts routing to
each site’s catchment, and computes the shortest hydrologic distance from the
catchment pour point to the nearest fire boundary intersection for every fire
event in that catchment.

> Version note
>
> This workflow was authored and tested with pgRouting 3.8. As of pgRouting 4.0, `pgr_createTopology` is deprecated/removed. The script uses `pgr_createTopology` to assign `source`/`target` vertex ids on `firearea.flow_edges_base`. For pgRouting 4.x, you’ll need to build these columns yourself. See the “Using pgRouting 4.x (no pgr_createTopology)” section below for a drop-in alternative. Migration docs: https://docs.pgrouting.org/latest/en/migration.html#migration-of-pgr-createtopology

## requirements
- Database `wildfire` with schema `firearea`
- Extensions: `postgis`, `pgrouting`
- Tables/geometry present:
  - `firearea.flowlines (usgs_site, nhdplus_comid, geometry LINESTRING 4326)`
  - `firearea.pour_points (usgs_site, geometry POINT 4326)`
  - `firearea.catchments`, `firearea.non_usgs_catchments` (geometry 4326)
  - `firearea.fires_catchments (usgs_site, event_id, geometry POLYGON/MULTIPOLYGON 4326)`

## what the script creates
- `firearea.catchments_all` view
- `firearea.flow_edges_base` edges + topology (`_vertices_pgr` auto-created)
- `firearea.pour_point_nodes` nearest vertex per pour point
- `firearea.flow_edges_in_catchment` view to limit graph to contributing network
- `firearea.hydro_fire_min_distance` results table
- `firearea.compute_hydro_fire_distances(site text)` per-site compute function
(now performs on-demand boundary distance aggregation; no global intersection
table)

## run order
1. Open and run `pg_hydro_fire_distance.sql` in the `wildfire` DB.
2. For a site, compute distances:  
   `SELECT firearea.compute_hydro_fire_distances('USGS-09474000');`
3. Inspect results:  
   `SELECT * FROM firearea.hydro_fire_min_distance WHERE usgs_site='USGS-09474000' ORDER BY min_distance_m;`

## notes
- Routing is undirected (reverse_cost = cost). If you need strict
downstream-only paths, we can add directionality when flow direction attributes
are available.
- Boundary targeting uses `ST_Boundary(f.geometry)` so distances are to the fire
edge along the network. Switch to polygon interior if needed.
- Topology tolerance is ~1 m (1e-5 degrees). If networks aren’t connected at
endpoints, increase slightly (e.g., 5e-5).

## using pgRouting 4.x (no pgr_createTopology)

In pgRouting 4.0+, `pgr_createTopology` was deprecated/removed. You must create
and populate the routing topology columns (`source`, `target`, `cost`,
`reverse_cost`) yourself and optionally generate a vertices table. The official
guidance is to use `pgr_extractVertices` and standard SQL/geometry operations to
assign vertex ids. Below is a concrete adaptation for this workflow.

What you need on `firearea.flow_edges_base` (same shape as 3.8):
- Columns: `id bigserial primary key`, `usgs_site text`, `geom
geometry(LineString,4326)`, `length_m double precision`, `source bigint`,
`target bigint`, `cost double precision`, `reverse_cost double precision`.
- Populate `length_m`, `cost`, `reverse_cost` as in the script (meters via
`ST_Length(geom::geography)`).
- Populate `source`/`target` using a vertices table derived from the edges.

Step-by-step replacement for section “Create topology (assign source/target
nodes)”: 
1) Ensure the edges exist (as the script already does):
   - Insert one row per flowline into `firearea.flow_edges_base` with `cost` and
   `reverse_cost` set to edge length (or your preferred weighting).
   
2) Build a vertices table from edges using `pgr_extractVertices`:
   - Create a temp or permanent vertices table that includes vertex id and point
   geometry.
   - Example (temporary table):
     - `CREATE TEMP TABLE _vertices AS SELECT * FROM pgr_extractVertices('SELECT
     id, geom FROM firearea.flow_edges_base ORDER BY id');`
     - The returned columns include `id` (vertex id), `geom` (POINT), and degree
     arrays you can ignore here.

3) Fill `source`/`target` on the edges:
   - Join each edge’s start/end point to the vertex table. Using exact equality
   works if your edges are cleanly noded. If not, use a small
   tolerance/snapping.
   - Exact match approach:
     - `UPDATE firearea.flow_edges_base e
        SET source = v.id
        FROM _vertices v
        WHERE ST_StartPoint(e.geom) = v.geom;`
     - `UPDATE firearea.flow_edges_base e
        SET target = v.id
        FROM _vertices v
        WHERE ST_EndPoint(e.geom) = v.geom;`
   - If exact equality fails due to floating precision, prefer nearest-vertex within a small threshold (about the same as the former topology tolerance ~1 m):
     - `UPDATE firearea.flow_edges_base e
        SET source = v.id
        FROM LATERAL (
          SELECT id FROM _vertices
          ORDER BY geom <-> ST_StartPoint(e.geom)
          LIMIT 1
        ) v
        WHERE e.id = e.id; -- correlate row`
     - Repeat similarly for `target` with `ST_EndPoint(e.geom)`.
     - Optionally guard with a maximum distance using `ST_DWithin` to catch problematic edges.

4) Indexes the script relies on still apply:
   - `CREATE INDEX IF NOT EXISTS flow_edges_base_geom_idx ON firearea.flow_edges_base USING gist (geom);`
   - `CREATE INDEX IF NOT EXISTS flow_edges_base_site_idx ON firearea.flow_edges_base (usgs_site);`
   - After populating `source` and `target`, consider btree indexes on them if you run vertex-centric queries frequently: `CREATE INDEX ON firearea.flow_edges_base (source); CREATE INDEX ON firearea.flow_edges_base (target);`

5) Vertices catalog table:
   - In pgRouting 3.8, `pgr_createTopology` also produced `<edges>_vertices_pgr`. In 4.x you can keep using the `_vertices` table you created with `pgr_extractVertices`. If you want it permanent, create it in the `firearea` schema and index it: `CREATE INDEX ON firearea.flow_edges_base_vertices_pgr (id); CREATE INDEX ON firearea.flow_edges_base_vertices_pgr USING gist (the_geom);` (match your actual column names from `pgr_extractVertices`).

6) The rest of this workflow is unchanged:
   - The Dijkstra call uses `id, source, target, cost, reverse_cost` from
   `firearea.flow_edges_in_catchment`. As long as those columns are present and
   populated, the per-site routing and event-distance logic runs identically on
   4.x.

References:
- pgRouting migration: `pgr_createTopology` deprecation and replacement with
`pgr_extractVertices`:
https://docs.pgrouting.org/latest/en/migration.html#migration-of-pgr-createtopology
- Concept of building routing topology (standardized columns):
https://docs.pgrouting.org/latest/en/pgRouting-concepts.html

## maintenance
- If you add new flowlines, re-run the section creating `flow_edges_base` (and
   rebuild topology if needed) then rerun distances for affected sites.
- If you add or change fires (`fires_catchments`), you only need to call
   `compute_hydro_fire_distances` again for impacted sites. Boundary intersections
   are now computed on-demand; no global table rebuild required.

## troubleshooting
- If a site returns no distance:
   - Ensure its pour point snapped to a vertex (`pour_point_nodes`).
   - Confirm there are edges in its catchment (`flow_edges_in_catchment`).
   - Check that at least one edge intersects or is within the near-distance
      tolerance (20 m) of a fire boundary for that site (`fires_catchments`).
   - Increase the near-distance tolerance in the function (constant `near_meters`)
      if boundaries nearly touch but do not intersect.

## design change: on-demand boundary intersections
Previously the workflow materialized a global table
`firearea.fire_boundary_points` containing every (site,event,edge) intersection
or near-miss point. This caused large storage and processing overhead. The
refactored design (Strategy B) computes, for each site processed, the minimum
hydrologic distance per event directly by pairing candidate edges with that
site's fire boundaries and locating the closest boundary point along each edge.
No persistent intersection table is created; only final distances are stored.

## debugging / QA workflow

Use the unified (production-parity) debug function
`firearea.debug_hydro_fire_distance_rows2(site text, event_id text)` to visually
inspect how a hydrologic distance was chosen. It reconstructs the exact path and
the terminal edge portion actually used.

### 1. Identify site + event needing inspection
```
SELECT usgs_site, event_id, min_distance_m
FROM firearea.hydro_fire_min_distance
WHERE usgs_site = 'USGS-09404900'
ORDER BY min_distance_m;
```

### 2. See provenance components (detail table)
```
SELECT *
FROM firearea.hydro_fire_min_distance_detail
WHERE usgs_site='USGS-09404900'
   AND event_id='UT3743511264420120702';
```
Columns of interest: `edge_id`, `orientation`, `fraction` (fractional position
along the terminal edge), `from_cost_m` (network distance to chosen endpoint
vertex), `partial_edge_m` (distance along terminal edge portion),
`min_distance_m` (sum).

### 3. Export debug geometries for GIS
The debug function returns multiple rows distinguished by a `role` column:
- flowpath_full
- flowpath_base
- terminal_edge
- terminal_edge_partial
- boundary_point
- pour_point
- fire_boundary
- fire_polygon
- catchment

Full export (GeoJSON):
```
ogr2ogr -f GeoJSON flowpath_debug_USGS-09404900_UT3743511264420120702.geojson \
PG:"host=YOURHOST dbname=wildfire user=YOURUSER password=YOURPASS" \ -sql
"SELECT role, geom, site, event_id, distance_m, from_cost_m, partial_edge_m,
fraction, edge_id, orientation FROM
firearea.debug_hydro_fire_distance_rows2('USGS-09404900','UT3743511264420120702')"
```

Minimal export (only final full path):
```
ogr2ogr -f GeoJSON flowpath_only.geojson \
   PG:"host=YOURHOST dbname=wildfire user=YOURUSER password=YOURPASS" \
   -sql "SELECT * FROM firearea.debug_hydro_fire_distance_rows2('USGS-09404900','UT3743511264420120702') WHERE role='flowpath_full'"
```

GeoPackage (keeps multiple geometry types cleanly for QGIS styling):
```
ogr2ogr -f GPKG flowpath_debug.gpkg \
   PG:"host=YOURHOST dbname=wildfire user=YOURUSER password=YOURPASS" \
   -sql "SELECT role, geom, site, event_id, distance_m, from_cost_m, partial_edge_m, fraction, edge_id, orientation FROM firearea.debug_hydro_fire_distance_rows2('USGS-09404900','UT3743511264420120702')"
```

### 4. Load in QGIS
Option A: Drag the exported `.gpkg` or `.geojson` into QGIS.
Option B: Add a PostGIS connection and run the same SQL in the QGIS DB Manager;
or simply add the function call as a virtual layer with the above SELECT.

Suggested styling:
- Symbol layer rule-based on `role`.
   - flowpath_full: wide solid color (e.g., blue, width 1.2)
   - terminal_edge_partial: dashed overlay or contrasting color
   - terminal_edge: thin outline gray
   - boundary_point & pour_point: distinct point symbols (circle vs star)
   - fire_boundary: red/orange line
   - fire_polygon & catchment: transparent fill with thin outline
- Label `flowpath_full` with `distance_m` (format to integer or 1 decimal).

### 5. Interpret orientation and fraction
- If `orientation='source'`, the path reached the boundary point moving from the
edge's source vertex forward `fraction * edge_length` along the edge.
- If `orientation='target'`, it approached from the target vertex; the used
portion length is from the boundary point back to the target (`partial_edge_m =
len_from_end`).
- Fraction is always measured from the edge start (source→target direction),
even when orientation='target'.

### 6. Adjust tolerance (if candidates missing)
If the boundary almost touches but isn't selected, increase `near_meters`
(default 20.0) inside the function definitions and rerun the compute for that
site. Keep it small to avoid spurious candidate edges.

### 7. Re-run after edits
After modifying SQL functions:
```
-- Reload functions (psql or GUI) then:
SELECT firearea.compute_hydro_fire_distances('USGS-09404900');
SELECT * FROM firearea.hydro_fire_min_distance_detail WHERE usgs_site='USGS-09404900' LIMIT 5;
```

### 8. Quick regression spot-check
Pick several random events and confirm recomputed debug path distance matches stored summary:
```
WITH sample AS (
   SELECT event_id FROM firearea.hydro_fire_min_distance
   WHERE usgs_site='USGS-09404900'
   ORDER BY random() LIMIT 5
)
SELECT s.event_id,
          d.min_distance_m AS stored,
          (SELECT distance_m FROM firearea.debug_hydro_fire_distance_rows2('USGS-09404900', s.event_id) WHERE role='flowpath_full') AS recomputed
FROM sample s
JOIN firearea.hydro_fire_min_distance d USING (event_id)
WHERE d.usgs_site='USGS-09404900';
```
Any discrepancies > small floating precision (millimeters) should be investigated.

### 9. Common issues
- No rows from debug function: event_id typo or site/event pair not in
`fires_catchments`.
- Distance unexpectedly large: verify there isn’t a nearer boundary point on the
same terminal edge (multi-intersection logic should handle this) and confirm
flow network connectivity (no unintended detours). Check for missing flowlines
in `flow_edges_in_catchment`.
- Fraction very close to 0 or 1: boundary point is almost exactly at an
endpoint; verify if a neighboring edge would have been shorter (possible
topology gap).

---