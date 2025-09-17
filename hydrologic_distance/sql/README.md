# Hydrologic distance to fire boundary (PostGIS + pgRouting)

This SQL builds a routing graph from `firearea.flowlines`, restricts routing to each site’s catchment, and computes the shortest hydrologic distance from the catchment pour point to the nearest fire boundary intersection for every fire event in that catchment.

## Requirements
- Database `wildfire` with schema `firearea`
- Extensions: `postgis`, `pgrouting`
- Tables/geometry present:
  - `firearea.flowlines (usgs_site, nhdplus_comid, geometry LINESTRING 4326)`
  - `firearea.pour_points (usgs_site, geometry POINT 4326)`
  - `firearea.catchments`, `firearea.non_usgs_catchments` (geometry 4326)
  - `firearea.fires_catchments (usgs_site, event_id, geometry POLYGON/MULTIPOLYGON 4326)`

## What the script creates
- `firearea.catchments_all` view
- `firearea.flow_edges_base` edges + topology (`_vertices_pgr` auto-created)
- `firearea.pour_point_nodes` nearest vertex per pour point
- `firearea.flow_edges_in_catchment` view to limit graph to contributing network
- `firearea.fire_boundary_points` exact intersection points of network edges with fire boundaries (by event)
- `firearea.hydro_fire_min_distance` results table
- `firearea.compute_hydro_fire_distances(site text)` per-site compute function

## Run order
1. Open and run `pg_hydro_fire_distance.sql` in the `wildfire` DB.
2. For a site, compute distances:  
   `SELECT firearea.compute_hydro_fire_distances('USGS-09474000');`
3. Inspect results:  
   `SELECT * FROM firearea.hydro_fire_min_distance WHERE usgs_site='USGS-09474000' ORDER BY min_distance_m;`

## Notes
- Routing is undirected (reverse_cost = cost). If you need strict downstream-only paths, we can add directionality when flow direction attributes are available.
- Boundary targeting uses `ST_Boundary(f.geometry)` so distances are to the fire edge along the network. Switch to polygon interior if needed.
- Topology tolerance is ~1 m (1e-5 degrees). If networks aren’t connected at endpoints, increase slightly (e.g., 5e-5).

## Maintenance
- If you add new flowlines or fires, re-run the script section creating `flow_edges_base` and `fire_boundary_points`, or convert them into materialized views and refresh.

## Troubleshooting
- If a site returns no distance, ensure its pour point is near the network (check `pour_point_nodes`) and that at least one edge intersects the fire polygon boundary (`fire_boundary_points`).
