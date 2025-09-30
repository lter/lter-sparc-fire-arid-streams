# Hydrologic Distance (Wildfire → Stream Pour Point) Module

This directory contains the lightweight along‐network distance workflow used to compute hydrologic (flowline) distances from a site’s single pour point to historical wildfire event catchments. It is a streamlined, dependency‐minimal alternative to earlier, heavier pgRouting + point‑insertion logic.

---
## Core Objectives
1. Build a routable river/flowline graph (per `usgs_site`).
2. Snap each site’s pour point(s) to the network, retaining fractional edge position & diagnostic quality metrics.
3. Intersect wildfire event polygons with the network to derive edge/fraction pairs that represent a fire’s contact point(s) with the flow network.
4. Compute shortest undirected network distances from the pour point to each fire event (one row per event), including offset adjustment when the pour point lies mid‑edge.
5. Persist results and rich run‑time diagnostics for reproducibility & troubleshooting.

---
## High‑Level Workflow
```
(fn_build_network_topology) → network_edges (+ vertices) →
(fn_refresh_pour_point_snaps) → pour_point_snaps →
(fn_compute_site_fire_distances_lite) → site_fire_distances (+ logs)
```
Optional debug: `fn_debug_withpoints` (tests environment pgRouting behavior).

---
## Functions Overview

### 1. `firearea.fn_build_network_topology(rebuild boolean DEFAULT true, in_tolerance double precision DEFAULT 0.5, in_srid integer DEFAULT 5070)`
Builds or rebuilds the graph from base flowlines (not shown in this repo excerpt). Produces:
- `firearea.network_edges`: Directed edges with source/target vertex ids & length.
- `firearea.network_edges_vertices_pgr`: Vertex table required by pgRouting.

Key responsibilities:
- Snap & clean raw flowline geometry.
- Generate source/target vertex ids (using `pgr_createTopology`).
- Create projected geometry column (e.g. `geom_5070`) for meter‑based distances.

### 2. `firearea.fn_refresh_pour_point_snaps(in_vertex_snap_tolerance double precision DEFAULT 0.2, in_srid integer DEFAULT 5070)`
Rebuilds `pour_point_snaps` from raw pour point sources.

Highlights:
- Site‑filtered nearest‐edge KNN; no global vertex KNN (performance).
- Flags `is_exact_vertex` when point lies within `in_vertex_snap_tolerance` of an endpoint.
- Produces fractional position along edge (`snap_fraction`) and planar snap distance.

### 3. `firearea.fn_rebuild_network_and_snaps(in_network_tolerance double precision DEFAULT 0.5, in_vertex_snap_tolerance double precision DEFAULT 0.2, in_srid integer DEFAULT 5070)`
Convenience orchestrator calling (2) & (3) sequentially.

### 4. `firearea.fn_compute_site_fire_distances_lite(in_usgs_site text, in_snap_tolerance_m double precision DEFAULT 60.0, in_include_unreachable boolean DEFAULT true)`
Computes one distance per wildfire `event_id` for the site. Removes upstream enforcement, path materialization, and `pgr_withPoints` reliance.

Steps (internally):
1. Select pour point (closest snap if more than one).
2. Build intersection points: boundary intersections + midpoint fallback for contained edges.
3. Distill unique fire event edges + vertex endpoints.
4. Run `pgr_dijkstra` once from starting vertex to all needed vertices (undirected).
5. Resolve per event: `min( source_path + edge.len * fraction, target_path + edge.len * (1-fraction) ) + pour_point_offset`.
6. Insert reachable rows; optionally tag unreachable events.
7. Emit detailed step log to `site_fire_distance_run_log`.

### 5. `firearea.fn_debug_withpoints(in_site text)`
Minimal environment probe for `pgr_withPoints`. Used only to prove local pgRouting installation characteristics (since the lite function avoids it entirely).

---
## Persistent Tables & Columns

### `firearea.network_edges`
| Column | Meaning |
|--------|---------|
| edge_id (bigint) | Unique edge identifier (stable per rebuild cycle). |
| usgs_site (text) | Site partition key (lowercased). |
| source (bigint) | Start vertex id (FK to `network_edges_vertices_pgr.id`). |
| target (bigint) | End vertex id. |
| length_m (double precision) | Edge length (meters in projected SRID). |
| cost, reverse_cost (double precision) | Routing weights (normally = length_m for undirected use). |
| geom_5070 (geometry(LineString,5070)) | Projected geometry for distance work. |
| (Other raw attributes) | Derived from base flowlines (not fully enumerated here). |

### `firearea.network_edges_vertices_pgr`
| Column | Meaning |
|--------|---------|
| id (bigint) | Vertex identifier. |
| the_geom (geometry(Point, 5070 or base SRID)) | Vertex geometry. |

### `firearea.pour_point_snaps`
| Column | Meaning |
|--------|---------|
| usgs_site | Lowercase site id. |
| identifier | Stable pour point logical id (synthesized if missing). |
| comid | Optional NHDPlus COMID. |
| sourceName | Metadata passthrough. |
| reachcode | NHD reach code if provided. |
| measure | Linear referencing measure if present. |
| original_geom (Point,4326) | Input geometry before projection/snapping. |
| snap_geom (Point,5070) | Projected working geometry. |
| snap_edge_id (bigint) | Edge chosen via site KNN. |
| snap_fraction (double precision [0,1]) | Fraction from edge source→target. |
| snap_distance_m (double precision) | Planar point‑to‑edge distance. |
| is_exact_vertex (boolean) | True if within tolerance of endpoint. |
| created_at (timestamptz) | Insertion timestamp. |

### `firearea.fires_catchments`
(Reference wildfire polygons; only columns used here are listed.)
| Column | Meaning |
|--------|---------|
| usgs_site | Site id filtered on. |
| event_id | Unique wildfire event identifier. |
| ig_date (date) | Ignition (or representative) date. |
| geometry (Multi/Poly) | Fire boundary geometry (SRID 4326). |

### `firearea.site_fire_distances`
| Column | Meaning |
|--------|---------|
| result_id (serial/bigint) | Primary key. |
| usgs_site | Site id. |
| pour_point_identifier | Pour point used (from snaps). |
| pour_point_comid | COMID (if any). |
| event_id | Fire event id. |
| ig_date | Event date. |
| distance_m (double precision) | Along‑network distance (meters) including pour point offset. |
| path_edge_ids (text / NULL) | Placeholder (NULL in lite version). |
| status (text) | 'ok', 'unreachable', or other codes. |
| message (text) | Implementation tag ('lite', 'lite_no_path'). |
| created_at (timestamptz default) | Insertion timestamp (if table has default). |

### `firearea.site_fire_distance_log`
| Column | Meaning |
|--------|---------|
| usgs_site | Site id. |
| pour_point_identifier | (Nullable) When available. |
| status | High‑level status ('no_network','no_fire','error', etc.). |
| detail | Human readable explanation / error. |
| created_at | Timestamp. |

### `firearea.site_fire_distance_run_log`
| Column | Meaning |
|--------|---------|
| run_id | Unique id for a run (timestamp + random). |
| usgs_site | Site id. |
| step | Step label (start_lite, event_points_lite, done_lite, etc.). |
| detail | Step diagnostics (counts, component info, reachable stats). |
| created_at | Timestamp. |

### Ephemeral (Session / Debug) Tables
Created inside `fn_compute_site_fire_distances_lite` (UNLOGGED, dropped per run):
- `_lite_event_points` (event_id, ig_date, edge_id, fraction)
- `_lite_event_edges` (distinct event edges with source/target/length)
- `_lite_vertices_needed` (list of vertex ids needed for Dijkstra targets)
- `_lite_components` (component diagnostics from `pgr_connectedComponents`)
- `_lite_vertex_sp` (shortest path distances per vertex_id)
- `_lite_event_candidate` (per event edge candidate distances via source/target)
- `_lite_event_min` (per event minimal distance before pour point offset)

These persist only for the session; querying them immediately after a function call (within the same session/connection) is allowed for debugging.

---
## Typical Usage Sequence
```sql
-- 1. (Re)build network & vertex topology (only when flowlines change)
SELECT firearea.fn_build_network_topology(true);  -- or fn_rebuild_network_and_snaps

-- 2. Refresh pour points (after network or pour point edits)
SELECT firearea.fn_refresh_pour_point_snaps();

-- 3. Compute distances for one site
SELECT firearea.fn_compute_site_fire_distances_lite('syca');

-- 4. Inspect results
SELECT * FROM firearea.site_fire_distances WHERE usgs_site='syca' ORDER BY distance_m;

-- 5. Examine run diagnostics (latest first)
SELECT step, detail
FROM firearea.site_fire_distance_run_log
WHERE usgs_site='syca'
ORDER BY created_at DESC;
```

### Batch over all sites
```sql
-- Example: iterate over distinct sites present in pour_point_snaps
DO $$
DECLARE r record; BEGIN
  FOR r IN (SELECT DISTINCT usgs_site FROM firearea.pour_point_snaps) LOOP
    PERFORM firearea.fn_compute_site_fire_distances_lite(r.usgs_site);
  END LOOP; END $$;
```

### Filter Fires Within X km
```sql
SELECT event_id, ig_date, ROUND(distance_m/1000.0,2) AS dist_km
FROM firearea.site_fire_distances
WHERE usgs_site='syca' AND status='ok' AND distance_m <= 20000
ORDER BY distance_m;
```

### Distance Distribution / Histogram
```sql
SELECT width_bucket(distance_m, 0, 50000, 10) AS bucket, COUNT(*)
FROM firearea.site_fire_distances
WHERE usgs_site='syca' AND status='ok'
GROUP BY bucket ORDER BY bucket;
```

### Identify Unreachable Events (if any)
```sql
SELECT event_id, ig_date
FROM firearea.site_fire_distances
WHERE usgs_site='syca' AND status='unreachable';
```

### Join Candidate Distances with Fire Metadata
(Assumes additional fire metadata table `firearea.fire_events` exists.)
```sql
SELECT d.event_id, d.ig_date, d.distance_m, fe.*
FROM firearea.site_fire_distances d
LEFT JOIN firearea.fire_events fe USING (event_id)
WHERE d.usgs_site='syca';
```

### Debug Shortest Path Coverage
Immediately after a run (same session):
```sql
SELECT COUNT(*) AS n_vertices, MIN(dist) AS min_d, MAX(dist) AS max_d
FROM firearea._lite_vertex_sp;
```

---
## Interpreting Diagnostics
| Step | Interpretation | Action if Problematic |
|------|----------------|-----------------------|
| start_lite | Function entered. | — |
| pour_point_selected_lite | Snap chosen & start vertex. | If snap_distance large ⇒ QA pour point or tolerance. |
| pour_point_offset_lite | Offset due to mid‑edge pour point. | Large offset might mean long headwater edge. |
| event_points_lite | Total raw intersection + fallback points. | Zero ⇒ geometry mismatch (SRID / coverage). |
| event_edges_lite | Distinct event edges. | Very large vs events ⇒ complex fire boundaries. |
| vertices_needed_lite | Unique endpoints for those edges. | High ratio edges:vertices suggests linear clustering. |
| components_lite | Component counts for event edges vs pour point component. | Nonzero other_edges ⇒ disconnected network segment. |
| sp_rows_lite | Reached vertices (should ≥ vertices_needed). | If < needed ⇒ gap in topology. |
| event_min_lite | Rows = number of distinct events. | If << event_edges, expected (aggregation). |
| done_lite | Final counts inserted. | Confirm reachable vs unreachable. |

---
## Performance Notes
- Single `pgr_dijkstra` call per site (multi‑target) keeps complexity low.
- Projected geometry column reuse avoids repeated ST_Transform.
- UNLOGGED temp tables reduce write overhead (acceptable; results persisted only in final table).
- Indexes to consider (if not already present):
  - `CREATE INDEX ON firearea.network_edges (usgs_site);`
  - `CREATE INDEX ON firearea.pour_point_snaps (usgs_site);`
  - `CREATE INDEX ON firearea.site_fire_distances (usgs_site, status);`

---
## Troubleshooting Quick Guide
| Symptom | Likely Cause | Remedy |
|---------|--------------|--------|
| 0 event_points | Fire polygons not intersecting network; SRID mismatch. | Verify SRIDs & network coverage. |
| All events unreachable | Different connected component or pour point too far off network. | Inspect `components_lite`; rebuild topology with adjusted tolerance. |
| Snap distance unexpectedly large | Bad pour point coordinates or missing transform. | Re‑check raw pour point source SRID. |
| sp_rows_lite < vertices_needed | Edge directionality or missing edges. | Confirm edges SQL includes both cost & reverse_cost; rebuild topology. |
| Slow pour point refresh | Very large network or missing index. | Ensure `(usgs_site)` index; consider simplifying geometry. |

---
## Extensibility Ideas
- Add upstream‐only enforcement (filter by flow direction) with a direction mask table.
- Store the chosen start vertex id & offset directly in `site_fire_distances` for audit.
- Add materialized view summarizing: earliest fire date within distance bands.
- Implement incremental pour point refresh (changed points only).
- Introduce path reconstruction optionally (store edge sequence) using on‑demand pgr_dijkstra path queries.

---
## Example: Recompute Everything Safely (Single Site)
```sql
-- Optional: clear old diagnostics (preserves historical distance rows if you skip the delete)
DELETE FROM firearea.site_fire_distance_run_log WHERE usgs_site='syca';
DELETE FROM firearea.site_fire_distance_log WHERE usgs_site='syca';
DELETE FROM firearea.site_fire_distances WHERE usgs_site='syca';

SELECT firearea.fn_build_network_topology(true);  -- only if flowlines changed
SELECT firearea.fn_refresh_pour_point_snaps();
SELECT firearea.fn_compute_site_fire_distances_lite('syca', 60.0, true);

SELECT * FROM firearea.site_fire_distances WHERE usgs_site='syca' ORDER BY distance_m;
```

---
## License / Data Provenance
(Adapt this section to your project’s actual licensing.)
- Underlying hydrography: NHDPlus (USGS/EPA) – follow original data license.
- Wildfire polygons: Source dataset (e.g., MTBS or agency feed) – cite appropriately.
- This SQL logic: Add repository/project license (MIT / Apache 2.0 / etc.) as appropriate.

---
## Glossary
| Term | Definition |
|------|-----------|
| Pour Point | The designated monitoring location or outlet for a site. |
| Snap | Nearest edge association plus fractional position. |
| Fraction | 0 at edge source vertex; 1 at edge target. |
| Offset | Linear distance from chosen start vertex to the actual pour point if mid‑edge. |
| Component | Connected subgraph identifier from `pgr_connectedComponents`. |

---
## Quick Sanity Checklist Before Large Batch Run
- [ ] `network_edges` populated and has reasonable edge count per site.
- [ ] `geom_5070` (or chosen SRID) present & non‑NULL.
- [ ] `pour_point_snaps` row per site (snap_distance_m within tolerance).
- [ ] Fires exist (`fires_catchments` not empty for target sites).
- [ ] Test one site → verify run log has expected progression and non‑zero results.

---
## Getting Help
Capture and share:
1. Run log subset: `SELECT * FROM firearea.site_fire_distance_run_log WHERE usgs_site='SITE' ORDER BY created_at;`
2. Snap diagnostics: `SELECT * FROM firearea.pour_point_snaps WHERE usgs_site='SITE';`
3. Component mismatch: Output of `components_lite` step.

These three artifacts usually pinpoint issues (snap tolerance, component isolation, or missing edges).

---
Happy routing! 🚰🔥
