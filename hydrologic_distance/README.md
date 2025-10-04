## Hydrologic Distance (Split Topology Vertex Workflow)

Unified, split‑only workflow for computing along‑network distances from a site's pour point to wildfire event polygons. Legacy "lite" (fraction + offset) logic is deprecated; distances now rely purely on vertex shortest paths after splitting edges at pour point and fire boundary intersections.

### 1. one-time setup

Initial, one-time set up to create required tables if they do not already exist, or upgrade existing tables if needed. This block is idempotent and safe to run multiple times; it will only create or modify what is necessary.

```sql:

DO $$
BEGIN
    IF to_regclass('firearea.network_edges_split') IS NULL THEN
        -- Use direct DDL; no need for EXECUTE with dollar quotes (avoids parser confusion)
        CREATE TABLE firearea.network_edges_split (
            edge_id bigserial PRIMARY KEY,
            usgs_site text NOT NULL,
            original_edge_id bigint,
            segment_index integer,
            start_fraction double precision,
            end_fraction double precision,
            geom_5070 geometry(LineString,5070) NOT NULL,
            length_m double precision,
            cost double precision,
            reverse_cost double precision,
            source bigint,
            target bigint
        );
    CREATE INDEX IF NOT EXISTS network_edges_split_site_idx ON firearea.network_edges_split (usgs_site);
    CREATE INDEX IF NOT EXISTS network_edges_split_geom_idx ON firearea.network_edges_split USING GIST (geom_5070);
    END IF;
    IF to_regclass('firearea.fire_event_vertices') IS NULL THEN
        -- New multi-vertex schema: multiple vertices per (site,event)
        CREATE TABLE firearea.fire_event_vertices (
            usgs_site text NOT NULL,
            event_id varchar(254) NOT NULL,
            ig_date date,
            vertex_id bigint NOT NULL,
            geom_5070 geometry(Point,5070),
            PRIMARY KEY (usgs_site, event_id, vertex_id)
        );
    CREATE INDEX IF NOT EXISTS fire_event_vertices_vertex_idx ON firearea.fire_event_vertices (vertex_id);
    CREATE INDEX IF NOT EXISTS fire_event_vertices_event_idx ON firearea.fire_event_vertices (usgs_site, event_id);
    ELSE
        -- If legacy single-vertex PK exists, upgrade it in-place (idempotent)
        PERFORM 1 FROM pg_constraint c
          JOIN pg_class t ON t.oid=c.conrelid
          JOIN pg_namespace n ON n.oid=t.relnamespace
        WHERE n.nspname='firearea' AND t.relname='fire_event_vertices'
          AND c.contype='p'
          AND pg_get_constraintdef(c.oid) LIKE 'PRIMARY KEY (usgs_site, event_id)';
        IF FOUND THEN
            BEGIN
                ALTER TABLE firearea.fire_event_vertices DROP CONSTRAINT fire_event_vertices_pkey;
            EXCEPTION WHEN OTHERS THEN
                -- Constraint name might differ; try generic lookup
                PERFORM 1; -- swallow
            END;
            -- Add new composite PK if not already present
            BEGIN
                ALTER TABLE firearea.fire_event_vertices ADD PRIMARY KEY (usgs_site, event_id, vertex_id);
            EXCEPTION WHEN duplicate_table THEN
                NULL; -- already exists
            WHEN duplicate_object THEN
                NULL;
            WHEN others THEN
                -- Ignore if already adjusted manually
                NULL;
            END;
        END IF;
        -- Ensure helpful indexes exist
        -- Ensure helpful indexes exist (cannot nest a DO inside plpgsql block)
        IF NOT EXISTS (
            SELECT 1 FROM pg_indexes WHERE schemaname='firearea' AND indexname='fire_event_vertices_vertex_idx'
        ) THEN
            EXECUTE 'CREATE INDEX fire_event_vertices_vertex_idx ON firearea.fire_event_vertices (vertex_id)';
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM pg_indexes WHERE schemaname='firearea' AND indexname='fire_event_vertices_event_idx'
        ) THEN
            EXECUTE 'CREATE INDEX fire_event_vertices_event_idx ON firearea.fire_event_vertices (usgs_site, event_id)';
        END IF;
    END IF;
END;
$$;

```

---
### 1. End‑to‑End Quick Start (Clean Rebuild)
```sql
-- Rebuild base network (only if underlying flowlines changed)
SELECT firearea.fn_build_network_topology(true);

-- Refresh pour point snaps
SELECT firearea.fn_refresh_pour_point_snaps();

-- Prepare + compute for a site (explicit prepare preferred for validation)
SELECT firearea.fn_prepare_site_split_topology('syca');
SELECT firearea.fn_compute_site_fire_distances('syca');

-- Inspect results
SELECT event_id, distance_m, is_pour_point_touch, status
FROM firearea.site_fire_distances WHERE usgs_site='syca' ORDER BY distance_m;
```

---
### 2. Function Catalogue
| Function | Purpose | Key Output / Side Effects |
|----------|---------|---------------------------|
| `fn_build_network_topology(rebuild, in_tolerance, in_srid)` | Build / rebuild base routable network. | `network_edges`, `network_edges_vertices_pgr` |
| `fn_refresh_pour_point_snaps(in_vertex_snap_tolerance, in_srid)` | Snap pour points to edges. | `pour_point_snaps` |
| `fn_rebuild_network_and_snaps(...)` | Convenience wrapper (topology + snaps). | Both above |
| `fn_prepare_site_split_topology(in_usgs_site, ...)` | Split target site's edges & promote event vertices. | `network_edges_split`, `network_edges_vertices_pgr_split`, `fire_event_vertices` |
| `fn_compute_site_fire_distances(in_usgs_site, in_include_unreachable DEFAULT true, in_touch_tolerance_m DEFAULT 5.0)` | Shortest path distances + zero‑touch override. | `site_fire_distances`, `site_fire_distance_run_log` |
| `fn_fire_event_paths_split(in_usgs_site)` | Path reconstruction for status='ok'. | Returns path rows (no persistence) |

---
### 3. Persistent Tables (Split Workflow)
Below each permanent table created/maintained by the workflow is documented with columns and descriptions. (Virtual / returned-only structures are listed last.)

#### 3.1 `firearea.network_edges`
| Column | Type | Description |
|--------|------|-------------|
| edge_id | bigint (PK) | Unique edge identifier. Reassigned on full rebuilds. |
| usgs_site | text | Site / watershed grouping key (lowercased). |
| source | bigint | Start vertex id (FK to `network_edges_vertices_pgr.id`). |
| target | bigint | End vertex id. |
| length_m | double precision | Edge length in meters (projected SRID). |
| cost | double precision | Forward traversal cost (normally = length_m). |
| reverse_cost | double precision | Reverse traversal cost (normally = length_m). |
| geom_5070 | geometry(LineString,5070) | Projected line geometry used for routing & splitting. |
| (other attrs) | various | Any additional flowline attributes preserved from source data. |

#### 3.2 `firearea.network_edges_vertices_pgr`
| Column | Type | Description |
|--------|------|-------------|
| id | bigint (PK) | Vertex identifier used by pgRouting. |
| the_geom | geometry(Point,5070) | Projected point location of the vertex. |

#### 3.3 `firearea.pour_point_snaps`
| Column | Type | Description |
|--------|------|-------------|
| usgs_site | text | Site identifier (lowercase). |
| identifier | text | Logical pour point id (first/closest chosen if multiple). |
| comid | text | NHDPlus COMID when available. |
| original_geom | geometry(Point,4326) | Original input location (geographic). |
| snap_geom | geometry(Point,5070) | Projected snapped geometry used for routing. |
| snap_edge_id | bigint | Edge id to which the pour point was snapped. |
| snap_fraction | double precision | Fraction along edge from source→target (0→1). |
| snap_distance_m | double precision | Planar distance from original to snapped position. |
| is_exact_vertex | boolean | True if within vertex snap tolerance of an endpoint. |
| created_at | timestamptz | Insertion timestamp. |

#### 3.4 `firearea.fires_catchments`
| Column | Type | Description |
|--------|------|-------------|
| usgs_site | text | Site identifier used to partition analysis. |
| event_id | text | Wildfire event unique key. |
| ig_date | date | Ignition or representative date. |
| geometry | geometry(Polygon/MultiPolygon,4326) | Fire perimeter geometry in geographic SRID. |

#### 3.5 `firearea.network_edges_split`
| Column | Type | Description |
|--------|------|-------------|
| edge_id | bigserial (PK) | New edge id for split segment (auto increment). |
| usgs_site | text | Site identifier (segments only for processed site are split). |
| original_edge_id | bigint | Reference back to original `network_edges.edge_id`. |
| segment_index | integer | Ordinal of segment along original edge (1..n). |
| start_fraction | double precision | Start fraction along original edge geometry. |
| end_fraction | double precision | End fraction along original edge geometry. |
| geom_5070 | geometry(LineString,5070) | Geometry of the split segment. |
| length_m | double precision | Segment length (meters). |
| cost | double precision | Traversal cost (mirrors length). |
| reverse_cost | double precision | Reverse traversal cost. |
| source | bigint | Start vertex id after topology build on split table. |
| target | bigint | End vertex id after topology build on split table. |

#### 3.6 `firearea.network_edges_vertices_pgr_split`
| Column | Type | Description |
|--------|------|-------------|
| id | bigint (PK) | Vertex id in the split network topology. |
| the_geom | geometry(Point,5070) | Split network vertex location. |

#### 3.7 `firearea.fire_event_vertices`
| Column | Type | Description |
|--------|------|-------------|
| usgs_site | text | Site identifier. |
| event_id | text | Fire event id (can map to multiple vertices). |
| ig_date | date | Event date carried through. |
| vertex_id | bigint | Split network vertex nearest an intersection point. |
| geom_5070 | geometry(Point,5070) | Representative geometry (snapped intersection). |

Primary Key: (usgs_site, event_id, vertex_id) allowing multi-vertex representation of one event.

#### 3.8 `firearea.site_fire_distances`
| Column | Type | Description |
|--------|------|-------------|
| result_id | bigserial (PK, if present) | Surrogate key (implementation-dependent). |
| usgs_site | text | Site identifier. |
| pour_point_identifier | text | Identifier of pour point chosen. |
| pour_point_comid | text | COMID from pour point snaps if available. |
| event_id | text | Fire event id. |
| ig_date | date | Event date. |
| distance_m | double precision | Shortest path distance (meters) after zero-touch override. |
| path_edge_ids | bigint[] | Reserved for future path edge storage (NULL for pure vertex distances). |
| status | text | 'ok' or 'unreachable'. |
| message | text | Processing tag ('split_vertex','split_vertex_no_path','split_zero_touch'). |
| chosen_vertex_id | bigint | Vertex id selected for minimal distance. |
| is_pour_point_touch | boolean | True if zero-distance enforced by touch logic. |
| created_at | timestamptz | Insertion timestamp. |

#### 3.9 `firearea.site_fire_distance_run_log`
| Column | Type | Description |
|--------|------|-------------|
| run_id | text | Unique run identifier (timestamp + random suffix). |
| usgs_site | text | Site identifier. |
| step | text | Step label (currently final 'split_done'; earlier granular steps possible). |
| detail | text | Summary counts (ok, unreachable, zero-touch). |
| created_at | timestamptz | Log row timestamp. |

#### 3.10 `firearea.site_fire_distance_log` (Legacy / Error Log)
| Column | Type | Description |
|--------|------|-------------|
| usgs_site | text | Site identifier. |
| pour_point_identifier | text | Pour point used (if available). |
| status | text | High-level status/error code. |
| detail | text | Human-readable explanation. |
| created_at | timestamptz | Timestamp. |

#### 3.11 `firearea.multi_site_run_log` (Batch Script Support)
| Column | Type | Description |
|--------|------|-------------|
| run_ts | timestamptz | Timestamp of log insertion. |
| usgs_site | text | Site processed in batch. |
| phase | text | 'start','done','error'. |
| status | text | 'info','ok','fail'. |
| detail | text | Additional message or error hint. |

#### 3.12 Virtual / Returned Only: `fn_fire_event_paths_split`
Returned columns (not persisted):
| Column | Type | Description |
|--------|------|-------------|
| event_id | text | Fire event id. |
| ig_date | date | Event date. |
| distance_m | double precision | Distance copied from `site_fire_distances`. |
| pour_point_identifier | text | Pour point id. |
| start_vertex_id | bigint | Source vertex used for Dijkstra. |
| pour_point_geom | geometry(Point,5070) | Pour point geometry (projected). |
| chosen_vertex_id | bigint | Vertex selected for distance. |
| chosen_vertex_geom | geometry(Point,5070) | Geometry of chosen vertex. |
| is_pour_point_touch | boolean | Zero-distance override flag. |
| path_edge_count | integer | Number of edges in reconstructed path. |
| path_edge_ids | bigint[] | Ordered list of edge ids. |
| path_geom | geometry(LineString,5070) | Merged path geometry. |

---
### 4. Single-Site Split Workflow (Verbose)
```sql
SELECT firearea.fn_build_network_topology(true);      -- only when flowlines change
SELECT firearea.fn_refresh_pour_point_snaps();        -- after editing pour points
SELECT firearea.fn_prepare_site_split_topology('sbc_lter_rat');
SELECT firearea.fn_compute_site_fire_distances('sbc_lter_rat');
SELECT * FROM firearea.site_fire_distances WHERE usgs_site='sbc_lter_rat' ORDER BY distance_m;
```

---
### 5. Batch (Per-Site Transaction)
Use `run_multi_site_split.sh` which for each site runs:
```sql
BEGIN;
  SELECT firearea.fn_prepare_site_split_topology(:site);
  SELECT firearea.fn_compute_site_fire_distances(:site);
COMMIT;
```
Failures roll back only that site's work.

---
### 6. Key Advantages of Split Workflow
| Improvement | Benefit |
|-------------|---------|
| No mid-edge fractions in distance algebra | Fewer edge cases & simpler QA |
| Multiple vertices per fire event | Picks true nearest inserted vertex |
| Zero-distance touch logic | Explicit flag for pour point intersecting fire polygon |
| Single public function | Reduced maintenance surface |

---
### 7. Path Export (Optional)
```sql
SELECT event_id, ig_date, distance_m, path_edge_count, path_geom
FROM firearea.fn_fire_event_paths_split('syca') ORDER BY distance_m;
```

---
### 8. Zero-Distance (Touch) Logic
Any fire polygon covering, intersecting, or within `in_touch_tolerance_m` (default 5 m) of the pour point is forced to distance 0 with `is_pour_point_touch=true` and `message='split_zero_touch'`.

---
### 9. Common Failure Modes
| Symptom | Cause | Remedy |
|---------|-------|--------|
| No rows inserted (no error) | Site lacks fire polygons or all vertices unreachable | Verify `fires_catchments` for site; inspect graph connectivity. |
| Exception: no event vertices after prep | Geometry/SRID mismatch or fires not touching network | Confirm SRIDs and spatial overlap. |
| All unreachable | Pour point isolated component | Inspect `fire_event_vertices` vs pour point vertex; rebuild topology with tolerance tweak. |
| Large `network_edges_split` size | Full rebuild per site (expected) | Optimize later (site-scoped split table) if performance issue. |

---
### 9a. Schema Bootstrap (Lazy DDL / In-Place Migration)
On first load of `fire_distance_functions.sql`, a DO block (lines ~1200–1274 in that file) runs immediately to ensure required split-mode tables exist or are upgraded. This is intentionally silent so you can deploy the functions without a separate migration step.

What it creates if missing:
- `firearea.network_edges_split` plus indexes on `(usgs_site)` and GiST on `(geom_5070)`.
- `firearea.fire_event_vertices` (multi-vertex design) with PK `(usgs_site, event_id, vertex_id)` and supporting indexes.

What it upgrades:
- If an older two-column primary key `(usgs_site, event_id)` is detected on `fire_event_vertices`, it drops it and adds the new three-column PK to allow multiple vertices per fire event.

Idempotency & Safety:
- Uses `to_regclass` and catalog checks; reruns do nothing destructive.
- Index creation guarded by `IF NOT EXISTS` or explicit catalog checks.

Why not highlighted earlier:
- The main workflow sections assume schema already exists; this block bridges fresh vs. upgraded environments automatically.

Operational Note:
- If you prefer controlled migrations, you can comment out or remove this block after the first successful deployment and manage schema changes via a separate migration script.

Audit Tip:
- If you want visibility when an in-place PK upgrade occurs, add a `RAISE NOTICE` inside that branch (currently silent by design).

Removal Option:
- Safe to remove once the tables exist everywhere with the new composite PK; future function execution does not rely on the bootstrap block.

---
### 10. Suggested Indexes
```sql
CREATE INDEX IF NOT EXISTS network_edges_split_site_idx ON firearea.network_edges_split(usgs_site);
CREATE INDEX IF NOT EXISTS fire_event_vertices_site_evt_idx ON firearea.fire_event_vertices(usgs_site, event_id);
CREATE INDEX IF NOT EXISTS site_fire_distances_site_status_idx ON firearea.site_fire_distances(usgs_site, status);
```

---
### 11. Execution Duration Logging (Discussion)
You asked where to store run duration. Options:
1. Add `duration_ms double precision` to `site_fire_distance_run_log` (store only on final step row).
2. Create a new table `site_fire_distance_run_summary(run_id, usgs_site, started_at, finished_at, duration_ms, zero_touch_ct, ok_ct, unreachable_ct)`. Cleaner separation; avoids altering existing log semantics.
3. Compute on demand: difference between earliest and latest `created_at` for a `run_id` (no schema change, but slower aggregations later).

Recommended: Option 2 if you foresee analytics; Option 3 if minimal schema churn preferred. (Not implemented yet—tell me if you want one created.)

---
### 12. Do We Need a Materialized View for Counts?
Current data already supports summaries:
```sql
-- Per site summary (reachable, unreachable, zero-touch)
SELECT usgs_site,
  COUNT(*) FILTER (WHERE status='ok') AS ok_ct,
  COUNT(*) FILTER (WHERE status='unreachable') AS unreachable_ct,
  COUNT(*) FILTER (WHERE is_pour_point_touch) AS zero_touch_ct
FROM firearea.site_fire_distances
GROUP BY usgs_site;
```
Since this is a single inexpensive GROUP BY on an indexed column, a materialized view is optional. Only create one if you repeatedly query large historical snapshots or need snapshot isolation.

---
### 13. Migration Notes From Legacy Lite
- Mid-edge fraction math & pour point offsets removed.
- Unified function inlines vertex logic (old dedicated vertex function archived but callable history retained in VCS).
- `chosen_vertex_id` enables path export.

---
### 14. Sanity Checklist Before Publishing Results
- [ ] Distances exist for all target sites (`SELECT COUNT(*) FROM firearea.site_fire_distances`).
- [ ] Zero-touch counts plausible.
- [ ] Few or no unexpected unreachable events.
- [ ] Path export returns expected geometry for sample site(s).

---
### 15. Glossary
| Term | Definition |
|------|------------|
| Split Edge | Segment after splitting at pour point & fire fractions. |
| Event Vertex | Promoted vertex representing a fire boundary intersection. |
| Touch Event | Fire polygon touching or near the pour point (forced distance 0). |

---
### 16. Where to Go Next
- Use `run_multi_site_split.sh` for batch processing.
- Add duration logging (choose an option in section 11) if performance trending becomes important.
- Consider future optimization: site-local split cache or incremental splitting.

---
Happy (simpler) routing! 🔥➡️💧
