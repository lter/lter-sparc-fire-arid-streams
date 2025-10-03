-- fire_distance_functions.sql
-- Purpose: Contains stable tables (if not already created elsewhere) and a lean
-- function to compute along-network distances from a site's pour point to
-- each fire event using pre-built network topology & pgr_withPoints.
--
-- Assumptions:
--   * Table firearea.network_edges already exists with columns:
--       edge_id (PK), usgs_site, nhdplus_comid, geom_5070 (LineString,5070),
--       cost, reverse_cost, source, target
--   * Table firearea.pour_point_snaps exists with pour point snap info
--   * Fire polygons in firearea.fires_catchments (event_id, ig_date, usgs_site, geometry(SRID 4326))
--   * Extensions postgis & pgrouting installed
--
-- This script is idempotent (CREATE TABLE IF NOT EXISTS / OR REPLACE FUNCTION).
-- Modify SRIDs/tolerances as needed.

-- (0) Extensions
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pgrouting;


-- 0a. Unified catchments and pour points (USGS + non-USGS)

DROP VIEW IF EXISTS firearea.catchments_all CASCADE;
CREATE VIEW firearea.catchments_all AS
SELECT lower(usgs_site) AS usgs_site, geometry FROM firearea.catchments
UNION ALL
SELECT lower(usgs_site) AS usgs_site, geometry FROM firearea.non_usgs_catchments;

-- Minimal unified pour points view (only site id & geometry). Metadata for USGS
-- pour points is still obtained directly from firearea.pour_points during snapping.
DROP VIEW IF EXISTS firearea.pour_points_all CASCADE;
CREATE VIEW firearea.pour_points_all AS
SELECT lower(usgs_site) AS usgs_site, geometry FROM firearea.pour_points
UNION ALL
SELECT lower(usgs_site) AS usgs_site, geometry FROM firearea.non_usgs_pour_points;

--------------------------------------------------------------------------------
-- 1. Results & Log Tables
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS firearea.site_fire_distances (
    result_id bigserial PRIMARY KEY,
    usgs_site text NOT NULL,
    pour_point_identifier text,
    pour_point_comid text,
    event_id varchar(254) NOT NULL,
    ig_date date,
    distance_m double precision,
    path_edge_ids bigint[],
    computed_at timestamptz DEFAULT now(),
    status text NOT NULL,         -- ok | no_network | no_pour_point | no_fire | unreachable | error
    message text
);

CREATE INDEX IF NOT EXISTS site_fire_distances_site_idx ON firearea.site_fire_distances (usgs_site);
CREATE INDEX IF NOT EXISTS site_fire_distances_event_idx ON firearea.site_fire_distances (event_id);
CREATE INDEX IF NOT EXISTS site_fire_distances_status_idx ON firearea.site_fire_distances (status);

-- Flag column to indicate pour point is inside or touches (within tolerance) a fire polygon.
ALTER TABLE firearea.site_fire_distances ADD COLUMN IF NOT EXISTS is_pour_point_touch boolean;
-- Chosen network vertex (where applicable) that yielded the minimal distance (split or lite endpoint)
ALTER TABLE firearea.site_fire_distances ADD COLUMN IF NOT EXISTS chosen_vertex_id bigint;

CREATE TABLE IF NOT EXISTS firearea.site_fire_distance_log (
    log_id bigserial PRIMARY KEY,
    usgs_site text,
    pour_point_identifier text,
    event_id varchar(254),
    log_ts timestamptz DEFAULT now(),
    status text,
    detail text
);

-- Detailed step logging per function invocation (optional diagnostic)
CREATE TABLE IF NOT EXISTS firearea.site_fire_distance_run_log (
    run_id text,
    usgs_site text,
    step text,
    detail text,
    created_at timestamptz DEFAULT now()
);

--------------------------------------------------------------------------------
-- 1B. Network Topology & Pour Point Snap Preparation Utilities
--     These functions let you (re)build the reusable network topology and
--     refresh snapped pour points outside of the per-site distance function.
--     Run them when flowlines or pour_points change.
--------------------------------------------------------------------------------

-- Function: build or rebuild network topology from firearea.flowlines
-- Parameters:
--   in_rebuild    : if true forces drop & full rebuild even if table exists
--   in_tolerance  : snapping tolerance for pgr_createTopology (meters in projected SRID)
--   in_srid       : target projection SRID for metric lengths (default 5070 - Albers)
CREATE OR REPLACE FUNCTION firearea.fn_build_network_topology(
    in_rebuild boolean DEFAULT false,
    in_tolerance double precision DEFAULT 0.5,
    in_srid integer DEFAULT 5070
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_exists boolean;
BEGIN
    SELECT to_regclass('firearea.network_edges') IS NOT NULL INTO v_exists;

    IF in_rebuild OR NOT v_exists THEN
        RAISE NOTICE 'Building network_edges (rebuild=%, existed=%)', in_rebuild, v_exists;
        -- Drop dependent topology table if present
        PERFORM 1 FROM pg_class WHERE relname = 'network_edges_vertices_pgr' AND relnamespace = 'firearea'::regnamespace;
        IF FOUND THEN
            EXECUTE 'DROP TABLE IF EXISTS firearea.network_edges_vertices_pgr CASCADE';
        END IF;
        EXECUTE 'DROP TABLE IF EXISTS firearea.network_edges CASCADE';

        -- Create base edges ensuring single-part LineStrings
        EXECUTE format($sql$
            CREATE TABLE firearea.network_edges AS
            WITH base AS (
          SELECT lower(f.usgs_site) AS usgs_site,
                       f.nhdplus_comid,
                       (ST_Dump(ST_LineMerge(f.geometry))).geom::geometry(LineString,4326) AS geom_4326
                FROM firearea.flowlines f
                WHERE f.geometry IS NOT NULL
            )
            SELECT row_number() OVER ()::bigint AS edge_id,
                   usgs_site,
                   nhdplus_comid,
                   geom_4326
            FROM base
            WHERE NOT ST_IsEmpty(geom_4326);
        $sql$);

        EXECUTE 'ALTER TABLE firearea.network_edges ADD CONSTRAINT network_edges_pkey PRIMARY KEY (edge_id)';
        -- Add projected geom & length
        EXECUTE format('ALTER TABLE firearea.network_edges ADD COLUMN geom_%s geometry(LineString,%s), ADD COLUMN length_m double precision', in_srid, in_srid);
        EXECUTE format('UPDATE firearea.network_edges SET geom_%1$s = ST_Transform(geom_4326,%1$s), length_m = ST_Length(ST_Transform(geom_4326,%1$s))', in_srid);
        EXECUTE 'DELETE FROM firearea.network_edges WHERE length_m IS NULL OR length_m = 0';

        -- Cost columns
        EXECUTE 'ALTER TABLE firearea.network_edges ADD COLUMN cost double precision, ADD COLUMN reverse_cost double precision';
        EXECUTE 'UPDATE firearea.network_edges SET cost = length_m, reverse_cost = length_m';

        -- Source / target columns for topology
        EXECUTE 'ALTER TABLE firearea.network_edges ADD COLUMN source bigint, ADD COLUMN target bigint';

        -- Indexes (geom index created after projection populated)
        EXECUTE 'CREATE INDEX network_edges_site_idx ON firearea.network_edges (usgs_site)';
        EXECUTE format('CREATE INDEX network_edges_geom_idx ON firearea.network_edges USING GIST (geom_%s)', in_srid);

        -- pgr_createTopology (explicit signature). Using dynamic geom column name.
        PERFORM pgr_createTopology(
            'firearea.network_edges',
            in_tolerance,
            format('geom_%s', in_srid),
            'edge_id',
            'source',
            'target',
            rows_where := 'true',
            clean := false
        );

        -- Post-topology indexes
        EXECUTE 'CREATE INDEX network_edges_source_idx ON firearea.network_edges (source)';
        EXECUTE 'CREATE INDEX network_edges_target_idx ON firearea.network_edges (target)';

        -- Analyze graph (optional; catches dangling edges)
        PERFORM pgr_analyzeGraph(
            'firearea.network_edges',
            in_tolerance,
            format('geom_%s', in_srid),
            'edge_id',
            'source',
            'target'
        );

        EXECUTE 'ANALYZE firearea.network_edges';
    ELSE
        RAISE NOTICE 'network_edges exists and rebuild not requested; skipping rebuild. Running analyzeGraph only.';
        PERFORM pgr_analyzeGraph(
            'firearea.network_edges',
            in_tolerance,
            -- Assume existing column geom_5070 or fallback to geom_4326 if not found
            CASE WHEN EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='firearea' AND table_name='network_edges' AND column_name='geom_5070')
                 THEN 'geom_5070'
                 ELSE 'geom_4326' END,
            'edge_id','source','target'
        );
    END IF;
END;
$$;

--------------------------------------------------------------------------------
-- TABLE / REFRESH FUNCTION: pour_point_snaps & fn_refresh_pour_point_snaps
-- Purpose
--   Materialized snapshot of all pour points (USGS + non‑USGS) snapped to the
--   hydrologic network stored in firearea.network_edges. The table is fully
--   (re)generated by fn_refresh_pour_point_snaps and is treated as ephemeral
--   derived data: you can safely TRUNCATE and rebuild at any time.
--
-- Creation & Refresh Strategy
--   * If the table does not exist it is created.
--   * If it exists, it is TRUNCATED then repopulated (no incremental logic).
--   * The snapping chooses, per input point, the single nearest edge (within
--     the same usgs_site subgraph) using a site‑filtered KNN (ORDER BY edge_geom <-> point).
--   * A point is labeled is_exact_vertex = true when it lies within the
--     in_vertex_snap_tolerance distance of either endpoint (source/target) of
--     the selected edge (avoids an extra global vertex KNN query for speed).
--
-- Column Definitions
--   usgs_site        Lower‑cased site identifier (partition key / filter key).
--   identifier       Stable identifier for the pour point. If original metadata
--                    record (firearea.pour_points) supplies one it is reused;
--                    otherwise synthesized as <usgs_site>_idx_<row_number>.
--   comid            Optional NHDPlus COMID carried from original pour_points.
--   sourceName       Metadata passthrough from original pour_points (if any).
--   reachcode        NHD reach code (if present in source data).
--   measure          Linear referencing measure from source (if present).
--   original_geom    Original input geometry (Point, SRID 4326) before snapping.
--   snap_geom        Projected point geometry in target metric SRID (default 5070).
--   snap_edge_id     edge_id in firearea.network_edges to which the point was snapped.
--   snap_fraction    Fractional position along the snapped edge in [0,1] computed via
--                    ST_LineLocatePoint(edge_geom, snap_geom). 0 = source vertex, 1 = target vertex.
--   snap_distance_m  Planar distance (meters in projected SRID) between snap_geom and the
--                    nearest point on the chosen edge (a diagnostic quality metric).
--   is_exact_vertex  Boolean flag true if snap_geom lies within tolerance of an edge endpoint.
--   created_at       Timestamp of insertion during the refresh operation.
--
-- Constraints & Indexes
--   * PRIMARY KEY (usgs_site, identifier) so multiple pour points per site are
--     permitted, but the current downstream "lite" distance function assumes
--     effectively one logical pour point per site (it selects the closest by
--     snap_distance_m if multiple exist).
--   * Index on (usgs_site) added for fast per‑site filtering.
--   * Consider adding a UNIQUE(usgs_site) constraint once the data model guarantees
--     a single pour point per site (remove identifier from PK in that case).
--
-- Performance Notes
--   * The refresh avoids per‑row global vertex nearest searches (uses edge endpoints).
--   * All ST_Transform operations are hoisted into the CTE so each point is projected once.
--   * If snapping becomes a bottleneck, you can (a) simplify network edge geometry for
--     snapping only, or (b) add a spatial index on a simplified geom column, or (c)
--     parameterize the refresh to process a subset of sites.
--
-- Quality / Diagnostics
--   * Large snap_distance_m values may indicate a mismatch in SRID or that the network
--     does not actually pass near the supplied pour point (data QA issue).
--   * A high fraction of is_exact_vertex = false may be acceptable; it simply means
--     points lie interior to edges rather than exactly on vertices.
--
-- Typical Queries
--   * Inspect snap statistics per site:
--       SELECT usgs_site, count(*) AS n, avg(snap_distance_m) AS avg_d, max(snap_distance_m) AS max_d
--       FROM firearea.pour_point_snaps GROUP BY usgs_site ORDER BY max_d DESC;
--   * View fractional distribution along edges:
--       SELECT histogram_width, count(*) FROM (
--           SELECT width_bucket(snap_fraction,0,1,10) AS histogram_width FROM firearea.pour_point_snaps
--       ) s GROUP BY histogram_width ORDER BY histogram_width;
--
-- When to Rebuild
--   * After updating firearea.flowlines (requires rebuilding topology first).
--   * After adding/modifying raw pour points (firearea.pour_points or non_usgs_pour_points).
--
-- Potential Future Enhancements
--   * Incremental refresh (detect and only reprocess changed points).
--   * Store nearest vertex id directly to skip search in downstream functions.
--   * Add geometry validity checks and automatic clipping to a study boundary.
--
-- Returns: The refresh function returns the number of rows inserted.
--------------------------------------------------------------------------------
-- Function: refresh / (re)populate pour_point_snaps from firearea.pour_points & existing network
-- Returns number of rows inserted.
CREATE OR REPLACE FUNCTION firearea.fn_refresh_pour_point_snaps(
    in_vertex_snap_tolerance double precision DEFAULT 0.2,
    in_srid integer DEFAULT 5070
) RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    v_cnt integer;
    v_geom_col text;
    v_existing_srid integer;
    v_create_sql text;
    -- identifier synthesis now handled inline in INSERT CTE
BEGIN
    -- Determine projected geom column name
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='firearea' AND table_name='network_edges' AND column_name = format('geom_%s', in_srid)) THEN
        v_geom_col := format('geom_%s', in_srid);
    ELSE
        RAISE EXCEPTION 'Projected geometry column geom_% not found in firearea.network_edges', in_srid;
    END IF;

    -- Ensure network exists
    IF NOT EXISTS (SELECT 1 FROM firearea.network_edges) THEN
        RAISE EXCEPTION 'network_edges empty; build topology first (call fn_build_network_topology)';
    END IF;

    -- Create table if not exists using dynamic SRID
    IF to_regclass('firearea.pour_point_snaps') IS NULL THEN
        v_create_sql := 'CREATE TABLE firearea.pour_point_snaps (
            usgs_site text NOT NULL,
            identifier text,
            comid text,
            sourceName text,
            reachcode text,
            measure double precision,
            original_geom geometry(Point,4326),
            snap_geom geometry(Point,' || in_srid || '),
            snap_edge_id bigint,
            snap_fraction double precision,
            snap_distance_m double precision,
            is_exact_vertex boolean,
            created_at timestamptz DEFAULT now(),
            PRIMARY KEY (usgs_site, identifier)
        )';
        EXECUTE v_create_sql;
    ELSE
        -- Validate existing SRID matches requested
        SELECT Find_SRID('firearea','pour_point_snaps','snap_geom') INTO v_existing_srid;
        IF v_existing_srid IS DISTINCT FROM in_srid THEN
            RAISE EXCEPTION 'Existing pour_point_snaps.snap_geom SRID % does not match requested %; drop table or call with matching SRID', v_existing_srid, in_srid;
        END IF;
        TRUNCATE firearea.pour_point_snaps;
    END IF;

    -- No per-row identifier expression needed outside INSERT.

    -- Dynamic SQL to reference projected geom column
    -- PERFORMANCE OPTIMIZATIONS (2025-09-29):
    --  * Remove per-row nearest vertex global KNN query against network_edges_vertices_pgr.
    --    Instead, treat a snap as an "exact vertex" if it lies within tolerance of either
    --    endpoint of the selected nearest edge. This avoids a full-table KNN for each point.
    --  * Compute ST_Transform(point) once in the base CTE (geom_proj) instead of 3–4 times.
    --  * Drop lower(usgs_site) on the network_edges filter so the plain btree index on
    --    (usgs_site) is usable (network_edges.usgs_site is already stored lowercased).
    --  * Keep fast KNN edge selection per point with ORDER BY geom <-> point restricted
    --    by site equality to prune candidates early.
    --  * Simplify identifier synthesis logic.
    EXECUTE format($ins$
        WITH base AS (
            SELECT
                ppa.usgs_site,
                ppa.geometry,
                ST_Transform(ppa.geometry,%1$s) AS geom_proj,
                up.identifier,
                up.comid,
                up."sourceName" AS sourceName,
                up.reachcode,
                up.measure,
                row_number() OVER (PARTITION BY ppa.usgs_site ORDER BY ST_X(ppa.geometry), ST_Y(ppa.geometry)) AS rn
            FROM firearea.pour_points_all ppa
            LEFT JOIN firearea.pour_points up
              ON up.usgs_site = ppa.usgs_site
             AND ST_Equals(up.geometry, ppa.geometry)
        )
        INSERT INTO firearea.pour_point_snaps(
            usgs_site, identifier, comid, sourceName, reachcode, measure,
            original_geom, snap_geom, snap_edge_id, snap_fraction, snap_distance_m, is_exact_vertex
        )
        SELECT
            b.usgs_site,
            COALESCE(b.identifier, concat(b.usgs_site,'_idx_', b.rn)) AS identifier,
            b.comid,
            b.sourceName,
            b.reachcode,
            b.measure,
            b.geometry AS original_geom,
            b.geom_proj AS snap_geom,
            ne.edge_id AS snap_edge_id,
            ST_LineLocatePoint(ne.%2$I, b.geom_proj) AS snap_fraction,
            ST_Distance(ne.%2$I, b.geom_proj) AS snap_distance_m,
            (LEAST(
                ST_Distance(b.geom_proj, ST_StartPoint(ne.%2$I)),
                ST_Distance(b.geom_proj, ST_EndPoint(ne.%2$I))
            ) < %3$s) AS is_exact_vertex
        FROM base b
        JOIN LATERAL (
            SELECT edge_id, %2$I
            FROM firearea.network_edges
            WHERE usgs_site = b.usgs_site
            ORDER BY %2$I <-> b.geom_proj
            LIMIT 1
        ) ne ON TRUE
    $ins$, in_srid, v_geom_col, in_vertex_snap_tolerance);

    GET DIAGNOSTICS v_cnt = ROW_COUNT;

    CREATE INDEX IF NOT EXISTS pour_point_snaps_site_idx ON firearea.pour_point_snaps (usgs_site);
    ANALYZE firearea.pour_point_snaps;
    RETURN v_cnt;
END;
$$;

-- Convenience wrapper: rebuild everything (network + snaps) in one call
CREATE OR REPLACE FUNCTION firearea.fn_rebuild_network_and_snaps(
    in_network_tolerance double precision DEFAULT 0.5,
    in_vertex_snap_tolerance double precision DEFAULT 0.2,
    in_srid integer DEFAULT 5070
) RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM firearea.fn_build_network_topology(true, in_network_tolerance, in_srid);
    PERFORM firearea.fn_refresh_pour_point_snaps(in_vertex_snap_tolerance, in_srid);
END;
$$;


--------------------------------------------------------------------------------
-- LITE FUNCTION: fn_compute_site_fire_distances_lite
-- Purpose: Minimal distance computation using only vertex-based logic.
-- Features intentionally OMITTED (per user request):
--   * Upstream-only traversal enforcement
--   * Path edge list reconstruction
--   * Extra metadata columns
--   * pgr_withPoints usage (avoids its environmental issues)
-- Method Summary:
--   1. Choose pour point (specified identifier or closest snap)
--   2. Build fire event intersection points (edge_id + fraction) with
--      containment fallback (midpoint) where no boundary intersection.
--   3. For each event point, compute distance as min( SP(start_vertex -> edge.source) + edge.length * fraction,
--                                                   SP(start_vertex -> edge.target) + edge.length * (1 - fraction) )
--   4. Add pour point offset from start vertex along pour point edge (if pour point not exactly a vertex)
--   5. Keep minimal distance per event; insert into site_fire_distances with path_edge_ids NULL.
-- Unreachable events get status 'unreachable'.
-- Notes:
--   * Assumes firearea.network_edges has topology (source/target, length_m) & geom_5070
--   * Assumes firearea.pour_point_snaps already populated.
--   * Idempotent per (site, pour_point_identifier): previous rows for that pair are deleted first.
--------------------------------------------------------------------------------
-- (Revised) Lite function now assumes exactly one pour point per site; identifier parameter removed.
DROP FUNCTION IF EXISTS firearea.fn_compute_site_fire_distances_lite(text, text, double precision);
DROP FUNCTION IF EXISTS firearea.fn_compute_site_fire_distances_lite(text, text, double precision, boolean);
DROP FUNCTION IF EXISTS firearea.fn_compute_site_fire_distances_lite(text, double precision, boolean);
CREATE OR REPLACE FUNCTION firearea.fn_compute_site_fire_distances_lite(
    in_usgs_site text,
    in_snap_tolerance_m double precision DEFAULT 60.0,
    in_include_unreachable boolean DEFAULT true  -- if false, skip inserting unreachable events
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_site_lower text := lower(in_usgs_site);
    v_run_id text := to_char(clock_timestamp(),'YYYYMMDDHH24MISSMS') || '_' || lpad((floor(random()*1000000))::int::text,6,'0');
    v_pp_identifier text;
    v_pp_comid text;
    v_pp_edge_id bigint;
    v_pp_fraction double precision;
    v_pp_is_vertex boolean;
    v_pp_snap_distance double precision;
    v_start_vertex_id bigint;
    v_offset_m double precision := 0;  -- distance along pour point edge from chosen start vertex to actual pour point
    v_edge_len double precision;
    v_edge_source bigint;
    v_edge_target bigint;
    -- Diagnostics
    v_cnt_event_points int;
    v_cnt_event_edges int;
    v_cnt_vertices_needed int;
    v_cnt_sp_rows int;
    v_cnt_event_min int;
    v_component_pourpoint int;
    v_cnt_same_component int;
    v_cnt_other_component int;
BEGIN
    -- Log start
    INSERT INTO firearea.site_fire_distance_run_log(run_id, usgs_site, step, detail)
    VALUES (v_run_id, v_site_lower, 'start_lite', 'invoke fn_compute_site_fire_distances_lite');

    -- Preconditions ---------------------------------------------------------
    IF NOT EXISTS (SELECT 1 FROM firearea.network_edges WHERE lower(usgs_site)=v_site_lower) THEN
        INSERT INTO firearea.site_fire_distance_log(usgs_site,status,detail)
        VALUES (v_site_lower,'no_network','No network edges for site');
        INSERT INTO firearea.site_fire_distance_run_log(run_id, usgs_site, step, detail)
        VALUES (v_run_id, v_site_lower, 'abort', 'no_network');
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM firearea.fires_catchments WHERE lower(usgs_site)=v_site_lower) THEN
        INSERT INTO firearea.site_fire_distance_log(usgs_site,status,detail)
        VALUES (v_site_lower,'no_fire','No fires for site');
        INSERT INTO firearea.site_fire_distance_run_log(run_id, usgs_site, step, detail)
        VALUES (v_run_id, v_site_lower, 'abort', 'no_fire');
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM firearea.pour_point_snaps WHERE lower(usgs_site)=v_site_lower) THEN
        INSERT INTO firearea.site_fire_distance_log(usgs_site,status,detail)
        VALUES (v_site_lower,'no_pour_point','No snapped pour points for site');
        INSERT INTO firearea.site_fire_distance_run_log(run_id, usgs_site, step, detail)
        VALUES (v_run_id, v_site_lower, 'abort', 'no_pour_point');
        RETURN;
    END IF;

    -- Select pour point (only one expected per site; pick closest defensively) ----
    SELECT identifier, comid, snap_edge_id, snap_fraction, is_exact_vertex, snap_distance_m
    INTO v_pp_identifier, v_pp_comid, v_pp_edge_id, v_pp_fraction, v_pp_is_vertex, v_pp_snap_distance
    FROM firearea.pour_point_snaps
    WHERE lower(usgs_site)=v_site_lower
    ORDER BY snap_distance_m, identifier
    LIMIT 1;

    IF v_pp_identifier IS NULL THEN
        INSERT INTO firearea.site_fire_distance_log(usgs_site,status,detail)
        VALUES (v_site_lower,'no_pour_point','Specified pour point not found');
        INSERT INTO firearea.site_fire_distance_run_log(run_id, usgs_site, step, detail)
        VALUES (v_run_id, v_site_lower, 'abort', 'pour_point_not_found');
        RETURN;
    END IF;
    IF v_pp_snap_distance IS NULL OR v_pp_snap_distance > in_snap_tolerance_m THEN
        INSERT INTO firearea.site_fire_distance_log(usgs_site,pour_point_identifier,status,detail)
        VALUES (
            v_site_lower,
            v_pp_identifier,
            'no_network',
            format(
                'Snap distance %s exceeds tolerance %s',
                to_char(COALESCE(v_pp_snap_distance, -1), 'FM9999990.00'),
                to_char(in_snap_tolerance_m, 'FM9999990.00')
            )
        );
        INSERT INTO firearea.site_fire_distance_run_log(run_id, usgs_site, step, detail)
        VALUES (v_run_id, v_site_lower, 'abort', 'snap_distance_exceeds_tolerance');
        RETURN;
    END IF;

    -- Identify nearest vertex to pour point (site subgraph only) ------------
    SELECT v.id
    INTO v_start_vertex_id
    FROM firearea.network_edges_vertices_pgr v
    JOIN firearea.network_edges e ON (e.source=v.id OR e.target=v.id)
    WHERE lower(e.usgs_site)=v_site_lower
    ORDER BY v.the_geom <-> (
        SELECT snap_geom FROM firearea.pour_point_snaps
        WHERE lower(usgs_site)=v_site_lower AND identifier=v_pp_identifier
        LIMIT 1
    )
    LIMIT 1;

    IF v_start_vertex_id IS NULL THEN
        INSERT INTO firearea.site_fire_distance_log(usgs_site,pour_point_identifier,status,detail)
        VALUES (v_site_lower,v_pp_identifier,'error','Could not determine start vertex');
        INSERT INTO firearea.site_fire_distance_run_log(run_id, usgs_site, step, detail)
        VALUES (v_run_id, v_site_lower, 'abort', 'no_start_vertex');
        RETURN;
    END IF;

    RAISE NOTICE '[lite] selected pour_point identifier=% edge_id=% fraction=% is_vertex=% snap_distance_m=% start_vertex_candidate=%',
        v_pp_identifier, v_pp_edge_id, ROUND(v_pp_fraction::numeric,4), v_pp_is_vertex, ROUND(COALESCE(v_pp_snap_distance, -1)::numeric,2), v_start_vertex_id;
    -- Replace C-style numeric format specifiers with preformatted strings
    INSERT INTO firearea.site_fire_distance_run_log(run_id, usgs_site, step, detail)
    VALUES (
        v_run_id,
        v_site_lower,
        'pour_point_selected_lite',
        format(
            'id=%s edge=%s fraction=%s dist=%s start_vid=%s',
            v_pp_identifier,
            v_pp_edge_id,
            to_char(v_pp_fraction, 'FM0.0000'),
            to_char(COALESCE(v_pp_snap_distance,-1), 'FM9999990.00'),
            v_start_vertex_id
        )
    );

    -- Pour point offset along its edge -------------------------------------
    SELECT source, target, length_m INTO v_edge_source, v_edge_target, v_edge_len
    FROM firearea.network_edges WHERE edge_id = v_pp_edge_id;
    IF v_edge_len IS NOT NULL AND NOT v_pp_is_vertex THEN
        IF v_start_vertex_id = v_edge_source THEN
            v_offset_m := v_edge_len * v_pp_fraction; -- from source to pour point
        ELSIF v_start_vertex_id = v_edge_target THEN
            v_offset_m := v_edge_len * (1 - v_pp_fraction); -- from target to pour point
        ELSE
            -- Fallback: choose nearer endpoint
            v_offset_m := LEAST(v_edge_len * v_pp_fraction, v_edge_len * (1 - v_pp_fraction));
        END IF;
    END IF;

    RAISE NOTICE '[lite] pour_point_offset_m=% (edge_len=% source=% target=%)', ROUND(v_offset_m::numeric,2), ROUND(COALESCE(v_edge_len,0)::numeric,2), v_edge_source, v_edge_target;
    INSERT INTO firearea.site_fire_distance_run_log(run_id, usgs_site, step, detail)
    VALUES (
        v_run_id,
        v_site_lower,
        'pour_point_offset_lite',
        format(
            'offset_m=%s edge_len=%s',
            to_char(v_offset_m, 'FM9999990.00'),
            to_char(COALESCE(v_edge_len,0), 'FM9999990.00')
        )
    );

    -- Build event intersection points --------------------------------------
    DROP TABLE IF EXISTS firearea._lite_event_points;
    CREATE UNLOGGED TABLE firearea._lite_event_points AS
    WITH fire_poly AS (
        SELECT event_id, ig_date,
               CASE WHEN GeometryType(geometry) LIKE 'Multi%'
                        THEN ST_CollectionExtract(geometry,3)
                    ELSE geometry END AS geom
        FROM firearea.fires_catchments
        WHERE lower(usgs_site)=v_site_lower
    ), boundary_pts AS (
        SELECT f.event_id, f.ig_date, ne.edge_id,
               (ST_Dump(ST_Intersection(ST_Boundary(ST_Transform(f.geom,5070)), ne.geom_5070))).geom AS ipt,
               ne.geom_5070
        FROM fire_poly f
        JOIN firearea.network_edges ne
          ON lower(ne.usgs_site)=v_site_lower
         AND ST_Intersects(ST_Transform(f.geom,5070), ne.geom_5070)
        WHERE ST_Intersects(ST_Boundary(ST_Transform(f.geom,5070)), ne.geom_5070)
    )
    SELECT event_id, ig_date, edge_id,
           ST_LineLocatePoint(geom_5070, ipt) AS fraction
    FROM boundary_pts
    WHERE GeometryType(ipt)='POINT';

    -- Containment fallback (midpoint) for events with no boundary points
    INSERT INTO firearea._lite_event_points(event_id, ig_date, edge_id, fraction)
    SELECT f.event_id, f.ig_date, ne.edge_id, 0.5
    FROM firearea.fires_catchments f
    JOIN firearea.network_edges ne
      ON lower(ne.usgs_site)=v_site_lower AND lower(f.usgs_site)=v_site_lower
     AND ST_Contains(ST_Transform(f.geometry,5070), ne.geom_5070)
    WHERE lower(f.usgs_site)=v_site_lower
      AND f.event_id NOT IN (SELECT DISTINCT event_id FROM firearea._lite_event_points);

    -- Deduplicate same event/edge/fraction rows
    DELETE FROM firearea._lite_event_points a USING firearea._lite_event_points b
    WHERE a.ctid < b.ctid
      AND a.event_id=b.event_id AND a.edge_id=b.edge_id AND a.fraction=b.fraction;

    SELECT COUNT(*) INTO v_cnt_event_points FROM firearea._lite_event_points;
    RAISE NOTICE '[lite] event_points=%', v_cnt_event_points;
    INSERT INTO firearea.site_fire_distance_run_log(run_id, usgs_site, step, detail)
    VALUES (v_run_id, v_site_lower, 'event_points_lite', 'count='||v_cnt_event_points);

    IF (SELECT COUNT(*) FROM firearea._lite_event_points)=0 THEN
        INSERT INTO firearea.site_fire_distances(usgs_site,pour_point_identifier,pour_point_comid,event_id,ig_date,status,message)
        SELECT v_site_lower, v_pp_identifier, v_pp_comid, f.event_id, f.ig_date,'no_fire','No intersection points'
        FROM firearea.fires_catchments f WHERE lower(f.usgs_site)=v_site_lower;
        RETURN;
    END IF;

    -- Collect unique endpoints for event edges -----------------------------
    DROP TABLE IF EXISTS firearea._lite_event_edges;
    CREATE UNLOGGED TABLE firearea._lite_event_edges AS
    SELECT DISTINCT lep.event_id, lep.ig_date, lep.edge_id, lep.fraction, e.source, e.target, e.length_m
    FROM firearea._lite_event_points lep
    JOIN firearea.network_edges e ON e.edge_id = lep.edge_id;

    SELECT COUNT(*) INTO v_cnt_event_edges FROM firearea._lite_event_edges;
    RAISE NOTICE '[lite] event_edges(distinct)=%', v_cnt_event_edges;
    INSERT INTO firearea.site_fire_distance_run_log(run_id, usgs_site, step, detail)
    VALUES (v_run_id, v_site_lower, 'event_edges_lite', 'distinct='||v_cnt_event_edges);

    DROP TABLE IF EXISTS firearea._lite_vertices_needed;
    CREATE UNLOGGED TABLE firearea._lite_vertices_needed AS
    SELECT DISTINCT unnest(ARRAY[source,target]) AS vertex_id FROM firearea._lite_event_edges;

    SELECT COUNT(*) INTO v_cnt_vertices_needed FROM firearea._lite_vertices_needed;
    RAISE NOTICE '[lite] vertices_needed=%', v_cnt_vertices_needed;
    INSERT INTO firearea.site_fire_distance_run_log(run_id, usgs_site, step, detail)
    VALUES (v_run_id, v_site_lower, 'vertices_needed_lite', 'count='||v_cnt_vertices_needed);

    -- Connectivity analysis (component overlap) for diagnostics -----------------
    DROP TABLE IF EXISTS firearea._lite_components;
    CREATE UNLOGGED TABLE firearea._lite_components AS
    SELECT * FROM pgr_connectedComponents(
        format($c$SELECT edge_id AS id, source, target, length_m AS cost, length_m AS reverse_cost FROM firearea.network_edges WHERE lower(usgs_site)=%L$c$, v_site_lower)
    );
    -- component containing pour point start vertex
    SELECT component INTO v_component_pourpoint FROM firearea._lite_components WHERE node = v_start_vertex_id LIMIT 1;
    -- classify event edges by component of their source (proxy) -----------------
    SELECT COUNT(*) FILTER (WHERE c.component = v_component_pourpoint),
           COUNT(*) FILTER (WHERE c.component IS DISTINCT FROM v_component_pourpoint)
      INTO v_cnt_same_component, v_cnt_other_component
    FROM (
        SELECT DISTINCT e.edge_id, ec.component
        FROM firearea._lite_event_edges e
        LEFT JOIN firearea._lite_components ec ON ec.node = e.source
    ) c;
    RAISE NOTICE '[lite] component pour_point=% same_component_edges=% other_component_edges=%',
        v_component_pourpoint, v_cnt_same_component, v_cnt_other_component;
    INSERT INTO firearea.site_fire_distance_run_log(run_id, usgs_site, step, detail)
    VALUES (v_run_id, v_site_lower, 'components_lite', format('pp_component=%s same_edges=%s other_edges=%s', v_component_pourpoint, v_cnt_same_component, v_cnt_other_component));

    -- Shortest path distances (undirected) to needed vertices --------------
    DROP TABLE IF EXISTS firearea._lite_vertex_sp;
    -- Use DISTINCT ON to keep the smallest agg_cost per node (target or intermediate) from pgr_dijkstra output.
    CREATE UNLOGGED TABLE firearea._lite_vertex_sp AS
    SELECT DISTINCT ON (node) node AS vertex_id, agg_cost AS dist
    FROM pgr_dijkstra(
        format($q$SELECT edge_id AS id, source, target, length_m AS cost, length_m AS reverse_cost FROM firearea.network_edges WHERE lower(usgs_site)=%L$q$, v_site_lower),
        v_start_vertex_id,
        ARRAY(SELECT vertex_id FROM firearea._lite_vertices_needed),
        false
    )
    ORDER BY node, agg_cost;  -- keep minimal distance

    SELECT COUNT(*) INTO v_cnt_sp_rows FROM firearea._lite_vertex_sp;
    RAISE NOTICE '[lite] sp_rows(needed vertices reached)=%', v_cnt_sp_rows;
    INSERT INTO firearea.site_fire_distance_run_log(run_id, usgs_site, step, detail)
    VALUES (v_run_id, v_site_lower, 'sp_rows_lite', 'reached='||v_cnt_sp_rows);

    -- Candidate distances per event point ----------------------------------
    DROP TABLE IF EXISTS firearea._lite_event_candidate;
    CREATE UNLOGGED TABLE firearea._lite_event_candidate AS
    SELECT e.event_id, e.ig_date, e.edge_id, e.fraction,
        e.source, e.target,
        vs.dist + e.length_m * e.fraction AS dist_via_source,
        vt.dist + e.length_m * (1 - e.fraction) AS dist_via_target
    FROM firearea._lite_event_edges e
    LEFT JOIN firearea._lite_vertex_sp vs ON vs.vertex_id = e.source
    LEFT JOIN firearea._lite_vertex_sp vt ON vt.vertex_id = e.target;

    -- Minimal distance per event (base + pour point offset) ----------------
    DROP TABLE IF EXISTS firearea._lite_event_min;
    CREATE UNLOGGED TABLE firearea._lite_event_min AS
    WITH per_row AS (
        SELECT event_id, ig_date,
               CASE
                   WHEN dist_via_source IS NULL AND dist_via_target IS NULL THEN NULL
                   WHEN dist_via_source IS NULL THEN dist_via_target
                   WHEN dist_via_target IS NULL THEN dist_via_source
                   ELSE LEAST(dist_via_source, dist_via_target)
               END AS candidate_min,
               CASE
                   WHEN dist_via_source IS NULL AND dist_via_target IS NULL THEN NULL
                   WHEN dist_via_source IS NULL THEN target
                   WHEN dist_via_target IS NULL THEN source
                   WHEN dist_via_source <= dist_via_target THEN source
                   ELSE target
               END AS candidate_vertex
        FROM firearea._lite_event_candidate
    ), ranked AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY candidate_min NULLS LAST) AS rn
        FROM per_row
    )
    SELECT event_id, ig_date, candidate_min AS base_distance, candidate_vertex AS chosen_vertex_id
    FROM ranked
    WHERE rn=1;

    SELECT COUNT(*) INTO v_cnt_event_min FROM firearea._lite_event_min;
    RAISE NOTICE '[lite] event_min rows=%', v_cnt_event_min;
    INSERT INTO firearea.site_fire_distance_run_log(run_id, usgs_site, step, detail)
    VALUES (v_run_id, v_site_lower, 'event_min_lite', 'rows='||v_cnt_event_min);

    -- (No sentinel cleanup required; NULLs preserved where both via_source & via_target unreachable)

        -- Remove existing results for this site (single pour point assumption) ----
        DELETE FROM firearea.site_fire_distances WHERE usgs_site = v_site_lower;

    -- Reachable events ------------------------------------------------------
    INSERT INTO firearea.site_fire_distances(
        usgs_site, pour_point_identifier, pour_point_comid,
        event_id, ig_date, distance_m, path_edge_ids, status, message, chosen_vertex_id
    )
    SELECT v_site_lower, v_pp_identifier, v_pp_comid,
           em.event_id, em.ig_date,
           (em.base_distance + v_offset_m) AS distance_m,
           NULL, 'ok', 'lite', em.chosen_vertex_id
    FROM firearea._lite_event_min em
    WHERE em.base_distance IS NOT NULL;

    -- Unreachable events ----------------------------------------------------
    IF in_include_unreachable THEN
                INSERT INTO firearea.site_fire_distances(
                        usgs_site, pour_point_identifier, pour_point_comid,
                        event_id, ig_date, status, message, chosen_vertex_id
                )
                SELECT v_site_lower, v_pp_identifier, v_pp_comid,
                             f.event_id, f.ig_date, 'unreachable', 'lite_no_path', NULL
                FROM firearea.fires_catchments f
                WHERE lower(f.usgs_site)=v_site_lower
                    AND NOT EXISTS (
                                SELECT 1 FROM firearea._lite_event_min em
                                WHERE em.event_id = f.event_id AND em.base_distance IS NOT NULL
                    );
    END IF;

    INSERT INTO firearea.site_fire_distance_run_log(run_id, usgs_site, step, detail)
    VALUES (v_run_id, v_site_lower, 'done_lite', format('reachable=%s unreachable=%s',
        (SELECT count(*) FROM firearea.site_fire_distances WHERE usgs_site=v_site_lower AND status='ok'),
        (SELECT count(*) FROM firearea.site_fire_distances WHERE usgs_site=v_site_lower AND status='unreachable')));

EXCEPTION WHEN OTHERS THEN
    INSERT INTO firearea.site_fire_distance_log(usgs_site,pour_point_identifier,status,detail)
    VALUES (v_site_lower,v_pp_identifier,'error',COALESCE(SQLERRM,'unknown error in lite function'));
    INSERT INTO firearea.site_fire_distance_run_log(run_id, usgs_site, step, detail)
    VALUES (v_run_id, v_site_lower, 'exception_lite', COALESCE(SQLERRM,'unknown error'));
END;
$$;

--------------------------------------------------------------------------------
-- 5. Minimal Debug Helper: fn_debug_withpoints
-- Purpose: Isolate whether pgr_withPoints can return ANY rows on a trivial
--          one-to-one test for a site without the large workflow.
-- Method:
--   * Picks first edge for the site (ordered by edge_id)
--   * Creates a synthetic mid-edge point (fraction=0.5) with a pid safely
--     above existing vertex ids (max_vertex_id + 1000)
--   * Uses an inline (literal) edges SQL & points SQL in a pgr_withPoints call
--   * Reports counts & min/max agg_cost so we can see basic behavior
-- Return: rows describing diagnostics; if withpoints_row_count=0 here, the
--         issue is environmental (installation/signature) or data isolation
--         rather than the big function's assembly logic.
--------------------------------------------------------------------------------
DROP FUNCTION IF EXISTS firearea.fn_debug_withpoints(text);
CREATE OR REPLACE FUNCTION firearea.fn_debug_withpoints(in_site text)
RETURNS TABLE(step text, detail text)
LANGUAGE plpgsql AS $$
DECLARE
    v_site text := lower(in_site);
    v_sample_edge bigint;
    v_source bigint;
    v_target bigint;
    v_cost double precision;
    v_rev double precision;
    v_start_vid bigint;
    v_pid_offset bigint;
    v_sample_pid bigint;
    v_rows int;
    v_min_cost float8;
    v_max_cost float8;
    v_edges_ct int;
BEGIN
    -- Basic checks
    SELECT count(*) INTO v_edges_ct FROM firearea.network_edges WHERE lower(usgs_site)=v_site;
    IF v_edges_ct = 0 THEN
        step := 'error'; detail := 'no_edges_for_site'; RETURN NEXT; RETURN; END IF;

    -- Pick a sample edge
    SELECT edge_id, source, target, cost, reverse_cost
      INTO v_sample_edge, v_source, v_target, v_cost, v_rev
    FROM firearea.network_edges
    WHERE lower(usgs_site)=v_site
    ORDER BY edge_id
    LIMIT 1;

    IF v_sample_edge IS NULL THEN
        step := 'error'; detail := 'failed_to_select_sample_edge'; RETURN NEXT; RETURN; END IF;

    -- Start vertex = source (arbitrary)
    v_start_vid := v_source;
    -- Safe pid offset
    SELECT COALESCE(MAX(id),0) + 1000 INTO v_pid_offset FROM firearea.network_edges_vertices_pgr;
    v_sample_pid := v_pid_offset; -- single synthetic point id

    step := 'edges_count'; detail := 'edges='||v_edges_ct; RETURN NEXT;
    step := 'sample_edge'; detail := format('edge=%s src=%s tgt=%s cost=%s rev=%s', v_sample_edge, v_source, v_target, round(v_cost::numeric,3), round(v_rev::numeric,3)); RETURN NEXT;
    step := 'sample_point'; detail := format('pid=%s edge=%s fraction=0.5 start_vid=%s', v_sample_pid, v_sample_edge, v_start_vid); RETURN NEXT;

    -- Run minimal pgr_withPoints
    DROP TABLE IF EXISTS firearea._dbg_withpoints;
    CREATE UNLOGGED TABLE firearea._dbg_withpoints AS
    SELECT * FROM pgr_withPoints(
        format('SELECT edge_id AS id, source, target, cost, reverse_cost FROM firearea.network_edges WHERE lower(usgs_site)=%L', v_site),
        format('SELECT %s::bigint AS pid, %s::bigint AS edge_id, 0.5::double precision AS fraction', v_sample_pid, v_sample_edge),
        ARRAY[ v_start_vid ],
        ARRAY[ v_sample_pid ],
        false,
        'b',
        true
    );

    SELECT count(*), MIN(agg_cost), MAX(agg_cost)
      INTO v_rows, v_min_cost, v_max_cost
    FROM firearea._dbg_withpoints;

    step := 'withpoints_row_count'; detail := format('rows=%s min_cost=%s max_cost=%s', v_rows,
        COALESCE(ROUND(v_min_cost::numeric,3)::text,'null'), COALESCE(ROUND(v_max_cost::numeric,3)::text,'null')); RETURN NEXT;

    IF v_rows > 0 THEN
        step := 'withpoints_sample';
    detail := (SELECT string_agg(format('seq=%s node=%s edge=%s cost=%s agg=%s', seq, node, edge, round(cost::numeric,2), round(agg_cost::numeric,2)), ' | ')
                   FROM (SELECT seq, node, edge, cost, agg_cost FROM firearea._dbg_withpoints ORDER BY seq LIMIT 5) s);
        RETURN NEXT;
    END IF;

    -- Try legacy simpler signature (no arrays) if zero rows
    IF v_rows = 0 THEN
        DROP TABLE IF EXISTS firearea._dbg_withpoints_legacy;
        BEGIN
            CREATE UNLOGGED TABLE firearea._dbg_withpoints_legacy AS
            SELECT * FROM pgr_withPoints(
                format('SELECT edge_id AS id, source, target, cost, reverse_cost FROM firearea.network_edges WHERE lower(usgs_site)=%L', v_site),
                format('SELECT %s::bigint AS pid, %s::bigint AS edge_id, 0.5::double precision AS fraction', v_sample_pid, v_sample_edge),
                v_start_vid,
                ARRAY[ v_sample_pid ],
                false
            );
            SELECT count(*), MIN(agg_cost), MAX(agg_cost)
              INTO v_rows, v_min_cost, v_max_cost
            FROM firearea._dbg_withpoints_legacy;
            step := 'withpoints_legacy_row_count'; detail := format('rows=%s min_cost=%s max_cost=%s', v_rows,
                COALESCE(ROUND(v_min_cost::numeric,3)::text,'null'), COALESCE(ROUND(v_max_cost::numeric,3)::text,'null')); RETURN NEXT;
        EXCEPTION WHEN OTHERS THEN
            step := 'withpoints_legacy_error'; detail := SQLERRM; RETURN NEXT;
        END;
    END IF;

    -- Dijkstra direct test from start vertex to the opposite endpoint of sample edge
    BEGIN
        DROP TABLE IF EXISTS firearea._dbg_dijkstra_edge;
        CREATE UNLOGGED TABLE firearea._dbg_dijkstra_edge AS
        SELECT * FROM pgr_dijkstra(
            format('SELECT edge_id AS id, source, target, cost, reverse_cost FROM firearea.network_edges WHERE lower(usgs_site)=%L', v_site),
            v_start_vid,
            ARRAY[ v_target ],
            false
        );
        SELECT count(*), MIN(agg_cost), MAX(agg_cost)
          INTO v_rows, v_min_cost, v_max_cost
        FROM firearea._dbg_dijkstra_edge;
        step := 'dijkstra_row_count'; detail := format('rows=%s min_cost=%s max_cost=%s', v_rows,
            COALESCE(ROUND(v_min_cost::numeric,3)::text,'null'), COALESCE(ROUND(v_max_cost::numeric,3)::text,'null')); RETURN NEXT;
        IF (SELECT count(*) FROM firearea._dbg_dijkstra_edge) > 0 THEN
            step := 'dijkstra_sample'; detail := (SELECT string_agg(format('seq=%s node=%s edge=%s cost=%s agg=%s', seq, node, edge, round(cost::numeric,2), round(agg_cost::numeric,2)),' | ')
                                                 FROM (SELECT seq, node, edge, cost, agg_cost FROM firearea._dbg_dijkstra_edge ORDER BY seq LIMIT 5) s);
            RETURN NEXT;
        END IF;
    EXCEPTION WHEN OTHERS THEN
        step := 'dijkstra_error'; detail := SQLERRM; RETURN NEXT;
    END;

    -- Version & signatures introspection
    BEGIN
        step := 'pgr_version'; detail := (SELECT 'version='||pgr_version()); RETURN NEXT;
    EXCEPTION WHEN OTHERS THEN
        step := 'pgr_version_error'; detail := SQLERRM; RETURN NEXT;
    END;
    BEGIN
        step := 'withpoints_sigs'; detail := (
            SELECT string_agg(pg_get_function_identity_arguments(p.oid),' || ')
            FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
            WHERE lower(p.proname)='pgr_withpoints'
        ); RETURN NEXT;
    EXCEPTION WHEN OTHERS THEN
        step := 'withpoints_sigs_error'; detail := SQLERRM; RETURN NEXT;
    END;

    -- Additional experiment: use a small negative pid (common pgRouting example pattern) to see if large positive pid is problematic
    BEGIN
        DROP TABLE IF EXISTS firearea._dbg_withpoints_neg;
        CREATE UNLOGGED TABLE firearea._dbg_withpoints_neg AS
        SELECT * FROM pgr_withPoints(
            format('SELECT edge_id AS id, source, target, cost, reverse_cost FROM firearea.network_edges WHERE lower(usgs_site)=%L', v_site),
            format('SELECT %s::bigint AS pid, %s::bigint AS edge_id, 0.5::double precision AS fraction', -1, v_sample_edge),
            v_start_vid,
            -1,
            false,
            'b',
            true
        );
        SELECT count(*), MIN(agg_cost), MAX(agg_cost)
          INTO v_rows, v_min_cost, v_max_cost
        FROM firearea._dbg_withpoints_neg;
        step := 'withpoints_negpid_row_count'; detail := format('rows=%s min_cost=%s max_cost=%s', v_rows,
            COALESCE(ROUND(v_min_cost::numeric,3)::text,'null'), COALESCE(ROUND(v_max_cost::numeric,3)::text,'null')); RETURN NEXT;
    EXCEPTION WHEN OTHERS THEN
        step := 'withpoints_negpid_error'; detail := SQLERRM; RETURN NEXT;
    END;

    RETURN;
END;
$$;

--------------------------------------------------------------------------------
-- EXPORT FUNCTION: fn_fire_event_paths_lite
-- Purpose:
--   Reconstruct (post-hoc) the shortest path geometry used implicitly by
--   fn_compute_site_fire_distances_lite for each fire event of a site, plus
--   the specific intersection point (vertex along an event edge) that yielded
--   the minimal distance. This does NOT modify any existing tables; it derives
--   everything on-the-fly and therefore can be safely used for ad hoc export.
--
-- Rationale:
--   The lite distance function intentionally omits path reconstruction for
--   performance and simplicity. For visualization or GIS export (e.g. via
--   ogr2ogr) we sometimes need the actual flow path polyline(s) from the pour
--   point to the selected event intersection point. This function repeats a
--   subset of the lite logic (pour point selection, event intersection
--   generation, shortest-path to edge endpoints, candidate evaluation) and
--   then gathers the edge sequence for the chosen endpoint.
--
-- Key Points:
--   * Does NOT depend on prior execution of the lite function (it recomputes).
--   * Returns one row per wildfire event_id.
--   * Path is assembled as a merged geometry of all edges in the dijkstra path
--     (start vertex → chosen endpoint vertex) plus the partial segment along
--     the event edge from that endpoint to the actual intersection point.
--   * Distance fields allow cross-check with site_fire_distances (base + offset).
--
-- Return Columns:
--   event_id                Fire event identifier
--   ig_date                 Event date
--   pour_point_identifier   Selected pour point id
--   chosen_endpoint         'source' or 'target' for the event edge endpoint used
--   fraction                Fraction along the event edge (source→target)
--   base_distance_m         Distance from pour point start vertex to intersection MINUS pour point mid-edge offset
--   pour_point_offset_m     Offset along pour point edge from chosen start vertex to actual pour point
--   total_distance_m        base_distance_m + pour_point_offset_m (should match site_fire_distances.distance_m)
--   path_edge_count         Number of whole edges in the vertex-to-vertex path (excludes partial event segment)
--   path_geom               Geometry (LineString or MultiLineString) in SRID 5070
--   intersection_point      Point geometry (SRID 5070) of selected fire intersection
--
-- Notes / Limitations:
--   * Undirected traversal (same as lite function).
--   * If multiple intersection edges tie exactly, source endpoint is preferred.
--   * Geometry merging uses ST_LineMerge(ST_Collect(...)); topology direction
--     is not explicitly enforced—suitable for visualization & distance display.
--
-- Example ogr2ogr Usage:
--   Export paths:
--     ogr2ogr -f GeoJSON syca_fire_paths.geojson \
--       PG:"host=HOST dbname=DB user=USER password=PASS" \
--       -sql "SELECT event_id, ig_date, total_distance_m, path_geom FROM firearea.fn_fire_event_paths_lite('syca')" \
--       -nln syca_fire_paths
--
--   Export intersection points:
--     ogr2ogr -f GeoJSON syca_fire_points.geojson \
--       PG:"host=HOST dbname=DB user=USER password=PASS" \
--       -sql "SELECT event_id, ig_date, intersection_point FROM firearea.fn_fire_event_paths_lite('syca')" \
--       -nln syca_fire_points
--
DROP FUNCTION IF EXISTS firearea.fn_fire_event_paths_lite(text, double precision);
CREATE OR REPLACE FUNCTION firearea.fn_fire_event_paths_lite(
    in_usgs_site text,
    in_snap_tolerance_m double precision DEFAULT 60.0
) RETURNS TABLE(
    event_id text,
    ig_date date,
    pour_point_identifier text,
    chosen_endpoint text,
    fraction double precision,
    base_distance_m double precision,
    pour_point_offset_m double precision,
    total_distance_m double precision,
    path_edge_count integer,
    path_edge_ids bigint[],
    start_vertex_id bigint,
    end_vertex_id bigint,
    end_vertex_geom geometry(Point,5070),
    path_geom geometry(LineString,5070),
    intersection_point geometry(Point,5070)
) LANGUAGE plpgsql AS $$
DECLARE
    v_site_lower text := lower(in_usgs_site);
    v_pp_identifier text;
    v_pp_comid text;
    v_pp_edge_id bigint;
    v_pp_fraction double precision;
    v_pp_is_vertex boolean;
    v_pp_snap_distance double precision;
    v_start_vertex_id bigint;
    v_offset_m double precision := 0; -- pour point offset along its edge
    v_edge_len double precision;
    v_edge_source bigint;
    v_edge_target bigint;
BEGIN
    -- Preconditions (mirror lite function minimal checks)
    IF NOT EXISTS (SELECT 1 FROM firearea.network_edges WHERE lower(usgs_site)=v_site_lower) THEN
        RAISE EXCEPTION 'No network edges for site %', v_site_lower; END IF;
    IF NOT EXISTS (SELECT 1 FROM firearea.fires_catchments WHERE lower(usgs_site)=v_site_lower) THEN
        RAISE EXCEPTION 'No fire events for site %', v_site_lower; END IF;
    IF NOT EXISTS (SELECT 1 FROM firearea.pour_point_snaps WHERE lower(usgs_site)=v_site_lower) THEN
        RAISE EXCEPTION 'No pour point snaps for site %', v_site_lower; END IF;
    -- Explicit projected geom column presence check (most common hidden cause of failures here)
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='firearea' AND table_name='network_edges' AND column_name='geom_5070'
    ) THEN
        RAISE EXCEPTION 'Expected column firearea.network_edges.geom_5070 not found. Rebuild topology with SRID 5070 (fn_build_network_topology) or adjust function to match existing SRID.';
    END IF;

    -- Select pour point (same ordering rule)
    SELECT identifier, comid, snap_edge_id, snap_fraction, is_exact_vertex, snap_distance_m
      INTO v_pp_identifier, v_pp_comid, v_pp_edge_id, v_pp_fraction, v_pp_is_vertex, v_pp_snap_distance
    FROM firearea.pour_point_snaps
    WHERE lower(usgs_site)=v_site_lower
    ORDER BY snap_distance_m, identifier
    LIMIT 1;
    IF v_pp_identifier IS NULL THEN
        RAISE EXCEPTION 'Pour point selection failed for site %', v_site_lower; END IF;
    IF v_pp_snap_distance IS NULL OR v_pp_snap_distance > in_snap_tolerance_m THEN
        RAISE EXCEPTION 'Pour point snap distance % exceeds tolerance % for site %', v_pp_snap_distance, in_snap_tolerance_m, v_site_lower; END IF;

    -- Nearest start vertex
    SELECT v.id INTO v_start_vertex_id
    FROM firearea.network_edges_vertices_pgr v
    JOIN firearea.network_edges e ON (e.source=v.id OR e.target=v.id)
    WHERE lower(e.usgs_site)=v_site_lower
    ORDER BY v.the_geom <-> (
        SELECT snap_geom FROM firearea.pour_point_snaps
        WHERE lower(usgs_site)=v_site_lower AND identifier=v_pp_identifier LIMIT 1)
    LIMIT 1;
    IF v_start_vertex_id IS NULL THEN
        RAISE EXCEPTION 'Start vertex could not be determined for site %', v_site_lower; END IF;

    -- Pour point offset
    SELECT source, target, length_m INTO v_edge_source, v_edge_target, v_edge_len
    FROM firearea.network_edges WHERE edge_id = v_pp_edge_id;
    IF v_edge_len IS NOT NULL AND NOT v_pp_is_vertex THEN
        IF v_start_vertex_id = v_edge_source THEN
            v_offset_m := v_edge_len * v_pp_fraction;
        ELSIF v_start_vertex_id = v_edge_target THEN
            v_offset_m := v_edge_len * (1 - v_pp_fraction);
        ELSE
            v_offset_m := LEAST(v_edge_len * v_pp_fraction, v_edge_len * (1 - v_pp_fraction));
        END IF;
    END IF;

    -- Event intersection points & fallback (enhanced robustness)
    -- Improvements in this revision:
    --   * Handles point AND line overlaps (boundary following an edge) by deriving a midpoint fraction from overlapping line segments.
    --   * Processes geometry collections safely (extracts point & line components separately).
    --   * Produces at most one fraction per (event_id, edge_id, fraction) combination (dedup downstream).
    --   * Reduces likelihood of NULL fractions causing runtime issues.
    DROP TABLE IF EXISTS _exp_event_points;
    CREATE TEMP TABLE _exp_event_points ON COMMIT DROP AS
    WITH fire_raw AS (
        SELECT fc.event_id, fc.ig_date,
               CASE WHEN GeometryType(fc.geometry) LIKE 'Multi%'
                        THEN ST_CollectionExtract(fc.geometry,3)
                    ELSE fc.geometry END AS geom_4326
        FROM firearea.fires_catchments fc
        WHERE lower(fc.usgs_site)=v_site_lower
    ), fire_poly AS (
        SELECT fr.event_id, fr.ig_date, ST_Transform(fr.geom_4326,5070) AS geom_5070
        FROM fire_raw fr
    ), edge_prepped AS (
        SELECT edge_id,
               CASE WHEN ne.geom_5070 IS NULL THEN NULL
                    WHEN GeometryType(ne.geom_5070) IN ('MULTILINESTRING','MULTICURVE') THEN ST_LineMerge(ne.geom_5070)
                    ELSE ne.geom_5070 END AS line_geom
        FROM firearea.network_edges ne
        WHERE lower(ne.usgs_site)=v_site_lower
    ), raw_intersections AS (
        -- Dump all boundary intersections (could be points, lines, or mixed)
        SELECT f.event_id, f.ig_date, ep.edge_id,
               (ST_Dump(
                   ST_Intersection(
                       ST_Boundary(f.geom_5070),
                       ep.line_geom
                   )
               )).geom AS ig
        FROM fire_poly f
        JOIN edge_prepped ep
          ON ep.line_geom IS NOT NULL
         AND ST_Intersects(f.geom_5070, ep.line_geom)
        WHERE ST_Intersects(ST_Boundary(f.geom_5070), ep.line_geom)
    ), point_geoms AS (
        SELECT ri.event_id, ri.ig_date, ri.edge_id, (ST_Dump(
            CASE WHEN GeometryType(ri.ig)='MULTIPOINT' THEN ri.ig ELSE ri.ig END)).geom AS pt
        FROM raw_intersections ri
        WHERE GeometryType(ri.ig) IN ('POINT','MULTIPOINT')
    ), line_geoms AS (
        -- Overlapping boundary/edge segments -> take midpoint for representative fraction
        SELECT ri.event_id, ri.ig_date, ri.edge_id,
               ST_LineMerge(ri.ig) AS seg
        FROM raw_intersections ri
        WHERE GeometryType(ri.ig) IN ('LINESTRING','MULTILINESTRING')
    ), line_midpoints AS (
        SELECT lg.event_id, lg.ig_date, lg.edge_id,
               -- Use midpoint along the overlapping segment as representative intersection
               ST_LineInterpolatePoint(lg.seg,0.5) AS pt
        FROM line_geoms lg
        WHERE lg.seg IS NOT NULL AND NOT ST_IsEmpty(lg.seg) AND ST_GeometryType(lg.seg) LIKE '%LINESTRING%'
    ), all_points AS (
        SELECT pg.event_id, pg.ig_date, pg.edge_id, pg.pt FROM point_geoms pg
        UNION ALL
        SELECT lm.event_id, lm.ig_date, lm.edge_id, lm.pt FROM line_midpoints lm
    ), fractions AS (
        SELECT ap.event_id, ap.ig_date, ap.edge_id,
               ST_LineLocatePoint(ep.line_geom, ST_Force2D(ap.pt)) AS fraction
        FROM all_points ap
        JOIN edge_prepped ep USING (edge_id)
        WHERE ap.pt IS NOT NULL AND NOT ST_IsEmpty(ap.pt)
    )
    SELECT DISTINCT fr.event_id, fr.ig_date, fr.edge_id, fr.fraction
    FROM fractions fr
    WHERE fr.fraction BETWEEN 0 AND 1;  -- guard against numeric edge cases

    -- Basic diagnostics (NOTICE only; harmless if ignored)
    BEGIN
        PERFORM 1;  -- placeholder block to allow EXCEPTION capture if needed later
        RAISE NOTICE '[paths_lite] intersections: points=% lines=% final_rows=%',
            (SELECT count(*) FROM raw_intersections WHERE GeometryType(ig) IN ('POINT','MULTIPOINT')),
            (SELECT count(*) FROM raw_intersections WHERE GeometryType(ig) IN ('LINESTRING','MULTILINESTRING')),
            (SELECT count(*) FROM _exp_event_points);
    EXCEPTION WHEN OTHERS THEN
        -- Swallow any diagnostic errors silently
        NULL;
    END;

    -- Fallback midpoint for containment
        INSERT INTO _exp_event_points(event_id, ig_date, edge_id, fraction)
        SELECT f.event_id, f.ig_date, ne.edge_id, 0.5
        FROM firearea.fires_catchments f
        JOIN firearea.network_edges ne
            ON lower(ne.usgs_site)=v_site_lower
         AND lower(f.usgs_site)=v_site_lower
         AND ST_Contains(ST_Transform(f.geometry,5070), ne.geom_5070)
        WHERE lower(f.usgs_site)=v_site_lower
            AND f.event_id NOT IN (SELECT DISTINCT ep.event_id FROM _exp_event_points ep);

    -- Deduplicate
    DELETE FROM _exp_event_points a USING _exp_event_points b
    WHERE a.ctid < b.ctid
      AND a.event_id=b.event_id AND a.edge_id=b.edge_id AND a.fraction=b.fraction;

    -- Candidate event edges
    DROP TABLE IF EXISTS _exp_event_edges;
    CREATE TEMP TABLE _exp_event_edges ON COMMIT DROP AS
    SELECT DISTINCT ep.event_id, ep.ig_date, ep.edge_id, ep.fraction, e.source, e.target, e.length_m
    FROM _exp_event_points ep
    JOIN firearea.network_edges e ON e.edge_id = ep.edge_id;

    -- Needed vertices list
    DROP TABLE IF EXISTS _exp_vertices_needed;
    CREATE TEMP TABLE _exp_vertices_needed ON COMMIT DROP AS
    SELECT DISTINCT unnest(ARRAY[source,target]) AS vertex_id FROM _exp_event_edges;

    -- Dijkstra full path (retain sequences) from single start to all needed vertices
    DROP TABLE IF EXISTS _exp_paths_raw;
    CREATE TEMP TABLE _exp_paths_raw ON COMMIT DROP AS
    SELECT * FROM pgr_dijkstra(
        format($q$SELECT edge_id AS id, source, target, length_m AS cost, length_m AS reverse_cost FROM firearea.network_edges WHERE lower(usgs_site)=%L$q$, v_site_lower),
        v_start_vertex_id,
        ARRAY(SELECT vertex_id FROM _exp_vertices_needed),
        false
    );

    -- Minimal distance per vertex_id (distinct on node)
    DROP TABLE IF EXISTS _exp_vertex_dist;
    CREATE TEMP TABLE _exp_vertex_dist ON COMMIT DROP AS
    SELECT DISTINCT ON (node) node AS vertex_id, agg_cost AS dist
    FROM _exp_paths_raw
    ORDER BY node, agg_cost;

    -- Event candidate distances (like lite)
    DROP TABLE IF EXISTS _exp_event_candidate;
    CREATE TEMP TABLE _exp_event_candidate ON COMMIT DROP AS
    SELECT e.event_id, e.ig_date, e.edge_id, e.fraction,
        vdS.dist + e.length_m * e.fraction AS dist_via_source,
        vdT.dist + e.length_m * (1 - e.fraction) AS dist_via_target,
           e.source, e.target, e.length_m
    FROM _exp_event_edges e
    LEFT JOIN _exp_vertex_dist vdS ON vdS.vertex_id = e.source
    LEFT JOIN _exp_vertex_dist vdT ON vdT.vertex_id = e.target;

    -- Choose best candidate per event (tie -> source)
    DROP TABLE IF EXISTS _exp_event_choice;
    CREATE TEMP TABLE _exp_event_choice ON COMMIT DROP AS
    SELECT * FROM (
        SELECT ec.event_id, ec.ig_date, ec.edge_id, ec.fraction,
               CASE
                   WHEN ec.dist_via_source IS NULL AND ec.dist_via_target IS NULL THEN NULL
                   WHEN ec.dist_via_source IS NULL THEN 'target'
                   WHEN ec.dist_via_target IS NULL THEN 'source'
                   WHEN ec.dist_via_source <= ec.dist_via_target THEN 'source'
                   ELSE 'target'
               END AS chosen_endpoint,
               CASE
                   WHEN ec.dist_via_source IS NULL AND ec.dist_via_target IS NULL THEN NULL
                   WHEN ec.dist_via_source IS NULL THEN ec.dist_via_target
                   WHEN ec.dist_via_target IS NULL THEN ec.dist_via_source
                   ELSE LEAST(ec.dist_via_source, ec.dist_via_target)
               END AS base_distance_m,
               ec.source, ec.target, ec.length_m,
               ROW_NUMBER() OVER (PARTITION BY ec.event_id ORDER BY 
                  COALESCE(LEAST(ec.dist_via_source, ec.dist_via_target), 1e308)) AS rn
        FROM _exp_event_candidate ec
    ) s WHERE rn=1;  -- pick minimal

    -- Assemble path edges for chosen endpoint
    RETURN QUERY
    WITH chosen AS (
        SELECT c.*, (c.chosen_endpoint='source')::boolean AS via_source
        FROM _exp_event_choice c
        WHERE c.base_distance_m IS NOT NULL
    ), endpoint_vertex AS (
        SELECT ch.event_id, ch.ig_date, ch.via_source,
               CASE WHEN ch.via_source THEN ch.source ELSE ch.target END AS end_vertex,
               ch.source AS source, ch.target AS target,
               ch.edge_id, ch.fraction, ch.base_distance_m, ch.chosen_endpoint, ch.length_m
        FROM chosen ch
    ), path_edges AS (
        SELECT ep.event_id, ep.ig_date, pr.seq, pr.edge, pr.node, pr.agg_cost,
               ne.geom_5070
        FROM endpoint_vertex ep
    JOIN _exp_paths_raw pr ON pr.end_vid = ep.end_vertex
        JOIN firearea.network_edges ne ON ne.edge_id = pr.edge
        WHERE pr.edge <> -1  -- exclude virtual rows produced for start vertex sometimes
          AND pr.edge IS NOT NULL
    ), path_geom_lines AS (
       SELECT pe.event_id, pe.ig_date,
            CASE WHEN COUNT(*) = 0 THEN NULL
                ELSE ST_LineMerge(ST_Collect(pe.geom_5070)) END AS core_path_geom,
            COUNT(*) AS edge_ct,
            CASE WHEN COUNT(*)=0 THEN ARRAY[]::bigint[]
                ELSE ARRAY_AGG(pe.edge ORDER BY pe.seq) END AS edge_ids
       FROM path_edges pe
       GROUP BY pe.event_id, pe.ig_date
    ), partial_seg AS (
       SELECT ev.event_id, ev.ig_date,
            CASE WHEN ev.via_source THEN
                ST_LineSubstring(ne.geom_5070, 0, ev.fraction)
            ELSE
                ST_Reverse(ST_LineSubstring(ne.geom_5070, ev.fraction, 1))
            END AS partial_geom
       FROM endpoint_vertex ev
       JOIN firearea.network_edges ne ON ne.edge_id = ev.edge_id
    ), intersection_point AS (
        SELECT ev.event_id, ev.ig_date,
               ST_LineInterpolatePoint(ne.geom_5070, ev.fraction) AS ipt
        FROM endpoint_vertex ev
        JOIN firearea.network_edges ne ON ne.edge_id = ev.edge_id
    ), end_vertex_geom AS (
        SELECT DISTINCT evx.event_id, evx.ig_date, evx.end_vertex AS end_vertex_id, v.the_geom::geometry(Point,5070) AS end_vertex_geom
        FROM (
            SELECT ep.event_id, ep.ig_date, ep.via_source,
                   CASE WHEN ep.via_source THEN ep.source ELSE ep.target END AS end_vertex
            FROM endpoint_vertex ep
        ) evx
        JOIN firearea.network_edges_vertices_pgr v ON v.id = evx.end_vertex
    ), merged AS (
     SELECT p.event_id, p.ig_date, p.edge_ct, p.edge_ids, ev.fraction, ev.base_distance_m,
         ev.chosen_endpoint, p.core_path_geom, ps.partial_geom, ip.ipt,
         ev.via_source, ev.edge_id, eg.end_vertex_id, eg.end_vertex_geom
     FROM path_geom_lines p
     JOIN endpoint_vertex ev USING (event_id, ig_date)
     LEFT JOIN partial_seg ps USING (event_id, ig_date)
     LEFT JOIN intersection_point ip USING (event_id, ig_date)
     LEFT JOIN end_vertex_geom eg USING (event_id, ig_date)
    )
    SELECT m.event_id::text AS event_id,
        m.ig_date,
        v_pp_identifier::text AS pour_point_identifier,
        m.chosen_endpoint::text,
           m.fraction,
           m.base_distance_m AS base_distance_m,
           v_offset_m AS pour_point_offset_m,
           (m.base_distance_m + v_offset_m) AS total_distance_m,
        m.edge_ct::int AS path_edge_count,
           m.edge_ids AS path_edge_ids,
           v_start_vertex_id AS start_vertex_id,
           m.end_vertex_id AS end_vertex_id,
           m.end_vertex_geom,
           ST_LineMerge(ST_Collect(COALESCE(m.core_path_geom, ST_GeomFromText('LINESTRING EMPTY',5070)), m.partial_geom))::geometry(LineString,5070) AS path_geom,
           m.ipt AS intersection_point
    FROM merged m
    GROUP BY m.event_id, m.ig_date, m.chosen_endpoint, m.fraction,
             m.base_distance_m, m.edge_ct, m.edge_ids, m.core_path_geom, m.partial_geom, m.ipt,
             v_pp_identifier, v_offset_m, v_start_vertex_id, m.end_vertex_id, m.end_vertex_geom
    ORDER BY total_distance_m;
END;
$$;

-- Example: SELECT * FROM firearea.fn_fire_event_paths_lite('syca');

-- Example usage:
-- SELECT * FROM firearea.fn_debug_withpoints('syca');

-- Example (commented):
-- SELECT firearea.fn_build_network_topology(true);
-- SELECT firearea.fn_refresh_pour_point_snaps();
-- SELECT firearea.fn_rebuild_network_and_snaps(); -- do not run!

-- TRUNCATE firearea.site_fire_distance_log ;
-- TRUNCATE firearea.site_fire_distance_run_log ;
-- TRUNCATE firearea.site_fire_distances ;

-- select * from firearea.site_fire_distance_log ;
-- select * from firearea.site_fire_distance_run_log ;
-- select * from firearea.site_fire_distances ;

-- SELECT * FROM firearea.fn_compute_site_fire_distances_lite('sbc_lter_rat', 60.0, true);
-- SELECT * FROM firearea.fn_debug_withpoints('syca');

-- SELECT * FROM firearea.fn_fire_event_paths_lite('syca');

--------------------------------------------------------------------------------
-- PERMANENT CLIPPED + SPLIT TOPOLOGY WORKFLOW (Modular Additions)
-- Goal:
--   Provide a preprocessing pipeline that (a) splits network edges at pour
--   point and fire event boundary intersection locations so those positions
--   become actual graph vertices (eliminating mid‑edge fraction logic & pour
--   point offset) and (b) enables an even simpler vertex‑only shortest path
--   distance function. These additions are intentionally separate from the
--   existing lite workflow so we can A/B validate results before migrating.
--
-- Overview of New Artifacts:
--   * Table firearea.network_edges_split               (fully rebuilt per invocation)
--   * Table firearea.network_edges_vertices_pgr_split  (topology vertices)
--   * Table firearea.fire_event_vertices               (event -> vertex mapping)
--   * Function firearea.fn_prepare_site_split_topology (per-site splitting)
--   * Function firearea.fn_compute_site_fire_distances_vertex (distance calc)
--
-- Design Notes:
--   1. Rebuild Strategy: For simplicity & determinism the prepare function
--      (re)creates the entire split edge table from the current canonical
--      network (firearea.network_edges) on each call, then applies splitting
--      only to the target site. Non‑target site edges pass through unmodified.
--      This avoids complexity of incremental topology edits but means the
--      operation is O(total edges). Optimize later if needed.
--   2. Splitting Logic: For the target site gather fractions per edge from:
--        - Pour point snap fraction (if not already ~0 or ~1)
--        - Fire polygon boundary intersection points
--        - Fire containment fallback midpoint (0.5) where no boundary points
--      Fractions are deduped & sorted; consecutive pairs produce segments via
--      ST_LineSubstring. Endpoints (0,1) always included.
--   3. Vertex Promotion: After splitting, all former intersection positions
--      correspond to edge endpoints (graph vertices). We then re‑derive the
--      precise event vertices by snapping original intersection points to the
--      nearest new vertex within a small tolerance.
--   4. Distance Simplification: New function runs a single pgr_dijkstra from
--      the pour point vertex to all event vertices. No offset, no per‑edge
--      candidate evaluation, no fraction arithmetic.
--   5. Future Optimization: If rebuild cost becomes large, introduce a site‑
--      scoped split table (e.g. network_edges_split_site) or incremental edge
--      replacement, or maintain a precomputed global intersection cache.
--
-- Assumptions:
--   * Existing tables: network_edges (with geom_5070, length_m, cost) and
--     pour_point_snaps, fires_catchments.
--   * SRID 5070 is the routing SRID (adjust param if different).
--
-- Caveats:
--   * Directionality / upstream constraints are NOT enforced (same as lite).
--   * Fire polygons updated after a split require a rerun of prepare function.
--   * Multi‑site concurrent usage will cause entire table rebuild each call.
--
--------------------------------------------------------------------------------

-- (A) Tables (created lazily if absent)
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
        CREATE INDEX network_edges_split_site_idx ON firearea.network_edges_split (usgs_site);
        CREATE INDEX network_edges_split_geom_idx ON firearea.network_edges_split USING GIST (geom_5070);
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
        CREATE INDEX fire_event_vertices_vertex_idx ON firearea.fire_event_vertices (vertex_id);
        CREATE INDEX fire_event_vertices_event_idx ON firearea.fire_event_vertices (usgs_site, event_id);
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

--------------------------------------------------------------------------------
-- (B) Function: fn_prepare_site_split_topology
-- Purpose: Rebuild network_edges_split (global) and split edges for the target
--          site at pour point & fire boundary intersection fractions so those
--          positions become graph vertices. Populates fire_event_vertices.
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION firearea.fn_prepare_site_split_topology(
    in_usgs_site text,
    in_srid integer DEFAULT 5070,
    in_network_tolerance double precision DEFAULT 0.5,
    in_vertex_snap_tolerance double precision DEFAULT 0.25,
    in_fraction_merge_epsilon double precision DEFAULT 1e-7
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_site_lower text := lower(in_usgs_site);
    v_pp_edge_id bigint;
    v_pp_fraction double precision;
    v_exists boolean;
    v_geom_col text := format('geom_%s', in_srid);
BEGIN
    -- Preconditions
    IF NOT EXISTS (SELECT 1 FROM firearea.network_edges LIMIT 1) THEN
        RAISE EXCEPTION 'Base network_edges empty; build topology first.'; END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='firearea' AND table_name='network_edges' AND column_name=v_geom_col) THEN
        RAISE EXCEPTION 'Projected geom column % for SRID %s missing in network_edges', v_geom_col, in_srid; END IF;
    IF NOT EXISTS (SELECT 1 FROM firearea.pour_point_snaps WHERE lower(usgs_site)=v_site_lower) THEN
        RAISE EXCEPTION 'Pour point snaps missing for site %', v_site_lower; END IF;
    IF NOT EXISTS (SELECT 1 FROM firearea.fires_catchments WHERE lower(usgs_site)=v_site_lower) THEN
        RAISE NOTICE 'No fires for site %; will split only at pour point (if needed).', v_site_lower; END IF;

    -- Select pour point (first / closest) for fraction
    SELECT snap_edge_id, snap_fraction
      INTO v_pp_edge_id, v_pp_fraction
    FROM firearea.pour_point_snaps
    WHERE lower(usgs_site)=v_site_lower
    ORDER BY snap_distance_m, identifier
    LIMIT 1;
    IF v_pp_edge_id IS NULL THEN
        RAISE EXCEPTION 'Could not determine pour point edge for site %', v_site_lower; END IF;

    -- Rebuild entire split table from base as pass-through copy first
    TRUNCATE firearea.network_edges_split RESTART IDENTITY;
    EXECUTE format('INSERT INTO firearea.network_edges_split(
          usgs_site, original_edge_id, segment_index,
          start_fraction, end_fraction, geom_5070,
          length_m, cost, reverse_cost)
         SELECT ne.usgs_site, ne.edge_id, NULL, NULL, NULL, ne.geom_%s,
             ne.length_m, ne.length_m, ne.length_m
         FROM firearea.network_edges ne', in_srid);

    -- Remove unsplit edges for the target site (we will replace with split segments)
    DELETE FROM firearea.network_edges_split WHERE lower(usgs_site)=v_site_lower;

    -- Gather fractions for target site edges
    DROP TABLE IF EXISTS tmp_site_edges;
    CREATE TEMP TABLE tmp_site_edges AS
    SELECT edge_id, usgs_site, geom_5070, length_m
    FROM firearea.network_edges
    WHERE lower(usgs_site)=v_site_lower;

    -- Boundary intersection points
    DROP TABLE IF EXISTS tmp_fire_intersections;
    CREATE TEMP TABLE tmp_fire_intersections AS
    WITH fire_poly AS (
        SELECT event_id, ig_date,
               CASE WHEN GeometryType(geometry) LIKE 'Multi%'
                        THEN ST_CollectionExtract(geometry,3)
                    ELSE geometry END AS geom
        FROM firearea.fires_catchments
        WHERE lower(usgs_site)=v_site_lower
    ), fire_5070 AS (
        SELECT event_id, ig_date, ST_Transform(geom, in_srid) AS geom_5070 FROM fire_poly
    ), raw_int AS (
        SELECT f.event_id, f.ig_date, e.edge_id,
               (ST_Dump(ST_Intersection(ST_Boundary(f.geom_5070), e.geom_5070))).geom AS ig
        FROM fire_5070 f
        JOIN tmp_site_edges e ON ST_Intersects(f.geom_5070, e.geom_5070)
        WHERE ST_Intersects(ST_Boundary(f.geom_5070), e.geom_5070)
    ), pts AS (
        SELECT event_id, ig_date, edge_id, ig AS pt
        FROM raw_int WHERE GeometryType(ig) IN ('POINT','MULTIPOINT')
    ), line_mid AS (
        SELECT event_id, ig_date, edge_id, ST_LineInterpolatePoint(ST_LineMerge(ig),0.5) AS pt
        FROM raw_int WHERE GeometryType(ig) IN ('LINESTRING','MULTILINESTRING')
    ), all_pts AS (
        SELECT * FROM pts
        UNION ALL
        SELECT * FROM line_mid
    )
    SELECT event_id, ig_date, edge_id,
           ST_LineLocatePoint(e.geom_5070, ST_Force2D(pt)) AS fraction,
           ST_Force2D(pt) AS pt_geom
    FROM all_pts ap
    JOIN tmp_site_edges e USING(edge_id)
    WHERE pt IS NOT NULL;

    -- Containment fallback midpoint for fires lacking boundary points
    INSERT INTO tmp_fire_intersections(event_id, ig_date, edge_id, fraction, pt_geom)
    SELECT f.event_id, f.ig_date, e.edge_id, 0.5,
           ST_LineInterpolatePoint(e.geom_5070,0.5)
    FROM firearea.fires_catchments f
    JOIN tmp_site_edges e ON ST_Contains(ST_Transform(f.geometry,in_srid), e.geom_5070)
    WHERE lower(f.usgs_site)=v_site_lower
      AND f.event_id NOT IN (SELECT DISTINCT event_id FROM tmp_fire_intersections);

    -- Primary fractions (fire + pour point if interior)
    DROP TABLE IF EXISTS tmp_edge_fractions_raw;
    CREATE TEMP TABLE tmp_edge_fractions_raw AS
    SELECT edge_id, fraction FROM tmp_fire_intersections
    UNION ALL
    SELECT v_pp_edge_id AS edge_id, v_pp_fraction AS fraction;

    -- Retain only interior pour point fraction if meaningfully distinct
    DELETE FROM tmp_edge_fractions_raw
    WHERE edge_id = v_pp_edge_id AND (fraction <= in_fraction_merge_epsilon OR (1 - fraction) <= in_fraction_merge_epsilon);

    -- Add 0 & 1 for each site edge, merge & deduplicate with tolerance
    DROP TABLE IF EXISTS tmp_edge_fractions;
    CREATE TEMP TABLE tmp_edge_fractions AS
    WITH base AS (
        SELECT edge_id, 0::double precision AS fraction FROM tmp_site_edges
        UNION ALL
        SELECT edge_id, 1::double precision FROM tmp_site_edges
        UNION ALL
        SELECT edge_id, fraction FROM tmp_edge_fractions_raw
    ), ordered AS (
        SELECT edge_id, fraction
        FROM (
            SELECT edge_id,
                   /* snap fractions very close together to a representative */
                   ROUND(fraction::numeric, 7) AS fraction
            FROM base
        ) s
        GROUP BY edge_id, fraction
    )
    SELECT * FROM ordered;

    -- Generate segments (consecutive fraction pairs)
    DROP TABLE IF EXISTS tmp_segments;
    CREATE TEMP TABLE tmp_segments AS
    WITH ord AS (
        SELECT ef.edge_id, ef.fraction,
               LEAD(ef.fraction) OVER (PARTITION BY ef.edge_id ORDER BY ef.fraction) AS next_fraction,
               ROW_NUMBER() OVER (PARTITION BY ef.edge_id ORDER BY ef.fraction) AS rn
        FROM tmp_edge_fractions ef
    )
    SELECT o.edge_id, o.fraction AS start_fraction, o.next_fraction AS end_fraction, o.rn
    FROM ord o
    WHERE o.next_fraction IS NOT NULL AND o.next_fraction > o.fraction + in_fraction_merge_epsilon;

    -- Insert split segments
    INSERT INTO firearea.network_edges_split(
        usgs_site, original_edge_id, segment_index, start_fraction, end_fraction,
        geom_5070, length_m, cost, reverse_cost
    )
    SELECT v_site_lower, s.edge_id AS original_edge_id, s.rn AS segment_index,
           s.start_fraction, s.end_fraction,
           ST_LineSubstring(e.geom_5070, s.start_fraction, s.end_fraction) AS geom_5070,
           ST_Length(ST_LineSubstring(e.geom_5070, s.start_fraction, s.end_fraction)) AS length_m,
           ST_Length(ST_LineSubstring(e.geom_5070, s.start_fraction, s.end_fraction)) AS cost,
           ST_Length(ST_LineSubstring(e.geom_5070, s.start_fraction, s.end_fraction)) AS reverse_cost
    FROM tmp_segments s
    JOIN tmp_site_edges e ON e.edge_id = s.edge_id
    WHERE NOT ST_IsEmpty(ST_LineSubstring(e.geom_5070, s.start_fraction, s.end_fraction));

    -- Rebuild topology on split table (global); cleans stale source/target
    PERFORM pgr_createTopology(
        'firearea.network_edges_split',
        in_network_tolerance,
        'geom_5070',
        'edge_id',
        'source',
        'target',
        rows_where := 'true',
        clean := false
    );

    -- Build vertices table (drop & recreate)
    PERFORM 1 FROM pg_class WHERE relname='network_edges_vertices_pgr_split' AND relnamespace='firearea'::regnamespace;
    IF FOUND THEN
        EXECUTE 'DROP TABLE firearea.network_edges_vertices_pgr_split CASCADE';
    END IF;
    -- pgr_createTopology already created a *_vertices_pgr table if absent; rename if needed
    -- We create explicitly for clarity.
    CREATE TABLE firearea.network_edges_vertices_pgr_split AS
        SELECT * FROM firearea.network_edges_split_vertices_pgr;
    -- Add index for performance
    EXECUTE 'CREATE INDEX IF NOT EXISTS network_edges_vertices_pgr_split_geom_idx ON firearea.network_edges_vertices_pgr_split USING GIST(the_geom)';

    -- Refresh fire_event_vertices for site (multi-vertex). For each intersection
    -- point we snap to nearest split vertex. Then deduplicate on (event,vertex).
    DELETE FROM firearea.fire_event_vertices WHERE lower(usgs_site)=v_site_lower;
    WITH raw_pts AS (
        SELECT event_id, ig_date, pt_geom
        FROM tmp_fire_intersections
        UNION ALL
        -- Include midpoint fallback geometries used for containment-only events
        SELECT f.event_id, f.ig_date, ST_LineInterpolatePoint(ne.geom_5070,0.5) AS pt_geom
        FROM firearea.fires_catchments f
        JOIN firearea.network_edges_split ne ON lower(ne.usgs_site)=v_site_lower
        WHERE lower(f.usgs_site)=v_site_lower
          AND f.event_id NOT IN (SELECT DISTINCT event_id FROM tmp_fire_intersections)
        GROUP BY f.event_id, f.ig_date, ne.geom_5070
    ), snapped AS (
        SELECT
            v_site_lower AS usgs_site,
            r.event_id,
            r.ig_date,
            (
                SELECT v.id
                FROM firearea.network_edges_vertices_pgr_split v
                JOIN firearea.network_edges_split e ON (v.id = e.source OR v.id = e.target)
                WHERE lower(e.usgs_site)=v_site_lower
                ORDER BY v.the_geom <-> r.pt_geom
                LIMIT 1
            ) AS vertex_id,
            r.pt_geom AS geom_5070
        FROM raw_pts r
    ), ranked AS (
        SELECT *, row_number() OVER (PARTITION BY usgs_site, event_id, vertex_id ORDER BY event_id) AS rn
        FROM snapped
        WHERE vertex_id IS NOT NULL
    )
    INSERT INTO firearea.fire_event_vertices(usgs_site, event_id, ig_date, vertex_id, geom_5070)
    SELECT usgs_site, event_id, ig_date, vertex_id, geom_5070
    FROM ranked
    WHERE rn=1;  -- one geometry exemplar per (event,vertex)

    RAISE NOTICE '[split_topology] site=% edges_split=% event_vertices(raw_unique)=% events=%', v_site_lower,
        (SELECT count(*) FROM firearea.network_edges_split WHERE lower(usgs_site)=v_site_lower),
        (SELECT count(*) FROM firearea.fire_event_vertices WHERE lower(usgs_site)=v_site_lower),
        (SELECT count(DISTINCT event_id) FROM firearea.fire_event_vertices WHERE lower(usgs_site)=v_site_lower);
END;
$$;

--------------------------------------------------------------------------------
-- (C) Distance Function Using Split Topology (Vertex Targets)
-- Simplified: no pour point offset, no mid-edge fractions. Distances are
-- shortest path costs between pour point vertex & event vertices.
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION firearea.fn_compute_site_fire_distances_vertex(
    in_usgs_site text,
    in_include_unreachable boolean DEFAULT true
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_site_lower text := lower(in_usgs_site);
    v_start_vertex_id bigint;
    v_pp_geom geometry(Point,5070);
    v_run_id text := to_char(clock_timestamp(),'YYYYMMDDHH24MISSMS') || '_' || lpad((floor(random()*1000000))::int::text,6,'0');
BEGIN
    IF NOT EXISTS (SELECT 1 FROM firearea.network_edges_split LIMIT 1) THEN
        RAISE EXCEPTION 'network_edges_split empty; run fn_prepare_site_split_topology first.'; END IF;
    IF NOT EXISTS (SELECT 1 FROM firearea.fire_event_vertices WHERE lower(usgs_site)=v_site_lower) THEN
        -- Attempt an automatic preparation for convenience (may be expensive: rebuilds split table)
        PERFORM firearea.fn_prepare_site_split_topology(v_site_lower);
        -- Re-check after preparation
        IF NOT EXISTS (SELECT 1 FROM firearea.fire_event_vertices WHERE lower(usgs_site)=v_site_lower) THEN
            RAISE EXCEPTION 'fire_event_vertices still empty for site % after attempted auto prep; verify fires_catchments and pour_point_snaps.', v_site_lower;
        END IF;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM firearea.pour_point_snaps WHERE lower(usgs_site)=v_site_lower) THEN
        RAISE EXCEPTION 'pour_point_snaps missing for site %', v_site_lower; END IF;

    -- Schema guard: ensure chosen_vertex_id column exists (migration safety)
    PERFORM 1 FROM information_schema.columns
     WHERE table_schema='firearea' AND table_name='site_fire_distances' AND column_name='chosen_vertex_id';
    IF NOT FOUND THEN
        BEGIN
            EXECUTE 'ALTER TABLE firearea.site_fire_distances ADD COLUMN chosen_vertex_id bigint';
        EXCEPTION WHEN duplicate_column THEN
            NULL; -- race or already added
        END;
    END IF;

    -- Derive pour point geometry (projected) & start vertex
    SELECT snap_geom INTO v_pp_geom
    FROM firearea.pour_point_snaps
    WHERE lower(usgs_site)=v_site_lower
    ORDER BY snap_distance_m, identifier LIMIT 1;
    IF v_pp_geom IS NULL THEN
        RAISE EXCEPTION 'Could not find pour point geometry for site %', v_site_lower; END IF;

    SELECT v.id INTO v_start_vertex_id
    FROM firearea.network_edges_vertices_pgr_split v
    JOIN firearea.network_edges_split e ON (v.id = e.source OR v.id = e.target)
    WHERE lower(e.usgs_site)=v_site_lower
    ORDER BY v.the_geom <-> v_pp_geom LIMIT 1;  -- restrict start vertex to site's subgraph
    IF v_start_vertex_id IS NULL THEN
        RAISE EXCEPTION 'Failed to locate start vertex for site %', v_site_lower; END IF;

    -- Clean previous results for site (single logical pour point assumption)
    DELETE FROM firearea.site_fire_distances WHERE usgs_site = v_site_lower;

    -- Run multi-target Dijkstra
    DROP TABLE IF EXISTS _vx_paths_raw;
    CREATE TEMP TABLE _vx_paths_raw ON COMMIT DROP AS
    SELECT * FROM pgr_dijkstra(
        format($q$SELECT edge_id AS id, source, target, cost, reverse_cost FROM firearea.network_edges_split WHERE lower(usgs_site)=%L$q$, v_site_lower),
        v_start_vertex_id,
        ARRAY(SELECT DISTINCT vertex_id FROM firearea.fire_event_vertices WHERE lower(usgs_site)=v_site_lower),
        false
    );
    
    -- Derive minimal distance per reachable vertex (distinct on node)
    DROP TABLE IF EXISTS _vx_vertex_dist;
    CREATE TEMP TABLE _vx_vertex_dist ON COMMIT DROP AS
    SELECT DISTINCT ON (node) node AS vertex_id, agg_cost AS dist
    FROM _vx_paths_raw
    ORDER BY node, agg_cost;

    -- Insert reachable (aggregate MIN distance across vertices per event)
    WITH per_vertex AS (
        SELECT fev.event_id, fev.ig_date, fev.vertex_id, vd.dist
        FROM firearea.fire_event_vertices fev
        JOIN _vx_vertex_dist vd ON vd.vertex_id = fev.vertex_id
        WHERE lower(fev.usgs_site)=v_site_lower
    ), ranked AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY dist) AS rn
        FROM per_vertex
    )
    INSERT INTO firearea.site_fire_distances(
        usgs_site, pour_point_identifier, pour_point_comid,
        event_id, ig_date, distance_m, path_edge_ids, status, message, chosen_vertex_id
    )
    SELECT v_site_lower,
           (SELECT identifier FROM firearea.pour_point_snaps WHERE lower(usgs_site)=v_site_lower ORDER BY snap_distance_m, identifier LIMIT 1) AS pour_point_identifier,
           (SELECT comid FROM firearea.pour_point_snaps WHERE lower(usgs_site)=v_site_lower ORDER BY snap_distance_m, identifier LIMIT 1) AS pour_point_comid,
           r.event_id, r.ig_date, r.dist AS distance_m, NULL, 'ok', 'split_vertex', r.vertex_id
    FROM ranked r
    WHERE r.rn=1;

    -- Unreachable events: all vertices missed
    IF in_include_unreachable THEN
        INSERT INTO firearea.site_fire_distances(
            usgs_site, pour_point_identifier, pour_point_comid,
            event_id, ig_date, status, message, chosen_vertex_id
        )
        SELECT v_site_lower,
               (SELECT identifier FROM firearea.pour_point_snaps WHERE lower(usgs_site)=v_site_lower ORDER BY snap_distance_m, identifier LIMIT 1),
               (SELECT comid FROM firearea.pour_point_snaps WHERE lower(usgs_site)=v_site_lower ORDER BY snap_distance_m, identifier LIMIT 1),
               x.event_id, x.ig_date, 'unreachable', 'split_vertex_no_path', NULL
        FROM (
            SELECT fev.event_id, fev.ig_date,
                   MAX(CASE WHEN vd.vertex_id IS NOT NULL THEN 1 ELSE 0 END) AS any_reached
            FROM firearea.fire_event_vertices fev
            LEFT JOIN _vx_vertex_dist vd ON vd.vertex_id = fev.vertex_id
            WHERE lower(fev.usgs_site)=v_site_lower
            GROUP BY fev.event_id, fev.ig_date
        ) x
        WHERE x.any_reached = 0;
    END IF;

    -- Log summary
    INSERT INTO firearea.site_fire_distance_run_log(run_id, usgs_site, step, detail)
    VALUES (v_run_id, v_site_lower, 'done_vertex', format('reachable=%s unreachable=%s',
        (SELECT count(*) FROM firearea.site_fire_distances WHERE usgs_site=v_site_lower AND status='ok'),
        (SELECT count(*) FROM firearea.site_fire_distances WHERE usgs_site=v_site_lower AND status='unreachable')));
END;
$$;

-- Usage Examples:
--   SELECT firearea.fn_prepare_site_split_topology('syca');
--   SELECT firearea.fn_compute_site_fire_distances_vertex('syca');
--   -- Compare with existing lite implementation distances.

--   SELECT firearea.fn_prepare_site_split_topology('sbc_lter_rat');
--   SELECT firearea.fn_compute_site_fire_distances_vertex('sbc_lter_rat');
--   -- Compare with existing lite implementation distances.

--------------------------------------------------------------------------------
-- END PERMANENT SPLIT TOPOLOGY SECTION
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- UNIFIED DISTANCE FUNCTION
-- Provides a single entry point to compute distances using either the legacy
-- lite (fraction + offset) approach or the split-vertex approach if prepared.
-- Adds zero-distance detection (pour point inside / within tolerance of fire).
-- PARAMETERS:
--   in_usgs_site            Site identifier
--   in_mode                 'auto' | 'force_lite' | 'force_split'
--   in_include_unreachable  Include unreachable events in output
--   in_touch_tolerance_m    Distance (meters, SRID 5070) for pour point proximity to fire polygon
-- NOTES:
--   * In 'auto', split mode is used only if prerequisite split tables populated for site.
--   * Zero-distance events get distance_m=0, status='ok', is_pour_point_touch=true,
--     message overwritten with '<mode>_zero_touch'.
--------------------------------------------------------------------------------
DROP FUNCTION IF EXISTS firearea.fn_compute_site_fire_distances(text, text, boolean, double precision);
CREATE OR REPLACE FUNCTION firearea.fn_compute_site_fire_distances(
    in_usgs_site text,
    in_mode text DEFAULT 'auto',
    in_include_unreachable boolean DEFAULT true,
    in_touch_tolerance_m double precision DEFAULT 5.0
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_site_lower text := lower(in_usgs_site);
    v_mode text := lower(in_mode);
    v_has_split boolean;
    v_pp_identifier text;
    v_pp_comid text;
    v_pp_geom geometry(Point,5070);
    v_msg_prefix text;
    v_run_id text := to_char(clock_timestamp(),'YYYYMMDDHH24MISSMS') || '_' || lpad((floor(random()*1000000))::int::text,6,'0');
BEGIN
    -- Basic prerequisites common to both modes
    IF NOT EXISTS (SELECT 1 FROM firearea.pour_point_snaps WHERE lower(usgs_site)=v_site_lower) THEN
        RAISE EXCEPTION 'No pour point snaps for site %', v_site_lower; END IF;
    IF NOT EXISTS (SELECT 1 FROM firearea.fires_catchments WHERE lower(usgs_site)=v_site_lower) THEN
        RAISE EXCEPTION 'No fire polygons for site %', v_site_lower; END IF;

    -- Schema guard (idempotent) for chosen_vertex_id so downstream inserts don't fail
    PERFORM 1 FROM information_schema.columns
      WHERE table_schema='firearea' AND table_name='site_fire_distances' AND column_name='chosen_vertex_id';
    IF NOT FOUND THEN
        BEGIN
            EXECUTE 'ALTER TABLE firearea.site_fire_distances ADD COLUMN chosen_vertex_id bigint';
        EXCEPTION WHEN duplicate_column THEN
            NULL;
        END;
    END IF;

    -- Pour point selection
    SELECT identifier, comid, snap_geom
      INTO v_pp_identifier, v_pp_comid, v_pp_geom
    FROM firearea.pour_point_snaps
    WHERE lower(usgs_site)=v_site_lower
    ORDER BY snap_distance_m, identifier
    LIMIT 1;
    IF v_pp_identifier IS NULL THEN
        RAISE EXCEPTION 'Failed to select pour point for site %', v_site_lower; END IF;

    -- Detect split readiness
    v_has_split := (
        EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='firearea' AND table_name='network_edges_split') AND
        EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='firearea' AND table_name='network_edges_vertices_pgr_split') AND
        EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='firearea' AND table_name='fire_event_vertices') AND
        EXISTS (SELECT 1 FROM firearea.network_edges_split WHERE lower(usgs_site)=v_site_lower) AND
        EXISTS (SELECT 1 FROM firearea.fire_event_vertices WHERE lower(usgs_site)=v_site_lower)
    );

    IF v_mode NOT IN ('auto','force_lite','force_split') THEN
        RAISE EXCEPTION 'Invalid mode % (use auto|force_lite|force_split)', in_mode; END IF;

    IF v_mode='auto' THEN
        v_mode := CASE WHEN v_has_split THEN 'force_split' ELSE 'force_lite' END;
    END IF;

    IF v_mode='force_split' AND NOT v_has_split THEN
        RAISE EXCEPTION 'Split mode requested but split topology not prepared for site %', v_site_lower; END IF;
    IF v_mode='force_lite' AND NOT EXISTS (SELECT 1 FROM firearea.network_edges WHERE lower(usgs_site)=v_site_lower) THEN
        RAISE EXCEPTION 'Lite mode requested but base network missing for site %', v_site_lower; END IF;

    INSERT INTO firearea.site_fire_distance_run_log(run_id, usgs_site, step, detail)
    VALUES (v_run_id, v_site_lower, 'unified_start', format('mode=%s has_split=%s', v_mode, v_has_split));

    IF v_mode='force_split' THEN
        -- Use existing vertex-based function (already wipes previous rows for site)
        PERFORM firearea.fn_compute_site_fire_distances_vertex(v_site_lower, in_include_unreachable);
        v_msg_prefix := 'split';
    ELSE
        -- Use existing lite function
        PERFORM firearea.fn_compute_site_fire_distances_lite(v_site_lower, 60.0, in_include_unreachable);
        v_msg_prefix := 'lite';
    END IF;

    -- Zero-distance detection (pour point inside or within tolerance)
    DROP TABLE IF EXISTS _unified_zero_events;
    CREATE TEMP TABLE _unified_zero_events AS
    WITH fires AS (
        SELECT event_id, ig_date, ST_Transform(geometry,5070) AS geom_5070
        FROM firearea.fires_catchments
        WHERE lower(usgs_site)=v_site_lower
    )
    SELECT f.event_id, f.ig_date
    FROM fires f
    WHERE ST_Covers(f.geom_5070, v_pp_geom)  -- includes boundary
       OR ST_Intersects(f.geom_5070, v_pp_geom)
       OR (in_touch_tolerance_m > 0 AND ST_DWithin(f.geom_5070, v_pp_geom, in_touch_tolerance_m));

    -- Update existing rows (reachable or unreachable) to zero distance
    UPDATE firearea.site_fire_distances d
    SET distance_m = 0,
        status = 'ok',
        is_pour_point_touch = true,
        message = v_msg_prefix || '_zero_touch'
    FROM _unified_zero_events z
    WHERE d.usgs_site = v_site_lower AND d.event_id = z.event_id;

    -- Insert any zero events missing from current rows
    INSERT INTO firearea.site_fire_distances(
        usgs_site, pour_point_identifier, pour_point_comid,
        event_id, ig_date, distance_m, path_edge_ids, status, message, is_pour_point_touch
    )
    SELECT v_site_lower, v_pp_identifier, v_pp_comid,
           z.event_id, z.ig_date, 0, NULL, 'ok', v_msg_prefix || '_zero_touch', true
    FROM _unified_zero_events z
    WHERE NOT EXISTS (
        SELECT 1 FROM firearea.site_fire_distances d
        WHERE d.usgs_site=v_site_lower AND d.event_id=z.event_id
    );

    INSERT INTO firearea.site_fire_distance_run_log(run_id, usgs_site, step, detail)
    VALUES (v_run_id, v_site_lower, 'unified_done', format('mode=%s zero_touch=%s total_ok=%s unreachable=%s',
        v_mode,
        (SELECT count(*) FROM firearea.site_fire_distances WHERE usgs_site=v_site_lower AND is_pour_point_touch),
        (SELECT count(*) FROM firearea.site_fire_distances WHERE usgs_site=v_site_lower AND status='ok'),
        (SELECT count(*) FROM firearea.site_fire_distances WHERE usgs_site=v_site_lower AND status='unreachable')));
END;
$$;

-- Convenience wrappers (optional future addition): could alias old names to unified.
-- For now, users call: SELECT firearea.fn_compute_site_fire_distances('syca');
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- SPLIT MODE PATH EXPORT: fn_fire_event_paths_split
-- Purpose:
--   Reconstruct shortest path geometries (split topology) from pour point
--   vertex to the chosen event vertex (chosen_vertex_id) for each fire event
--   with status='ok' in site_fire_distances. Includes zero-touch events with
--   NULL path (distance 0) and their chosen vertex (possibly NULL).
-- Use cases:
--   * GIS export via ogr2ogr (analogous to lite path export function)
--   * QA comparisons / visualization
-- Requirements:
--   * fn_prepare_site_split_topology (or bulk prep) already executed
--   * Distances computed via split mode (vertex or unified selecting split)
-- Return Columns:
--   event_id, ig_date, distance_m, pour_point_identifier, start_vertex_id,
--   pour_point_geom, chosen_vertex_id, chosen_vertex_geom, is_pour_point_touch,
--   path_edge_count, path_edge_ids, path_geom
--------------------------------------------------------------------------------
DROP FUNCTION IF EXISTS firearea.fn_fire_event_paths_split(text);
CREATE OR REPLACE FUNCTION firearea.fn_fire_event_paths_split(
    in_usgs_site text
) RETURNS TABLE(
    event_id text,
    ig_date date,
    distance_m double precision,
    pour_point_identifier text,
    start_vertex_id bigint,
    pour_point_geom geometry(Point,5070),
    chosen_vertex_id bigint,
    chosen_vertex_geom geometry(Point,5070),
    is_pour_point_touch boolean,
    path_edge_count integer,
    path_edge_ids bigint[],
    path_geom geometry(LineString,5070)
) LANGUAGE plpgsql AS $$
DECLARE
    v_site_lower text := lower(in_usgs_site);
    v_pp_identifier text;
    v_pp_geom geometry(Point,5070);
    v_start_vertex_id bigint;
    v_has_rows boolean;
BEGIN
    -- Preconditions
    IF NOT EXISTS (SELECT 1 FROM firearea.network_edges_split LIMIT 1) THEN
        RAISE EXCEPTION 'network_edges_split empty; run split prep first.'; END IF;
    IF NOT EXISTS (SELECT 1 FROM firearea.network_edges_vertices_pgr_split LIMIT 1) THEN
        RAISE EXCEPTION 'network_edges_vertices_pgr_split missing; run split prep.'; END IF;
    IF NOT EXISTS (SELECT 1 FROM firearea.site_fire_distances WHERE lower(usgs_site)=v_site_lower) THEN
        RAISE EXCEPTION 'No site_fire_distances rows for site % (compute distances first)', v_site_lower; END IF;

    -- Schema guard for is_pour_point_touch (older deployments may lack it)
    PERFORM 1 FROM information_schema.columns
     WHERE table_schema='firearea' AND table_name='site_fire_distances' AND column_name='is_pour_point_touch';
    IF NOT FOUND THEN
        BEGIN
            EXECUTE 'ALTER TABLE firearea.site_fire_distances ADD COLUMN is_pour_point_touch boolean DEFAULT false';
        EXCEPTION WHEN duplicate_column THEN NULL; END;
    END IF;

    -- Pour point selection
    SELECT identifier, snap_geom INTO v_pp_identifier, v_pp_geom
    FROM firearea.pour_point_snaps
    WHERE lower(usgs_site)=v_site_lower
    ORDER BY snap_distance_m, identifier
    LIMIT 1;
    IF v_pp_identifier IS NULL THEN
        RAISE EXCEPTION 'Pour point not found for site %', v_site_lower; END IF;

    -- Start vertex (nearest split vertex)
    SELECT v.id INTO v_start_vertex_id
    FROM firearea.network_edges_vertices_pgr_split v
    JOIN firearea.network_edges_split e ON (v.id=e.source OR v.id=e.target)
    WHERE lower(e.usgs_site)=v_site_lower
    ORDER BY v.the_geom <-> v_pp_geom LIMIT 1;  -- restrict start vertex to site
    IF v_start_vertex_id IS NULL THEN
        RAISE EXCEPTION 'Start vertex not located (split vertices missing)'; END IF;

    -- Candidate events (only status ok) keep chosen_vertex_id / zero-touch flag
    DROP TABLE IF EXISTS _split_path_events;
    CREATE TEMP TABLE _split_path_events ON COMMIT DROP AS
    SELECT d.event_id, d.ig_date, d.distance_m, d.chosen_vertex_id,
        d.is_pour_point_touch
    FROM firearea.site_fire_distances d
    WHERE lower(d.usgs_site)=v_site_lower
      AND d.status='ok';

    SELECT COUNT(*)>0 INTO v_has_rows FROM _split_path_events;
    IF NOT v_has_rows THEN
        RETURN; -- nothing to return
    END IF;

    -- Non-zero events for which we need path reconstruction
    DROP TABLE IF EXISTS _split_path_targets;
    CREATE TEMP TABLE _split_path_targets ON COMMIT DROP AS
    -- Qualify chosen_vertex_id to avoid ambiguity with OUT table column variable
    SELECT DISTINCT e.chosen_vertex_id AS vertex_id
    FROM _split_path_events e
    WHERE e.chosen_vertex_id IS NOT NULL AND (e.distance_m IS NULL OR e.distance_m > 0);

    -- If there are path targets run Dijkstra
    IF EXISTS (SELECT 1 FROM _split_path_targets) THEN
        DROP TABLE IF EXISTS _split_paths_raw;
        CREATE TEMP TABLE _split_paths_raw ON COMMIT DROP AS
        SELECT * FROM pgr_dijkstra(
            format($q$SELECT edge_id AS id, source, target, cost, reverse_cost FROM firearea.network_edges_split WHERE lower(usgs_site)=%L$q$, v_site_lower),
            v_start_vertex_id,
            ARRAY(SELECT vertex_id FROM _split_path_targets),
            false
        );
    ELSE
        -- Create empty table with expected columns to simplify joins
        DROP TABLE IF EXISTS _split_paths_raw;
        CREATE TEMP TABLE _split_paths_raw(
            seq int, path_seq int, start_vid bigint, end_vid bigint, node bigint,
            edge bigint, cost double precision, agg_cost double precision
        ) ON COMMIT DROP;
    END IF;

    -- Aggregate edges per event
    RETURN QUERY
    WITH evt AS (
        SELECT e.event_id, e.ig_date, e.distance_m, e.chosen_vertex_id, e.is_pour_point_touch
        FROM _split_path_events e
    ), path_edges AS (
        SELECT p.end_vid AS vertex_id, p.seq, p.edge, p.agg_cost
        FROM _split_paths_raw p
        WHERE p.edge <> -1 AND p.edge IS NOT NULL
    ), edges_join AS (
        SELECT pe.vertex_id, pe.seq, pe.edge, ne.geom_5070
        FROM path_edges pe
        JOIN firearea.network_edges_split ne ON ne.edge_id = pe.edge
    ), edge_aggs AS (
        SELECT v.event_id, v.ig_date, v.distance_m, v.chosen_vertex_id, v.is_pour_point_touch,
            COUNT(ej.edge)::int AS path_edge_count,
               CASE WHEN COUNT(ej.edge)=0 THEN ARRAY[]::bigint[]
                    ELSE ARRAY_AGG(ej.edge ORDER BY ej.seq) END AS path_edge_ids,
               CASE WHEN COUNT(ej.edge)=0 THEN NULL
                    ELSE ST_LineMerge(ST_Collect(ej.geom_5070)) END AS path_geom
        FROM evt v
        LEFT JOIN edges_join ej ON ej.vertex_id = v.chosen_vertex_id
        GROUP BY v.event_id, v.ig_date, v.distance_m, v.chosen_vertex_id, v.is_pour_point_touch
    ), chosen_geom AS (
        SELECT d.event_id, d.chosen_vertex_id, v.the_geom::geometry(Point,5070) AS chosen_vertex_geom
        FROM firearea.site_fire_distances d
        LEFT JOIN firearea.network_edges_vertices_pgr_split v ON v.id = d.chosen_vertex_id
        WHERE lower(d.usgs_site)=v_site_lower AND d.status='ok'
    )
    SELECT ea.event_id::text,
           ea.ig_date,
           ea.distance_m,
           v_pp_identifier::text AS pour_point_identifier,
           v_start_vertex_id,
           v_pp_geom AS pour_point_geom,
           ea.chosen_vertex_id,
           cg.chosen_vertex_geom,
           ea.is_pour_point_touch,
           ea.path_edge_count,
           ea.path_edge_ids,
           ea.path_geom::geometry(LineString,5070)
    FROM edge_aggs ea
    LEFT JOIN chosen_geom cg USING (event_id, chosen_vertex_id)
    ORDER BY ea.distance_m NULLS LAST, ea.event_id;
END;
$$;

-- Example ogr2ogr export (paths):
-- ogr2ogr -f GeoJSON syca_split_paths.geojson \
--   PG:"host=HOST dbname=DB user=USER password=PASS" \
--   -sql "SELECT event_id, ig_date, distance_m, path_geom FROM firearea.fn_fire_event_paths_split('syca')" \
--   -nln syca_split_paths
-- Example points (chosen vertices):
-- ogr2ogr -f GeoJSON syca_split_vertices.geojson \
--   PG:"host=HOST dbname=DB user=USER password=PASS" \
--   -sql "SELECT event_id, ig_date, chosen_vertex_geom FROM firearea.fn_fire_event_paths_split('syca') WHERE chosen_vertex_geom IS NOT NULL" \
--   -nln syca_split_vertices


--------------------------------------------------------------------------------
-- BULK PRECOMPUTE: fn_prepare_all_split_topology
-- Purpose:
--   Pre-split network edges and populate fire_event_vertices for ALL (or a
--   specified subset of) sites in one pass so downstream distance calls in
--   split mode become fast, avoiding repeated per-site rebuild cost.
-- Features:
--   * Optional site list filter (text[])
--   * Optional reuse of existing base pass-through edges (skip full rebuild)
--   * Metadata tracking per site (prepared_at, counts) for monitoring
-- Notes:
--   * This function intentionally does NOT drop/recreate the single-site
--     function; they can coexist.
--   * Safe to re-run; will replace split segments & fire_event_vertices for
--     processed sites.
--   * Assumes network_edges is authoritative. If that table changes, run with
--     in_rebuild_base := true.
--------------------------------------------------------------------------------
DO $$
BEGIN
    IF to_regclass('firearea.split_topology_metadata') IS NULL THEN
        CREATE TABLE firearea.split_topology_metadata (
            usgs_site text PRIMARY KEY,
            prepared_at timestamptz NOT NULL,
            split_edge_count integer NOT NULL,
            event_vertex_count integer NOT NULL,
            notes text
        );
        CREATE INDEX split_topology_metadata_prepared_idx ON firearea.split_topology_metadata (prepared_at DESC);
    END IF;
END;$$;

CREATE OR REPLACE FUNCTION firearea.fn_prepare_all_split_topology(
    in_srid integer DEFAULT 5070,
    in_network_tolerance double precision DEFAULT 0.5,
    in_vertex_snap_tolerance double precision DEFAULT 0.25,
    in_fraction_merge_epsilon double precision DEFAULT 1e-7,
    in_sites text[] DEFAULT NULL,              -- restrict to these (case-insensitive)
    in_rebuild_base boolean DEFAULT true       -- if true rebuild pass-through base first
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_geom_col text := format('geom_%s', in_srid);
    v_site text;
    v_site_lower text;
    v_pp_edge_id bigint;
    v_pp_fraction double precision;
    v_edge_pass_through_deleted int;
    v_edge_split_count int;
    v_event_vertex_count int;
    v_total_sites int;
    v_site_index int := 0;
BEGIN
    -- Preconditions
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='firearea' AND table_name='network_edges' AND column_name=v_geom_col
    ) THEN
        RAISE EXCEPTION 'Projected geom column % missing in network_edges; build topology first.', v_geom_col; END IF;
    IF NOT EXISTS (SELECT 1 FROM firearea.network_edges LIMIT 1) THEN
        RAISE EXCEPTION 'network_edges empty.'; END IF;

    -- Ensure target tables exist (reuse prior lazy DDL block logic)
    PERFORM 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
      WHERE n.nspname='firearea' AND c.relname='network_edges_split';
    IF NOT FOUND THEN
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
        CREATE INDEX network_edges_split_site_idx ON firearea.network_edges_split (usgs_site);
        CREATE INDEX network_edges_split_geom_idx ON firearea.network_edges_split USING GIST (geom_5070);
    END IF;
    PERFORM 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
      WHERE n.nspname='firearea' AND c.relname='fire_event_vertices';
    IF NOT FOUND THEN
        CREATE TABLE firearea.fire_event_vertices (
            usgs_site text NOT NULL,
            event_id varchar(254) NOT NULL,
            ig_date date,
            vertex_id bigint NOT NULL,
            geom_5070 geometry(Point,5070),
            PRIMARY KEY (usgs_site, event_id, vertex_id)
        );
        CREATE INDEX fire_event_vertices_vertex_idx ON firearea.fire_event_vertices (vertex_id);
        CREATE INDEX fire_event_vertices_event_idx  ON firearea.fire_event_vertices (usgs_site, event_id);
    END IF;

    -- Optionally rebuild base pass-through (one row per original edge)
    IF in_rebuild_base THEN
        TRUNCATE firearea.network_edges_split RESTART IDENTITY;
        EXECUTE format('INSERT INTO firearea.network_edges_split(
              usgs_site, original_edge_id, segment_index,
              start_fraction, end_fraction, geom_5070,
              length_m, cost, reverse_cost)
             SELECT ne.usgs_site, ne.edge_id, NULL, NULL, NULL, ne.%s,
                    ne.length_m, ne.length_m, ne.length_m
             FROM firearea.network_edges ne', v_geom_col);
    END IF;

    -- Determine site list
    WITH base_sites AS (
        SELECT DISTINCT lower(p.usgs_site) AS usgs_site
        FROM firearea.pour_point_snaps p
        WHERE EXISTS (
            SELECT 1 FROM firearea.network_edges ne WHERE lower(ne.usgs_site)=lower(p.usgs_site)
        )
    )
    SELECT count(*) INTO v_total_sites FROM base_sites
    WHERE in_sites IS NULL OR lower(usgs_site)=ANY(SELECT lower(s) FROM unnest(in_sites) s);

    FOR v_site IN
        SELECT usgs_site FROM (
            SELECT DISTINCT lower(p.usgs_site) AS usgs_site
            FROM firearea.pour_point_snaps p
            WHERE EXISTS (SELECT 1 FROM firearea.network_edges ne WHERE lower(ne.usgs_site)=lower(p.usgs_site))
        ) s
        WHERE in_sites IS NULL OR usgs_site = ANY(SELECT lower(x) FROM unnest(in_sites) x)
        ORDER BY usgs_site
    LOOP
        v_site_lower := v_site;
        v_site_index := v_site_index + 1;

        -- Skip if no fires (still split pour point edge if we want consistency)
        IF NOT EXISTS (SELECT 1 FROM firearea.pour_point_snaps WHERE lower(usgs_site)=v_site_lower) THEN
            RAISE NOTICE '[all_split] (%/% %) skipping (no pour point snaps)', v_site_index, v_total_sites, v_site_lower;
            CONTINUE;
        END IF;

        -- Pour point selection
        SELECT snap_edge_id, snap_fraction INTO v_pp_edge_id, v_pp_fraction
        FROM firearea.pour_point_snaps
        WHERE lower(usgs_site)=v_site_lower
        ORDER BY snap_distance_m, identifier LIMIT 1;
        IF v_pp_edge_id IS NULL THEN
            RAISE NOTICE '[all_split] (%/% %) skipping (no pour point edge)', v_site_index, v_total_sites, v_site_lower;
            CONTINUE;
        END IF;

        -- Remove prior split segments for this site (retain other sites)
        DELETE FROM firearea.network_edges_split
        WHERE lower(usgs_site)=v_site_lower;

        -- Site edges temp
        DROP TABLE IF EXISTS tmp_all_site_edges;
        CREATE TEMP TABLE tmp_all_site_edges AS
        SELECT edge_id, usgs_site, geom_5070, length_m
        FROM firearea.network_edges
        WHERE lower(usgs_site)=v_site_lower;

        -- Fire boundary intersections (can be zero)
        DROP TABLE IF EXISTS tmp_all_fire_intersections;
        CREATE TEMP TABLE tmp_all_fire_intersections AS
        WITH fire_poly AS (
            SELECT event_id, ig_date,
                   CASE WHEN GeometryType(geometry) LIKE 'Multi%'
                            THEN ST_CollectionExtract(geometry,3)
                        ELSE geometry END AS geom
            FROM firearea.fires_catchments
            WHERE lower(usgs_site)=v_site_lower
        ), fire_5070 AS (
            SELECT event_id, ig_date, ST_Transform(geom, in_srid) AS geom_5070 FROM fire_poly
        ), raw_int AS (
            SELECT f.event_id, f.ig_date, e.edge_id,
                   (ST_Dump(ST_Intersection(ST_Boundary(f.geom_5070), e.geom_5070))).geom AS ig
            FROM fire_5070 f
            JOIN tmp_all_site_edges e ON ST_Intersects(f.geom_5070, e.geom_5070)
            WHERE ST_Intersects(ST_Boundary(f.geom_5070), e.geom_5070)
        ), pts AS (
            SELECT event_id, ig_date, edge_id, ig AS pt FROM raw_int WHERE GeometryType(ig) IN ('POINT','MULTIPOINT')
        ), line_mid AS (
            SELECT event_id, ig_date, edge_id, ST_LineInterpolatePoint(ST_LineMerge(ig),0.5) AS pt
            FROM raw_int WHERE GeometryType(ig) IN ('LINESTRING','MULTILINESTRING')
        ), all_pts AS (
            SELECT * FROM pts UNION ALL SELECT * FROM line_mid
        )
        SELECT event_id, ig_date, edge_id,
               ST_LineLocatePoint(e.geom_5070, ST_Force2D(pt)) AS fraction,
               ST_Force2D(pt) AS pt_geom
        FROM all_pts ap
        JOIN tmp_all_site_edges e USING(edge_id)
        WHERE pt IS NOT NULL;

        -- Containment midpoint fallback
        INSERT INTO tmp_all_fire_intersections(event_id, ig_date, edge_id, fraction, pt_geom)
        SELECT f.event_id, f.ig_date, e.edge_id, 0.5, ST_LineInterpolatePoint(e.geom_5070,0.5)
        FROM firearea.fires_catchments f
        JOIN tmp_all_site_edges e ON ST_Contains(ST_Transform(f.geometry,in_srid), e.geom_5070)
        WHERE lower(f.usgs_site)=v_site_lower
          AND f.event_id NOT IN (SELECT DISTINCT event_id FROM tmp_all_fire_intersections);

        -- Fractions union with pour point
        DROP TABLE IF EXISTS tmp_all_edge_fractions_raw;
        CREATE TEMP TABLE tmp_all_edge_fractions_raw AS
        SELECT edge_id, fraction FROM tmp_all_fire_intersections
        UNION ALL SELECT v_pp_edge_id AS edge_id, v_pp_fraction AS fraction;

        DELETE FROM tmp_all_edge_fractions_raw
        WHERE edge_id = v_pp_edge_id AND (fraction <= in_fraction_merge_epsilon OR (1 - fraction) <= in_fraction_merge_epsilon);

        DROP TABLE IF EXISTS tmp_all_edge_fractions;
        CREATE TEMP TABLE tmp_all_edge_fractions AS
        WITH base AS (
            SELECT edge_id, 0::double precision AS fraction FROM tmp_all_site_edges
            UNION ALL SELECT edge_id, 1::double precision FROM tmp_all_site_edges
            UNION ALL SELECT edge_id, fraction FROM tmp_all_edge_fractions_raw
        ), dedup AS (
            SELECT edge_id, ROUND(fraction::numeric,7) AS fraction FROM base GROUP BY edge_id, ROUND(fraction::numeric,7)
        )
        SELECT * FROM dedup;

        DROP TABLE IF EXISTS tmp_all_segments;
        CREATE TEMP TABLE tmp_all_segments AS
        WITH ord AS (
            SELECT ef.edge_id, ef.fraction,
                   LEAD(ef.fraction) OVER (PARTITION BY ef.edge_id ORDER BY ef.fraction) AS next_fraction,
                   ROW_NUMBER() OVER (PARTITION BY ef.edge_id ORDER BY ef.fraction) AS rn
            FROM tmp_all_edge_fractions ef
        )
        SELECT edge_id, fraction AS start_fraction, next_fraction AS end_fraction, rn
        FROM ord
        WHERE next_fraction IS NOT NULL AND next_fraction > fraction + in_fraction_merge_epsilon;

        -- Insert split segments for site
        INSERT INTO firearea.network_edges_split(
            usgs_site, original_edge_id, segment_index, start_fraction, end_fraction,
            geom_5070, length_m, cost, reverse_cost
        )
        SELECT v_site_lower, s.edge_id, s.rn, s.start_fraction, s.end_fraction,
               ST_LineSubstring(e.geom_5070, s.start_fraction, s.end_fraction) AS geom_5070,
               ST_Length(ST_LineSubstring(e.geom_5070, s.start_fraction, s.end_fraction)),
               ST_Length(ST_LineSubstring(e.geom_5070, s.start_fraction, s.end_fraction)),
               ST_Length(ST_LineSubstring(e.geom_5070, s.start_fraction, s.end_fraction))
        FROM tmp_all_segments s
        JOIN tmp_all_site_edges e ON e.edge_id = s.edge_id
        WHERE NOT ST_IsEmpty(ST_LineSubstring(e.geom_5070, s.start_fraction, s.end_fraction));

        -- Rebuild topology for just inserted site's edges: cheaper to re-run full global pgr_createTopology
        -- only after all sites IF rebuilding base. Simplicity: run once after loop.

        -- Refresh event vertices for site
        DELETE FROM firearea.fire_event_vertices WHERE lower(usgs_site)=v_site_lower;
        WITH raw_pts AS (
            SELECT event_id, ig_date, pt_geom FROM tmp_all_fire_intersections
            UNION ALL
            SELECT f.event_id, f.ig_date, ST_LineInterpolatePoint(ne.geom_5070,0.5) AS pt_geom
            FROM firearea.fires_catchments f
            JOIN firearea.network_edges_split ne ON lower(ne.usgs_site)=v_site_lower
            WHERE lower(f.usgs_site)=v_site_lower
              AND f.event_id NOT IN (SELECT DISTINCT event_id FROM tmp_all_fire_intersections)
            GROUP BY f.event_id, f.ig_date, ne.geom_5070
        ), snapped AS (
            SELECT v_site_lower AS usgs_site, r.event_id, r.ig_date,
                (
                    SELECT v.id FROM firearea.network_edges_split_vertices_pgr v
                    JOIN firearea.network_edges_split e ON (v.id=e.source OR v.id=e.target)
                    WHERE lower(e.usgs_site)=v_site_lower
                    ORDER BY v.the_geom <-> r.pt_geom LIMIT 1
                ) AS vertex_id,
                   r.pt_geom AS geom_5070
            FROM raw_pts r
        ), ranked AS (
            SELECT *, row_number() OVER (PARTITION BY usgs_site, event_id, vertex_id ORDER BY event_id) AS rn
            FROM snapped WHERE vertex_id IS NOT NULL
        )
        INSERT INTO firearea.fire_event_vertices(usgs_site, event_id, ig_date, vertex_id, geom_5070)
        SELECT usgs_site, event_id, ig_date, vertex_id, geom_5070
        FROM ranked WHERE rn=1;

        -- Counts for metadata (source/target not yet assigned until topology build below; lengths ok)
        SELECT count(*) INTO v_edge_split_count FROM firearea.network_edges_split WHERE lower(usgs_site)=v_site_lower;
        SELECT count(*) INTO v_event_vertex_count FROM firearea.fire_event_vertices WHERE lower(usgs_site)=v_site_lower;

        INSERT INTO firearea.split_topology_metadata(usgs_site, prepared_at, split_edge_count, event_vertex_count, notes)
        VALUES (v_site_lower, now(), v_edge_split_count, v_event_vertex_count, 'pre-topology build')
        ON CONFLICT (usgs_site) DO UPDATE SET
            prepared_at = EXCLUDED.prepared_at,
            split_edge_count = EXCLUDED.split_edge_count,
            event_vertex_count = EXCLUDED.event_vertex_count,
            notes = EXCLUDED.notes;

        RAISE NOTICE '[all_split] (%/% %) edges_split=% event_vertices=%',
            v_site_index, v_total_sites, v_site_lower, v_edge_split_count, v_event_vertex_count;
    END LOOP;

    -- (Re)build topology (global) once at end for all accumulated segments
    PERFORM pgr_createTopology(
        'firearea.network_edges_split',
        in_network_tolerance,
        'geom_5070',
        'edge_id',
        'source',
        'target',
        rows_where := 'true',
        clean := false
    );

    -- Recreate vertices table from pgr output for split (global refresh)
    PERFORM 1 FROM pg_class WHERE relname='network_edges_vertices_pgr_split' AND relnamespace='firearea'::regnamespace;
    IF FOUND THEN
        EXECUTE 'DROP TABLE firearea.network_edges_vertices_pgr_split CASCADE';
    END IF;
    CREATE TABLE firearea.network_edges_vertices_pgr_split AS
        SELECT * FROM firearea.network_edges_split_vertices_pgr;
    CREATE INDEX IF NOT EXISTS network_edges_vertices_pgr_split_geom_idx ON firearea.network_edges_vertices_pgr_split USING GIST(the_geom);

    -- Post-topology: event vertices may reference nearest vertices; resnap could refine, but optional.
    UPDATE firearea.split_topology_metadata SET notes='ready'
    WHERE notes='pre-topology build';

    RAISE NOTICE '[all_split] completed: sites=%', v_total_sites;
END;
$$;

-- Example usage:
-- SELECT firearea.fn_prepare_all_split_topology();                -- all sites full rebuild
-- SELECT firearea.fn_prepare_all_split_topology(in_sites=>ARRAY['syca','sbc_lter_rat']);
-- After bulk prep: SELECT firearea.fn_compute_site_fire_distances('syca','force_split');
