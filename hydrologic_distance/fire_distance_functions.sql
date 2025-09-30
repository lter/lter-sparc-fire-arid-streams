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
           vs.dist + e.length_m * e.fraction AS dist_via_source,
           vt.dist + e.length_m * (1 - e.fraction) AS dist_via_target
    FROM firearea._lite_event_edges e
    LEFT JOIN firearea._lite_vertex_sp vs ON vs.vertex_id = e.source
    LEFT JOIN firearea._lite_vertex_sp vt ON vt.vertex_id = e.target;

    -- Minimal distance per event (base + pour point offset) ----------------
    DROP TABLE IF EXISTS firearea._lite_event_min;
    CREATE UNLOGGED TABLE firearea._lite_event_min AS
    WITH per_edge AS (
        SELECT event_id, ig_date,
               CASE
                   WHEN dist_via_source IS NULL AND dist_via_target IS NULL THEN NULL
                   WHEN dist_via_source IS NULL THEN dist_via_target
                   WHEN dist_via_target IS NULL THEN dist_via_source
                   ELSE LEAST(dist_via_source, dist_via_target)
               END AS candidate_min
        FROM firearea._lite_event_candidate
    )
    SELECT event_id, ig_date, MIN(candidate_min) AS base_distance
    FROM per_edge
    GROUP BY event_id, ig_date;

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
        event_id, ig_date, distance_m, path_edge_ids, status, message
    )
    SELECT v_site_lower, v_pp_identifier, v_pp_comid,
           em.event_id, em.ig_date,
           (em.base_distance + v_offset_m) AS distance_m,
           NULL, 'ok', 'lite'
    FROM firearea._lite_event_min em
    WHERE em.base_distance IS NOT NULL;

    -- Unreachable events ----------------------------------------------------
    IF in_include_unreachable THEN
        INSERT INTO firearea.site_fire_distances(
            usgs_site, pour_point_identifier, pour_point_comid,
            event_id, ig_date, status, message
        )
        SELECT v_site_lower, v_pp_identifier, v_pp_comid,
               f.event_id, f.ig_date, 'unreachable', 'lite_no_path'
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

-- SELECT * FROM firearea.fn_compute_site_fire_distances_lite('syca', 60.0, true);
-- SELECT * FROM firearea.fn_debug_withpoints('syca');