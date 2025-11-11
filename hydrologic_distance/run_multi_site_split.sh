#!/usr/bin/env bash
# Purpose: Prepare split topology and compute distances (split mode) for a
#          dynamic multi-site list, ensuring each site runs in its OWN
#          transaction.
#
# Usage (explicit connection flags preferred):
#   ./run_multi_site_split.sh -h host -d mydb -U myuser [-W mypass]
#   (Optional: -p port if not 5432; defaults to 5432)
#
# Examples:
#   Basic run:
#     ./run_multi_site_split.sh -h localhost -d firedb -U analyst
#
#   With password & explicit port:
#     ./run_multi_site_split.sh -h db.example -p 5433 -d firedb -U analyst \
#       -W s3cr3t
#
#   Use a custom site list (file containing a SQL query returning usgs_site):
#     SITE_QUERY_FILE=./my_sites.sql \
#       ./run_multi_site_split.sh -h localhost -d firedb -U analyst
#
#   Inline site query override (one-liner):
#     SITE_QUERY="SELECT 'usgs-11060400'" \
#       ./run_multi_site_split.sh -h localhost -d firedb -U analyst
#
#   Dry run (show per-site transaction SQL, do not execute):
#     DRY_RUN=1 ./run_multi_site_split.sh -h localhost -d firedb -U analyst
#
#   Limit to first 5 sites (smoke test):
#     MAX_SITES=5 ./run_multi_site_split.sh -h localhost -d firedb -U analyst
#
#   Combine custom file + dry run + max:
#     DRY_RUN=1 MAX_SITES=3 SITE_QUERY_FILE=sites.sql \
#       ./run_multi_site_split.sh -h localhost -d firedb -U analyst
#
# Required flags:
#   -h host        Database host
#   -d database    Database name
#   -U user        Database user
# Optional:
#   -p port        Database port (default 5432)
#   -W password    Password (else rely on PGPASSWORD/.pgpass)
#   Environment flags (unchanged): SITE_QUERY_FILE, SITE_QUERY, DRY_RUN, MAX_SITES
#
# If flags are omitted we fall back to existing libpq env vars (PGHOST, etc.).
#
# Environment knobs:
#   SITE_QUERY_FILE   Path with SQL producing one usgs_site per row (optional)
#   SITE_QUERY        Inline SQL for sites (overrides default if set)
#   DRY_RUN=1         Show per-site SQL, do not execute
#   MAX_SITES=n       Process only first n sites (for smoke tests)
#   DEBUG=1           Verbose: echo SQL statements & psql errors
#
# All SQL and Bash kept <= 80 chars width for readability.

set -euo pipefail
if [[ -n "${DEBUG:-}" ]]; then set -x; fi
IFS=$'\n\t'

log() { printf '[multi] %s\n' "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }

# Early banner to confirm script actually starts (helps when user sees no output)
log "Script start pid=$$ file=$(readlink -f "$0" 2>/dev/null || echo "$0")"

# Ensure psql exists early; silent failures sometimes trace to missing client.
if ! command -v psql >/dev/null 2>&1; then
  die "psql not found in PATH"
fi

# Default site query (normalized lowercasing). Using a here-doc keeps formatting
# clear without multiple concatenations while staying <80 cols.
read -r -d '' DEFAULT_SITE_QUERY <<'SQL' || true
WITH all_sites AS (
  SELECT usgs_site FROM firearea.largest_ammonium_valid_fire_per_site
  UNION ALL SELECT usgs_site FROM firearea.largest_nitrate_valid_fire_per_site
  UNION ALL SELECT usgs_site FROM firearea.largest_orthop_valid_fire_per_site
  UNION ALL SELECT usgs_site FROM firearea.largest_spcond_valid_fire_per_site
)
SELECT DISTINCT lower(all_sites.usgs_site)
FROM all_sites
JOIN firearea.flowlines ON (all_sites.usgs_site = flowlines.usgs_site)
WHERE lower(all_sites.usgs_site) NOT IN (
  SELECT DISTINCT lower(site_fire_distance_run_log.usgs_site)
  FROM firearea.site_fire_distance_run_log
)
FROM all_sites
ORDER BY 1;
SQL

if [[ -n "${SITE_QUERY_FILE:-}" ]]; then
  [[ -f "$SITE_QUERY_FILE" ]] || die "SITE_QUERY_FILE not found: $SITE_QUERY_FILE"
  SITE_QUERY="$(<"$SITE_QUERY_FILE")"
elif [[ -n "${SITE_QUERY:-}" ]]; then
  : # use provided SITE_QUERY
else
  SITE_QUERY="$DEFAULT_SITE_QUERY"
fi

###############################################################################
# Connection argument parsing (-h -p -d -U -W). If not supplied, fall back to
# existing PG* environment variables. Password (-W) sets PGPASSWORD env.
###############################################################################
DB_HOST="${PGHOST:-}"
# Default port fixed at 5432 unless overridden by flag or PGPORT
DB_PORT=5432
if [[ -n "${PGPORT:-}" ]]; then DB_PORT="$PGPORT"; fi
DB_NAME="${PGDATABASE:-}"
DB_USER="${PGUSER:-}"
DB_PASS="${PGPASSWORD:-}"

while getopts ":h:p::d:U:W:" opt; do
  case "$opt" in
    h) DB_HOST="$OPTARG" ;;
    p) DB_PORT="$OPTARG" ;;
    d) DB_NAME="$OPTARG" ;;
    U) DB_USER="$OPTARG" ;;
    W) DB_PASS="$OPTARG" ;;
    :) echo "Missing value for -$OPTARG" >&2; exit 2 ;;
    \?) echo "Unknown option -$OPTARG" >&2; exit 2 ;;
  esac
done
shift $((OPTIND-1))

usage() {
  cat <<USAGE
Usage: $0 -h host -d database -U user [-W password] [-p port]
Optional env: SITE_QUERY_FILE, SITE_QUERY, DRY_RUN=1, MAX_SITES=N
If flags omitted, falls back to PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD.
USAGE
}

# Validate required connection pieces (host/db/user/port) after parsing.
if [[ -z "$DB_HOST" || -z "$DB_NAME" || -z "$DB_USER" ]]; then
  log "WARN: Some connection flags missing; attempting env fallback." >&2
fi
if [[ -z "$DB_HOST" || -z "$DB_NAME" || -z "$DB_USER" ]]; then
  [[ -n "$DB_HOST"     ]] || log "Missing host (-h or PGHOST)" >&2
  [[ -n "$DB_NAME"     ]] || log "Missing database (-d or PGDATABASE)" >&2
  [[ -n "$DB_USER"     ]] || log "Missing user (-U or PGUSER)" >&2
fi
if [[ -z "$DB_HOST" || -z "$DB_NAME" || -z "$DB_USER" ]]; then
  usage
  exit 2
fi

# Export for libpq / psql compatibility, then build explicit psql base.
export PGHOST="$DB_HOST" PGPORT="$DB_PORT" PGDATABASE="$DB_NAME" PGUSER="$DB_USER"
if [[ -n "$DB_PASS" ]]; then export PGPASSWORD="$DB_PASS"; fi

PSQL_BASE=(psql -h "$DB_HOST" -p "$DB_PORT" -d "$DB_NAME" -U "$DB_USER" -v ON_ERROR_STOP=1 -X -At)

run_sql() { # run_sql <sql> [-q]
  local sql="$1"; shift || true
  if [[ -n "${DEBUG:-}" ]]; then
    # Collapse newlines for concise echo
    log "SQL> ${sql//$'\n'/ }"
  fi
  "${PSQL_BASE[@]}" -c "$sql" "$@"
}

log "Fetching site list..."
if ! SITE_LIST=$(run_sql "$SITE_QUERY" 2>__site_err.tmp); then
  rc=$?
  log "Failed fetching site list (exit=$rc)" >&2
  if [[ -s __site_err.tmp ]]; then
    while IFS= read -r line; do
      printf '[psql] %s\n' "$line" >&2
    done < __site_err.tmp
  fi
  rm -f __site_err.tmp
  exit 1
fi
rm -f __site_err.tmp
if [[ -z "$SITE_LIST" ]]; then
  log "No sites returned. Exiting."
  exit 0
fi

if [[ -n "${MAX_SITES:-}" ]]; then
  SITE_LIST=$(printf '%s\n' "$SITE_LIST" | head -n "$MAX_SITES")
  log "Truncated to first $MAX_SITES site(s)."
fi

# Driver now logs into existing firearea.site_fire_distance_run_log instead of
# maintaining a separate multi_site_run_log table. Verify presence upfront so
# we can warn (rather than silently failing) and optionally skip driver rows.
read -r -d '' RUN_LOG_CHECK_SQL <<'SQL' || true
SELECT CASE
  WHEN to_regclass('firearea.site_fire_distance_run_log') IS NULL THEN '0'
  ELSE '1'
END;
SQL
RUN_LOG_EXISTS=$(run_sql "$RUN_LOG_CHECK_SQL")
if [[ "$RUN_LOG_EXISTS" != "1" ]]; then
  log "WARN: firearea.site_fire_distance_run_log missing; driver log rows skipped."
  HAVE_RUN_LOG=0
else
  HAVE_RUN_LOG=1
fi

total=0 success=0 fail=0
for site in $SITE_LIST; do
  # Use pre-increment to avoid exit status 1 on first iteration under set -e
  (( ++total ))
  log "($total) site=$site"
  # Generate a driver run_id analogous to function run_ids for traceability
  driver_run_id="drv_$(date +%Y%m%d%H%M%S%3N)_$RANDOM"
  if [[ $HAVE_RUN_LOG -eq 1 ]]; then
    read -r -d '' TX <<SQL || true
BEGIN;
INSERT INTO firearea.site_fire_distance_run_log(run_id,usgs_site,step,detail)
VALUES ('${driver_run_id}','${site}','driver_start','begin');
SELECT firearea.fn_prepare_site_split_topology('${site}');
SELECT firearea.fn_compute_site_fire_distances('${site}');
INSERT INTO firearea.site_fire_distance_run_log(run_id,usgs_site,step,detail)
VALUES ('${driver_run_id}','${site}','driver_done','completed');
COMMIT;
SQL
  else
    read -r -d '' TX <<SQL || true
BEGIN;
SELECT firearea.fn_prepare_site_split_topology('${site}');
SELECT firearea.fn_compute_site_fire_distances('${site}');
COMMIT;
SQL
  fi

  if [[ -n "${DRY_RUN:-}" ]]; then
    printf '\n--- DRY RUN SQL (site=%s) ---\n%s\n' "$site" "$TX"
    continue
  fi

  if psql -v ON_ERROR_STOP=1 -X -q -c "$TX" >/dev/null 2>&1; then
    (( ++success ))
    log "($total) site=$site OK"
  else
    (( ++fail ))
    log "($total) site=$site FAILED (rolled back)"
    if [[ $HAVE_RUN_LOG -eq 1 ]]; then
      read -r -d '' DRIVER_ERR_SQL <<SQL || true
INSERT INTO firearea.site_fire_distance_run_log(run_id,usgs_site,step,detail)
VALUES ('${driver_run_id}','${site}','driver_error','see server logs');
SQL
      run_sql "$DRIVER_ERR_SQL" || true
    fi
  fi
done

log "Complete: total=$total success=$success fail=$fail"

# Optional summary (driver rows only, if table exists)
if [[ $HAVE_RUN_LOG -eq 1 ]]; then
  read -r -d '' DRIVER_SUMMARY_SQL <<'SQL' || true
SELECT step, count(*)
FROM firearea.site_fire_distance_run_log
WHERE step LIKE 'driver_%'
GROUP BY step
ORDER BY 1;
SQL
  run_sql "$DRIVER_SUMMARY_SQL" 2>/dev/null || true
fi
