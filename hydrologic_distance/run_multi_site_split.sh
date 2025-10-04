#!/usr/bin/env bash
# Purpose: Prepare split topology and compute distances (split mode) for a dynamic
#          multi-site list, ensuring each site runs in its OWN transaction.
#
# Requirements:
#   * psql available and connection parameters set via environment or .pgpass.
#   * Functions: firearea.fn_prepare_site_split_topology, firearea.fn_compute_site_fire_distances
#
# Connection env vars (override as needed):
#   PGHOST / PGPORT / PGDATABASE / PGUSER / PGPASSWORD
#
# Site list query (edit to taste). The DISTINCT + lower() pattern keeps things normalized.
SITE_QUERY="WITH all_sites AS (\n  SELECT usgs_site FROM firearea.largest_ammonium_valid_fire_per_site\n  UNION ALL SELECT usgs_site FROM firearea.largest_nitrate_valid_fire_per_site\n  UNION ALL SELECT usgs_site FROM firearea.largest_orthop_valid_fire_per_site\n  UNION ALL SELECT usgs_site FROM firearea.largest_spcond_valid_fire_per_site\n) SELECT DISTINCT lower(usgs_site) FROM all_sites ORDER BY 1;"

# Fail fast on unset critical variables (optional)
: "${PGDATABASE:?Need PGDATABASE set}"

# psql flags
PSQL_BASE=(psql -v ON_ERROR_STOP=1 -At)

# Derive list
echo "[multi] Fetching site list..." >&2
SITE_LIST=$("${PSQL_BASE[@]}" -c "$SITE_QUERY") || { echo "Failed to fetch site list" >&2; exit 1; }

if [[ -z "$SITE_LIST" ]]; then
  echo "[multi] No sites returned. Exiting." >&2
  exit 0
fi

# Optional: log table (create if absent) for per-site outcomes
CREATE_LOG="DO $$ BEGIN IF to_regclass('firearea.multi_site_run_log') IS NULL THEN\n  CREATE TABLE firearea.multi_site_run_log (\n    run_ts timestamptz DEFAULT now(),\n    usgs_site text,\n    phase text,\n    status text,\n    detail text\n  );\nEND IF; END $$;"
psql -v ON_ERROR_STOP=1 -c "$CREATE_LOG" || { echo "[multi] Failed to ensure log table" >&2; }

# Iterate sites
total=0
success=0
fail=0
for site in $SITE_LIST; do
  ((total++))
  echo "[multi][$total] Processing site=$site" >&2

  # Wrap each site in its own explicit transaction so a failure rolls back only that site
  TX="BEGIN;\n  INSERT INTO firearea.multi_site_run_log(usgs_site,phase,status,detail) VALUES ('$site','start','info','begin');\n  SELECT firearea.fn_prepare_site_split_topology('$site');\n  -- Unified split-only function (signature: text, boolean DEFAULT true, touch_tolerance DEFAULT 5.0)\n  SELECT firearea.fn_compute_site_fire_distances('$site');\n  INSERT INTO firearea.multi_site_run_log(usgs_site,phase,status,detail) VALUES ('$site','done','ok','completed');\n  COMMIT;"

  if psql -v ON_ERROR_STOP=1 -c "$TX" >/dev/null 2>&1; then
    ((success++))
    echo "[multi][$total] site=$site OK" >&2
  else
    ((fail++))
    echo "[multi][$total] site=$site FAILED — logging rollback" >&2
    # Log failure (outside failed transaction)
    psql -v ON_ERROR_STOP=0 -c "INSERT INTO firearea.multi_site_run_log(usgs_site,phase,status,detail) VALUES ('$site','error','fail','see server logs');" >/dev/null 2>&1
  fi
done

echo "[multi] Complete: total=$total success=$success fail=$fail" >&2

# Quick summary query (optional)
psql -c "SELECT status, count(*) FROM firearea.multi_site_run_log GROUP BY status ORDER BY 1;" 2>/dev/null || true
