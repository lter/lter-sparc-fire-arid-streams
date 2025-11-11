# AI Coding Assistant Instructions

These guidelines help AI agents work productively in this repository.

## Project Purpose & Big Picture
Quantify wildfire impacts on stream hydrology and biogeochemistry across aridland catchments. The repo blends:
- Spatial & fire history processing (PostgreSQL/PostGIS in `wildfire_database`)
- Water chemistry standardization + aggregation (SQL functions + views)
- Statistical modeling (Stan templates in `models/`, orchestration in `model_fitting/`)
- Derived spatial / hydrologic metrics (R + shell scripts in thematic folders)

Data & computation flow (core path):
Raw USGS + non-USGS chem & discharge → Standardization functions (SQL) → Materialized/views (e.g. `usgs_water_chem_std`, `ranges_agg`, nitrate / ammonium products) → CSV exports via psql / `just` → Modeling & figures (R / Stan).

## Key Directories
- `wildfire_database/` Quarto `.qmd` specs containing curated SQL blocks (NOT executed by Quarto; extracted & run via tooling)
- `wildfire_database/*.md` Rendered documentation for the SQL logic
- `models/` Stan templates (progressive complexity: basic → pre/post → delta → unified → hierarchical)
- `model_fitting/STAN_dev_script.R` Prepares data & fits each Stan model
- `hydrologic_distance/` R + shell scripts computing distance metrics
- `spatial_covars/` Spatial covariate preparation & README guidance
- `burn_severity/`, `conceptual_figure_one/`, `marss_figure_one/` Figure generation scripts

## Database / Schema Conventions
- Target schema: `firearea` (in database `wildfire_dev` by default)
- Geometry area calculations use spheroidal form `ST_Area(geom, TRUE)`; catchment areas usually divided by 1,000,000 to km²
- Fire aggregation view (`ranges_agg`) provides: `events` (array of fire ids), `cum_fire_area`, `cum_per_cent_burned`, cumulative site metrics `all_fire_area`, `all_per_cent_burned`, temporal spacing (`days_since`, `days_until`)
- Largest valid fire logic differs per analyte depending on pre/post coverage rules; intentional that “largest” may vary across products
- Flow quartiles (1–4) added to unified discharge table after unioning USGS + non-USGS sources; queries assume quartiles present
- Nitrate / discharge pre/post fire selection requires presence of Q2–Q4 before & after within 3-year windows (flexible coverage, not continuous time)

## Quarto & SQL Execution Pattern
Quarto is documentation-first here. Direct execution of arbitrary SQL DDL via knitr/Jupyter can be unreliable.
Preferred workflow:
1. Author / edit `.qmd` with fenced ```sql blocks
2. Use `just` recipes to extract SQL (transaction-wrapped by default) to `.sql`
3. Execute extracted SQL against Postgres (local or remote via env vars)
4. Optionally render a `.md` for GitHub anchorable documentation

Do NOT try to “run” complex SQL inside Quarto; rely on extraction.

## `just` Task System (critical)
Representative recipes (see `wildfire_database/README.md` for full list):
- `just list-qmd` enumerate available Quarto specs
- `just extract wildfire_database_query_nitrate.qmd` produce SQL only
- `just run wildfire_database_query_nitrate.qmd` extract + execute
- `just docs wildfire_database_query_nitrate.qmd` render markdown documentation
- `just all wildfire_database_query_nitrate.qmd` full pipeline (functions, views, docs, exports if embedded)
- `just manual water_chem_functions.sql -d wildfire` run a standalone SQL file with custom DB flags
Environment precedence for connection:
`PGHOST`, `PGDATABASE`, `PGUSER` (falls back to shell `$USER`), optional port.

## Environment / Execution Assumptions
- Default DB: `wildfire_dev`; production or test overridden by env vars
- Spatial operations require PostGIS installed
- Exports rely on `\COPY` (server-side independent of client locale)
- Long-running fire aggregation / materialized view refreshes acceptable (performance not primary constraint)

## Adding / Modifying SQL
- Maintain NO table aliasing when instructed for specific objects (e.g., in `ranges_agg` modifications)
- Preserve ordering semantics and window functions; modifying partition/order clauses changes scientific meaning
- When adding cumulative metrics: prefer window functions with explicit `ORDER BY end_date ROWS UNBOUNDED PRECEDING`

## Common Pitfalls
- Forgetting to rebuild materialized views after modifying upstream functions/views (run rebuild function or re-extract SQL)
- Missing flow quartiles if discharge union logic updated—ensure quartile assignment step rerun
- Attempting to rely on Quarto execution for DDL (use `just` instead)
- Array equality joins: sort arrays before comparison when order shouldn’t matter (`ARRAY(SELECT unnest(events) ORDER BY 1)`)

## When Editing Documentation
- Provide both `.qmd` (source) and generated `.md` if linkable anchors are needed for cross-referencing
- Use explicit header IDs `{#anchor-id}` in Quarto if later rendered to HTML/PDF; for GitHub anchor links prefer `.md`

## How AI Should Propose Changes
1. Identify target file(s) & confirm existing patterns (don’t introduce new style unless justified)
2. If modifying SQL logic, state scientific implication of change
3. Maintain deterministic ordering in outputs (`ORDER BY`) for reproducibility
4. For new automation, suggest additions to the `justfile` as well as ad-hoc shell scripts

## SQL Conventions (Project-Specific)
- Never use table aliasing in SQL delivered for this repository (explicit schema-qualified names improve readability & diff clarity). Example:
  - Preferred: `SELECT firearea.ranges_agg.usgs_site, firearea.ranges_agg.cum_fire_area FROM firearea.ranges_agg`.
  - Avoid: `SELECT r.usgs_site, r.cum_fire_area FROM firearea.ranges_agg r`.
- When joining arrays of fire ids, always sort for equality tests: `ARRAY(SELECT unnest(firearea.ranges_agg.events) ORDER BY 1)`.
- Always include a deterministic `ORDER BY` in exported datasets to keep CSV outputs stable across rebuilds.
- When adding new views/materialized views, consider index hints in comments if query patterns filter by `usgs_site`, date ranges, or `year`.

## R Coding Conventions
- Always namespace functions (e.g., `dplyr::mutate`, `purrr::map_dfr`, `stringr::str_detect`) to avoid ambiguity and make dependencies explicit.
- Prefer `purrr` functional patterns over explicit `for` loops when mapping over vectors/data frames (e.g., `purrr::map`, `purrr::map2`, `purrr::pmap`); return tibbles with `purrr::map_dfr` where row-binding is intended.
- Use snake_case for all object, function, and column names; avoid dots or camelCase (e.g., `nitrate_stats`, not `nitrateStats`).
- When iterating with progress or rate limiting, wrap mapping calls with a safely/quietly pattern (`purrr::safely`, `purrr::possibly`) and emit messages using `rlang::inform` or `cli::cli_inform` only when necessary.
- Prefer explicit column selection and creation pipelines: start with `dplyr::select` → `dplyr::mutate` → `dplyr::filter` → summarise/join; keep joins keyed explicitly (`by = c("usgs_site", "date" )`).
- Always check for zero-row or all-NA edge cases before modeling or exporting; use `dplyr::summarize(dplyr::across(everything(), ~ sum(is.na(.))))` patterns for quick diagnostics.
- For reproducible sampling or stochastic modeling steps, set seeds locally (`withr::with_seed(123, { ... })`) rather than globally.

---
Questions: clarify missing patterns (e.g., if a new analyte or fire selection rule) before generating large diffs.