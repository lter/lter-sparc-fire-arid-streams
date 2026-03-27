# Methods

## Study Scope and Synthesis Objective

This project develops and applies a reproducible, catchment-scale workflow to quantify how wildfire history and hydrologic variability alter stream biogeochemistry in arid and semi-arid systems of the southwestern United States. The analytical objective is to estimate changes in concentration-discharge (CQ) behavior associated with fire exposure while preserving temporal context, spatial provenance, and compatibility across heterogeneous monitoring programs.

The methods combine database-centered harmonization of chemistry and discharge records, spatial fire-catchment aggregation, hydrologic-distance characterization, and Bayesian model fitting. The conceptual framing is consistent with the argument that fire effects on streams in aridlands are shaped by interactions among accumulation, combustion, transport, and propagation under episodic precipitation and variable connectivity (https://doi.org/10.1093/biosci/biae120). The inferential target therefore includes both immediate post-fire changes and lagged responses that may emerge after delayed transport events (https://doi.org/10.1007/s10533-024-01154-y).

## Data Acquisition and Standardization

### Hydrochemical and Discharge Inputs

The workflow ingests stream chemistry and flow data from USGS and non-USGS sources. Inputs include analyte concentrations, parameter metadata, sample timestamps, and discharge records aligned to site identifiers. Because source programs differ in units, forms, and reporting conventions, records are standardized in PostgreSQL before analysis.

In practice, chemistry standardization logic is authored in Quarto source files and executed as extracted SQL. This includes analyte alignment and reconciliation into standardized views (for example, a canonical water chemistry table and derived analyte-specific products). The repository documentation emphasizes deterministic build semantics and reproducibility of transformed outputs.

### Fire History and Spatial Inputs

Fire exposure data are based on MTBS perimeters and associated event timing attributes. Catchment boundaries, hydrography, and ecoregion overlays are used to assign fire exposure to monitoring sites and to derive covariates describing burned area, timing since fire, and cumulative disturbance history.

Spatial operations use PostGIS geometry operations with spheroidal area estimates where appropriate (for example, `ST_Area(geom, TRUE)`), and catchment/burned-area reporting is commonly normalized to square kilometers for modeling compatibility.

## Spatial Processing and firearea-Supported Workflows

The companion firearea repository (https://srearl.gitlab.io/firearea/index.html) provides reusable R tooling that supports the upstream spatial preparation required by lter-sparc-fire-arid-streams.

### Catchment Delineation and Site Preparation

firearea functions support delineation of catchments above sampling or gaging points using USGS-linked hydrographic services. These workflows provide standardized watershed polygons for subsequent fire intersection and covariate extraction. Relevant documented pathways include NHDPlus-based delineation and StreamStats-based delineation for alternate service contexts.

### Fire-Catchment Intersection and Burn Metrics

firearea workflows intersect MTBS fire perimeters with delineated catchments and compute burned-area statistics both at whole-fire and within-catchment extents. This step establishes site-specific disturbance exposure features and enables downstream computation of percent burned and cumulative burned area across event histories.

### Fire-to-Channel Distance Metrics

firearea methods also quantify minimum distance from fire boundaries to channel networks for each site-fire context. These distance calculations are computationally intensive but methodologically important because they provide a connectivity-relevant descriptor of disturbance proximity that can influence transport potential and lag structure.

Together, these firearea-supported products are transferred into the database-centric synthesis workflow, where they are joined with chemistry and discharge records to create model-ready tables.

## Database Engineering and Derived Products

### Documentation-First SQL Pattern

In lter-sparc-fire-arid-streams, SQL is authored in Quarto (`.qmd`) as documentation-first source, then extracted to executable SQL and run with task recipes (`just`). This pattern is used instead of relying on Quarto runtime SQL execution for DDL-heavy workflows. It provides traceability between methods documentation and concrete database state.

### Build and Execution Semantics

The database workflow is orchestrated through extraction and execution recipes (for example, `just extract`, `just run`, `just docs`, `just all`) and supports environment-variable-driven targeting of development or production contexts. The method design prioritizes reproducibility and deterministic outputs over interactive ad hoc querying.

### Fire Exposure Aggregation

A core derived object is the fire aggregation view/materialized view (`ranges_agg`), which integrates event timing and cumulative burn metrics at site level. The resulting output includes:

- Fire period boundaries (`start_date`, `end_date`) and neighboring event windows.
- Inter-fire timing measures (`days_since`, `days_until`).
- Event arrays for grouped fire periods.
- Cumulative burned area and percentage burned metrics (`cum_fire_area`, `cum_per_cent_burned`).
- Site-level cumulative totals through time (`all_fire_area`, `all_per_cent_burned`).

These products enable explicit assignment of observations to pre-, post-, and inter-event contexts during model preparation.

### Harmonized Chemistry Products and Exports

Standardized chemistry views are rebuilt with dependencies and then exported for modeling in R/Stan. Export logic retains deterministic ordering to ensure reproducible CSV/RDS handoffs and stable downstream diagnostics.

## Modeling Framework

### CQ Formulation

The analytical core is a log-transformed CQ framework:

- Concentration response: $C$ (often standardized log concentration).
- Discharge predictor: $Q$ (often standardized log discharge).
- Baseline linear relationship: $C = A + bQ$.

Stan templates in the repository implement progressively richer structures to characterize fire-related changes in this relationship.

### Model Progression

1. Baseline linear model.
   - Estimates a single intercept, slope, and residual error.
   - Used to validate scaling choices and establish baseline CQ behavior.

2. Pre/post-fire split model.
   - Introduces fire-period indexing to estimate separate pre-fire and post-fire parameters for intercept, slope, and residual variance.
   - Supports direct comparison of CQ structure across disturbance states.

3. Delta-oriented model family.
   - Computes derived contrasts (for example, slope, intercept, and variance differences between pre and post periods).
   - Facilitates interpretation of fire-associated directional change.

4. Unified pre/post model.
   - Estimates indexed parameters in a single model and reports generated quantities for deltas.
   - Improves internal coherence of parameter estimation and contrast calculation.

5. Hierarchical extension (in progress).
   - Expands from within-site inference to partial pooling across sites.
   - Intended to improve cross-site generalization while retaining site-level variability.

### Data Preparation for Stan

Model inputs are prepared in R, including filtering by analyte/site criteria, transformations, scaling, fire-period coding, and NA/NaN checks required for Stan compatibility. The development script documents iterative quality checks (for example, convergence diagnostics, posterior summaries, and parameter interval plots) used to evaluate model performance.

## Reproducibility and Execution Environment

### Core Stack

- PostgreSQL with PostGIS for data harmonization and spatial processing.
- Quarto as SQL documentation source.
- `just` for extraction/execution/documentation automation.
- R for preprocessing, diagnostics, and orchestration.
- Stan (`rstan`) for Bayesian model estimation.

### Reproducibility Practices

The implementation emphasizes:

- Explicit build steps for functions/views and dependencies.
- Deterministic ordering in exported datasets.
- Stable schema-level object naming and SQL readability conventions.
- Separation of documentation source (`.qmd`) from executable SQL artifacts.

This design allows methods and outputs to be rebuilt with minimal ambiguity when upstream data or logic are updated.

## Integration of Publication Context

Two project-linked publications inform interpretation and reporting choices in the current methods:

- Conceptual integration of fire-hydrology-biogeochemistry interactions, including aridity-dependent heterogeneity, episodic transport, and downstream propagation (https://doi.org/10.1093/biosci/biae120).
- Empirical emphasis on persistent and lagged solute responses tied to intermittent precipitation regimes in arid catchments (https://doi.org/10.1007/s10533-024-01154-y).

These references are used to frame hypotheses and expected response modes rather than to substitute for the repository-specific computational procedures documented above.

## Assumptions and Limitations

1. Temporal assignment boundaries.
- Event-window categorization near ignition dates can affect pre/post labeling; boundary behavior requires careful verification for edge cases.

2. Coverage heterogeneity.
- Site-level observation density and temporal span vary across analytes and monitoring programs, influencing model identifiability and uncertainty.

3. Spatial uncertainty and service dependence.
- Delineation and hydrographic derivations depend on external geospatial services and CRS consistency in firearea-supported workflows.

4. Computational intensity.
- Fire intersection and distance calculations can be expensive for large spatial domains and dense hydrographic networks.

5. Model development status.
- Hierarchical Stan formulations are included as active extensions and should be interpreted as in-progress until fully validated across full multi-site datasets.

## Summary of Methodological Contribution

The combined lter-sparc-fire-arid-streams and firearea workflow (https://srearl.gitlab.io/firearea/index.html) delivers an end-to-end synthesis pipeline that links wildfire disturbance history to stream chemistry through transparent data engineering, spatially explicit exposure attribution, and probabilistic CQ modeling. This structure is designed to support rigorous inference under the defining aridland condition of hydroclimatic intermittency, including delayed and persistent post-fire responses.
