# Quantifying interactive effects of fire and precipitation regimes on catchment biogeochemistry of aridlands

[Project Summary](https://lternet.edu/working-groups/fire-and-aridland-streams/)

## PIs: 

- Tamara Harms, Associate Professor, Department of Biology & Wildlife and Institute of Arctic Biology, University of Alaska Fairbanks
- Heili Lowman, Department of Natural Resources and Environmental Science, University of Nevada, Reno
- Stevan Earl, Information Manager for the Central Arizona-Phoenix Long-Term Ecological Research (CAP LTER) project

## Script Explanations

- `extract_...` - Extract climate data (for the variable defined in the `...` part of the script name) within site polygons (provided as a GeoJSON)

- In the `models` folder, the following STAN scripts are available for progressively built out model structures:
1. `STAN_lm_template.stan` - Provides a basic linear model with the structure of y = mx + b.
2. `STAN_lm_prepost_template.stan` - Splits observations into pre- and post-fire and estimates CQ slopes before and after.
3. `STAN_lm_delta_template.stan` - Uses pre- and post-fire CQ slopes to calculate delta, the change in slopes, as a derived variable.
4. `STAN_lm_delta_unified_template.stan` - Unifies model structure to estimate all parameters in a single model structure, both pre- and post-fire.
5. `STAN_lm_hierarchical_template.stan` - Estimates all parameters in a hierarchical structure (at site- and universal-level). UNDER DEVELOPMENT

- In the `model_fitting` folder, the `STAN_dev_script.R` works through prepping data and fitting all the model structures above.

## Related Repositories

- [hlowman / **crass_bgc**](https://github.com/hlowman/crass_bgc)
