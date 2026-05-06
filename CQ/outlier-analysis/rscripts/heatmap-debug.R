# step 1 - summarise
step1 <- preds %>%
  group_by(dsf_scaled, pct_burned) %>%
  summarise(
    q05 = quantile(.prediction, 0.05),
    q50 = quantile(.prediction, 0.50),
    q95 = quantile(.prediction, 0.95),
    .groups = "drop"
  ) %>%
  mutate(
    dsf_unscaled        = (dsf_scaled * 291.9441) + 557.7047,
    pct_burned_unscaled = (pct_burned * 13.44289) + 5.447886
  )

range(step1$dsf_unscaled)  # should be non-zero

# step 2 - dsf sensitivity
step2 <- step1 %>%
  arrange(pct_burned_unscaled, dsf_unscaled) %>%
  group_by(pct_burned_unscaled) %>%
  mutate(across(c(q05, q50, q95),
                ~ abs(. - lag(.)) / abs(dsf_unscaled - lag(dsf_unscaled)),
                .names = "dsf_sens_{.col}")) %>%
  ungroup()
str(step2)
range(step2$dsf_unscaled)       # still non-zero?
range(step2$dsf_sens_q50, na.rm = TRUE)  # should have values now

# step 3 - burn sensitivity
step3 <- step2 %>%
  arrange(dsf_unscaled, pct_burned_unscaled) %>%
  group_by(dsf_unscaled) %>%
  mutate(across(c(q05, q50, q95),
                ~ abs(. - lag(.)) / abs(pct_burned_unscaled - lag(pct_burned_unscaled)),
                .names = "burn_sens_{.col}")) %>%
  ungroup()
str(step3)
range(step3$dsf_unscaled)        # still non-zero?
range(step3$burn_sens_q50, na.rm = TRUE)  # should have values now

str(step1)
# dsf sensitivity
dsf_sens <- step1 %>%
  arrange(pct_burned_unscaled, dsf_unscaled) %>%
  group_by(pct_burned_unscaled) %>%  # use scaled to avoid float issues
  mutate(across(c(q05, q50, q95),
                ~ abs(. - lag(.)) / abs(dsf_unscaled - lag(dsf_unscaled)),
                .names = "dsf_sens_{.col}")) %>%
  ungroup() %>%
  select(dsf_scaled, pct_burned, pct_burned_unscaled, dsf_unscaled, starts_with("dsf_sens"))
str(dsf_sens)
# burn sensitivity
burn_sens <- step1 %>%
  arrange(dsf_unscaled, pct_burned_unscaled) %>%
  group_by(dsf_unscaled) %>%  # use scaled to avoid float issues
  mutate(across(c(q05, q50, q95),
                ~ abs(. - lag(.)) / abs(pct_burned_unscaled - lag(pct_burned_unscaled)),
                .names = "burn_sens_{.col}")) %>%
  ungroup() %>%
  select(dsf_scaled, pct_burned, dsf_unscaled, pct_burned_unscaled, starts_with("burn_sens"))
str(burn_sens)
str(dsf_sens)
# join back together
step3 <- step1 %>%
  left_join(dsf_sens,  by = c("dsf_unscaled", "pct_burned_unscaled")) %>%
  left_join(burn_sens, by = c("dsf_unscaled", "pct_burned_unscaled"))
str(step3)
range(step3$dsf_sens_q50,  na.rm = TRUE)
range(step3$burn_sens_q50, na.rm = TRUE)

heatmap_sensitivity <- preds %>%
  group_by(dsf_scaled, pct_burned) %>%
  summarise(
    q05 = quantile(.prediction, 0.05),
    q25 = quantile(.prediction, 0.25),
    q50 = quantile(.prediction, 0.50),
    q75 = quantile(.prediction, 0.75),
    q95 = quantile(.prediction, 0.95),
    .groups = "drop"
  ) %>%
  mutate(
    dsf_unscaled        = (dsf_scaled * 291.9441) + 557.7047,
    pct_burned_unscaled = (pct_burned * brn_sd) + brn_mean,
    across(c(q05, q25, q50, q75, q95), ~ (. * y_sd) + y_mean)
  ) %>%
  arrange(pct_burned_unscaled, dsf_unscaled) %>%
  group_by(pct_burned_unscaled) %>%
  mutate(across(c(q05, q25, q50, q75, q95), 
                ~ abs(. - lag(.)) / abs(dsf_unscaled - lag(dsf_unscaled)),
                .names = "dsf_sens_{.col}")) %>%
  ungroup() %>%
  arrange(dsf_unscaled, pct_burned_unscaled) %>%
  group_by(dsf_unscaled) %>%
  mutate(across(c(q05, q25, q50, q75, q95),
                ~ abs(. - lag(.)) / abs(pct_burned_unscaled - lag(pct_burned_unscaled)),
                .names = "burn_sens_{.col}")) %>%
  ungroup() %>%
  pivot_longer(
    cols          = c(starts_with("dsf_sens_"), starts_with("burn_sens_")),
    names_to      = c("direction", "quantile"),
    names_pattern = "(dsf_sens|burn_sens)_(q\\d+)",
    values_to     = "sensitivity"

  ) %>%
  mutate(
    direction = recode(direction,
      dsf_sens  = "Days since fire",
      burn_sens = "Burn severity"
    ),
    quantile = recode(quantile,
      q05 = "5th percentile",  q25 = "25th percentile",
      q50 = "50th percentile", q75 = "75th percentile",
      q95 = "95th percentile"
    ),
    quantile  = factor(quantile,  levels = c("5th percentile", "25th percentile",
                                              "50th percentile", "75th percentile",
                                              "95th percentile")),
    direction = factor(direction, levels = c("Days since fire", "Burn severity"))
  )
str(heatmap_sensitivity)
