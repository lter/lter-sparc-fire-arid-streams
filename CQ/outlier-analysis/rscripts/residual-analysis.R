###instead of looking at only the specific quantiles of the residuals, we can look across the distribution 
##of residuals by plotting the coefficient estimates against the residual values. 
##This way we can see how the effect changes across the distribution of residuals without constraining the data to specific quantiles.
## then maybe we can look at the different quantiles

library(tidyverse)
library(brms)
library(tidybayes)
library(patchwork)

# ── Load & prep data ──────────────────────────────────────────────────────────
df <- read_csv("/Users/ash/Documents/projects/CRASS/nitrate_cq_residuals_with_ecoregion_.2slope.csv") %>%
  #filter(days_since_last_fire > 0) %>%  # post-fire only
  mutate(
    catchment   = factor(usgs_site),
    # Reference ecoregion = COLD DESERTS (first alphabetically)
    # Relevel here if you want a different reference
    ecoregion   = factor(ecoregion_name),
    # Reference CQ status = neutral (chemostatic analog)
    cq_status   = factor(slopedir, levels = c("neutral", "positive", "negative")),
    log_n = log(value_std),
    log_q = log(Flow),
    #post  = as.integer(segment == "after"), 
  )
colnames(df)

##get the means and sds for back-transforming later
x_mean <- mean(df$log_q)
x_sd   <- sd(df$log_q)
y_mean <- mean(df$log_n)
y_sd   <- sd(df$log_n)


scaled_df <- df %>%
  mutate(
    log_Q_sc  = (log_q - x_mean)/ x_sd,
    log_N_sc  = (log_n - y_mean)/ y_sd,
    ##scale these after removing the 0 values
    #pct_burned = (total_burned - brn_mean) / brn_sd,
    #dsf_scaled = (days_since_last_fire - dsf_mean) / dsf_sd
    )

colnames(scaled_df)

models_nitrate <- scaled_df %>%
  group_by(usgs_site) %>%
  mutate(rs = resid((lm(log_N_sc ~ log_Q_sc))),
         int = coef(lm(log_N_sc ~ log_Q_sc))[1],
         slope = coef(lm(log_N_sc ~ log_Q_sc))[2],
         slopedir = ifelse(slope >= .2, "positive", ifelse(slope <= -.2 , "negative", "neutral"))) %>% ungroup()
colnames(models_nitrate)

##need post fire only for this model
models_nitrate <- models_nitrate %>% filter(days_since_last_fire > 0)

##no nonburned sites included
brn_mean <- mean(models_nitrate$total_burned)
brn_sd   <- sd(models_nitrate$total_burned)
dsf_mean <- mean(models_nitrate$days_since_last_fire)
dsf_sd   <- sd(models_nitrate$days_since_last_fire)

models_nitrate <- models_nitrate %>%
  mutate(
    pct_burned = (total_burned - brn_mean) / brn_sd,
    dsf_scaled = (days_since_last_fire - dsf_mean) / dsf_sd
  )

brms_formula <- bf(
  rs ~ dsf_scaled + pct_burned + dsf_scaled:pct_burned +
      cq_status + (1 | ecoregion)
)

get_prior(brms_formula, data = models_nitrate)

priors <- c(
  prior(normal(0, 1), class = b),                        # all slopes
  prior(normal(0, 1), class = Intercept),                # intercept        # specific slope
  prior(exponential(1), class = sd),                     # random effect SDs                        # random effect correlations
  prior(exponential(1), class = sigma)                   # residual SD
)

fit1 <- brm(
  formula = brms_formula,
  data    = models_nitrate,
  prior   = priors,
  chains  = 4,
  cores   = 4,
  iter    = 4000,
  warmup  = 1000,
  seed    = 123,
  threads = threading(4),
  backend = "cmdstanr",
  control = list(adapt_delta = 0.99, max_treedepth = 15),
  file    = "fit_all_v4"
)

summary(fit1)
pp_check(fit1)
plot(fit1)
pairs(fit1)

conditional_effects(fit1)
coef_comparison <- fit1 %>%
  gather_draws(b_dsf_scaled, b_pct_burned, `b_dsf_scaled:pct_burned`,b_cq_statuspositive, b_cq_statusnegative) %>%
  mutate(term = recode(.variable,
                       b_dsf_scaled = "Days since fire",
                       b_pct_burned = "Cumulative % burn",
                       `b_dsf_scaled:pct_burned` = "DSF x % burn interaction",
                       cq_statuspositive = "CQ status: positive",
                       cq_statusnegative = "CQ status: negative"
                       ))
#unique(coef_comparison$term)
coef_comparison_summary <- coef_comparison %>%
  group_by(term) %>%
  median_qi(.value, .width = 0.95)

coef_comparison_summary %>%
  ggplot(aes(x = .value, y = term, colour = term)) +
  geom_point() +
  geom_errorbarh(aes(xmin = .lower, xmax = .upper), height = 0.2) +
  geom_vline(xintercept = 0, linetype = "dashed", colour = "grey40") +
  labs(x = "Coefficient estimate (scaled predictors)", y = NULL, title = "Posterior estimates with 95% credible intervals") +
  theme(panel.grid.minor = element_blank(), legend.position = "none")



##plot the effects across the quantiles of the residuals
conditional_effects(fit1, effects = "dsf_scaled")
conditional_effects(fit1, effects = "pct_burned")
conditional_effects(fit1, effects = "dsf_scaled:pct_burned")

# create a prediction grid
new_data <- models_nitrate %>%
  data_grid(
    dsf_scaled = seq_range(dsf_scaled, n = 50),
    pct_burned = seq_range(pct_burned, n = 50), 
    cq_status  = "neutral"
  )

# get full posterior predictions
preds <- new_data %>% 
  add_predicted_draws(fit1, ndraws = 500, re_formula = NA)  # include all random effects

# then summarize at quantiles of the predicted response
preds %>% 
  group_by(dsf_scaled, pct_burned) %>% 
  summarise(
    q05 = quantile(.prediction, 0.05),
    q25 = quantile(.prediction, 0.25),
    q50  = quantile(.prediction, 0.50),
    q75  = quantile(.prediction, 0.75),
    q90  = quantile(.prediction, 0.90),
    q95  = quantile(.prediction, 0.95),
    q99  = quantile(.prediction, 0.99)
  ) %>%
     mutate(
    # unscale x-axis: days since fire
    dsf_unscaled = (dsf_scaled * dsf_sd) + dsf_mean,
    pct_burned_unscaled = (pct_burned * brn_sd) + brn_mean,
    # unscale y-axis: rs are residuals of log_N_sc, so unscale by y
    q05 = (q05 * y_sd) + y_mean,
    q25 = (q25 * y_sd) + y_mean,
    q50 = (q50 * y_sd) + y_mean,
    q75 = (q75 * y_sd) + y_mean,
    q90 = (q90 * y_sd) + y_mean,
    q95 = (q95 * y_sd) + y_mean,
    q99 = (q99 * y_sd) + y_mean
  ) %>% ungroup()

preds <- new_data %>% 
  add_predicted_draws(
    fit1, 
    re_formula        = NULL,
    allow_new_levels  = TRUE,
    sample_new_levels = "gaussian",
    ndraws            = 500
  )

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
    dsf_unscaled        = (dsf_scaled  * dsf_sd)  + dsf_mean,
    pct_burned_unscaled = (pct_burned  * brn_sd)  + brn_mean,
    across(c(q05, q25, q50, q75, q95), ~ (. * y_sd) + y_mean)
  ) %>%
  arrange(dsf_unscaled, pct_burned_unscaled) %>%
  group_by(pct_burned_unscaled) %>%
  mutate(across(c(q05, q25, q50, q75, q95), 
                ~ abs(. - lag(.)) / (dsf_unscaled - lag(dsf_unscaled)),
                .names = "dsf_sens_{.col}")) %>%
    # dsf_sens_q05 = abs(q05 - lag(q05,default = 0))/ abs(dsf_unscaled - lag(dsf_unscaled)),
    #      dsf_sens_q25 = abs(q25 - lag(q25,default = 0)) / abs(dsf_unscaled - lag(dsf_unscaled)),
    #      dsf_sens_q50 = abs(q50 - lag(q50,default = 0))/ abs(dsf_unscaled - lag(dsf_unscaled)),
    #      dsf_sens_q75 = abs(q75 - lag(q75,default = 0)) / abs(dsf_unscaled - lag(dsf_unscaled)),
    #      dsf_sens_q95 = abs(q95 - lag(q95,default = 0)))/ abs(dsf_unscaled - lag(dsf_unscaled))  %>%
#    across(c(q05, q25, q50, q75, q95), 
#                 ~ abs(. - lag(.)) / (dsf_unscaled - lag(dsf_unscaled)),
#                 .names = "dsf_sens_{.col}")) %>%
  ungroup() %>%
  group_by(dsf_unscaled) %>%
  mutate(burn_sens_q05 = abs(q05 - lag(q05)) / abs(pct_burned_unscaled - lag(pct_burned_unscaled)),
         burn_sens_q25 = abs(q25 - lag(q25)) / abs(pct_burned_unscaled - lag(pct_burned_unscaled)),
         burn_sens_q50 = abs(q50 - lag(q50)) / abs(pct_burned_unscaled - lag(pct_burned_unscaled)),
         burn_sens_q75 = abs(q75 - lag(q75)) / abs(pct_burned_unscaled - lag(pct_burned_unscaled)),
         burn_sens_q95 = abs(q95 - lag(q95)) / abs(pct_burned_unscaled - lag(pct_burned_unscaled))) %>%
    # across(c(q05, q25, q50, q75, q95),
    #             ~ abs(. - lag(.)) / (pct_burned_unscaled - lag(pct_burned_unscaled)),
    #             .names = "burn_sens_{.col}")) %>%
  ungroup() %>%
#   # combine into total sensitivity (magnitude of gradient)
  mutate(
    sens_q05 = sqrt(dsf_sens_q05^2 + burn_sens_q05^2),
    sens_q25 = sqrt(dsf_sens_q25^2 + burn_sens_q25^2),
    sens_q50 = sqrt(dsf_sens_q50^2 + burn_sens_q50^2),
    sens_q75 = sqrt(dsf_sens_q75^2 + burn_sens_q75^2),
    sens_q95 = sqrt(dsf_sens_q95^2 + burn_sens_q95^2)
  ) %>%
  pivot_longer(
    cols      = c(sens_q05, sens_q25, sens_q50, sens_q75, sens_q95),
    names_to  = "quantile",
    values_to = "sensitivity"
  ) %>%
  mutate(
    quantile = recode(quantile,
      sens_q05 = "5th percentile",
      sens_q25 = "25th percentile",
      sens_q50 = "50th percentile",
      sens_q75 = "75th percentile",
      sens_q95 = "95th percentile"
    ),
    quantile = factor(quantile, levels = c("5th percentile",
                                            "25th percentile",
                                            "50th percentile",
                                            "75th percentile",
                                            "95th percentile"))
  )



head(heatmap_sensitivity)
hist(heatmap_sensitivity$dsf_sens_q50, na.rm = TRUE)
# plot
ggplot(heatmap_sensitivity, aes(x = dsf_unscaled, y = pct_burned_unscaled,
                                  fill = sensitivity)) +
  geom_tile() +
  facet_wrap(~ quantile) +
  scale_fill_viridis_c(option = "magma", na.value = "grey90") +
  labs(
    x     = "Days since fire",
    y     = "Percent burned",
    fill  = "Sensitivity\n(|Δ CQ residual|)",
    title = "Sensitivity of CQ residuals to area burned and recovery time"
  ) +
  theme_bw() +
  theme(strip.text = element_text(face = "bold"))
hist(heatmap_sensitivity$sensitivity)
ggsave("heatmap_sensitivity-bothdir.png", width = 8, height = 6, dpi = 300)
###ecoregion-level sensitivity
new_data_eco <- models_nitrate %>%
  data_grid(
    dsf_scaled = seq_range(dsf_scaled, n = 50),
    pct_burned = seq_range(pct_burned, n = 50), 
    cq_status  = "neutral",
    ecoregion = unique(models_nitrate$ecoregion)
  )

preds_eco <- new_data_eco %>% 
  add_predicted_draws(
    fit1, 
    re_formula        = NULL,
    allow_new_levels  = TRUE,
    ndraws            = 500
  )
str(preds_eco)
colnames(preds_eco)

eco_sensitivity <- preds_eco %>%
  group_by(ecoregion,dsf_scaled,pct_burned) %>%
  summarise(
    q05 = quantile(.prediction, 0.05),
    q25 = quantile(.prediction, 0.25),
    q50 = quantile(.prediction, 0.50),
    q75 = quantile(.prediction, 0.75),
    q95 = quantile(.prediction, 0.95),
    .groups = "drop"
  ) %>%
   mutate(
    dsf_unscaled        = (dsf_scaled  * dsf_sd)  + dsf_mean,
    pct_burned_unscaled = (pct_burned  * brn_sd)  + brn_mean,
    across(c(q05, q25, q50, q75, q95), ~ (. * y_sd) + y_mean)
  ) %>%
  arrange(dsf_unscaled, pct_burned_unscaled) %>%
  group_by(pct_burned_unscaled) %>%
  mutate(across(c(q05, q25, q50, q75, q95), 
                ~ abs(. - lag(.)) / (dsf_unscaled - lag(dsf_unscaled)),
                .names = "dsf_sens_{.col}")) %>%
  ungroup() %>%
  group_by(dsf_unscaled) %>%
  mutate(across(c(q05, q25, q50, q75, q95),
                ~ abs(. - lag(.)) / (pct_burned_unscaled - lag(pct_burned_unscaled)),
                .names = "burn_sens_{.col}")) %>%
  ungroup() %>%
  # combine into total sensitivity (magnitude of gradient)
  mutate(
    sens_q05 = sqrt(dsf_sens_q05^2 + burn_sens_q05^2),
    sens_q25 = sqrt(dsf_sens_q25^2 + burn_sens_q25^2),
    sens_q50 = sqrt(dsf_sens_q50^2 + burn_sens_q50^2),
    sens_q75 = sqrt(dsf_sens_q75^2 + burn_sens_q75^2),
    sens_q95 = sqrt(dsf_sens_q95^2 + burn_sens_q95^2)
  ) %>%
  pivot_longer(
    cols      = c(sens_q05, sens_q25, sens_q50, sens_q75, sens_q95),
    names_to  = "quantile",
    values_to = "sensitivity"
  ) %>%
  mutate(
    quantile = recode(quantile,
      sens_q05 = "5th percentile",
      sens_q25 = "25th percentile",
      sens_q50 = "50th percentile",
      sens_q75 = "75th percentile",
      sens_q95 = "95th percentile"
    ),
    quantile = factor(quantile, levels = c("5th percentile",
                                            "25th percentile",
                                            "50th percentile",
                                            "75th percentile",
                                            "95th percentile"))
  )

str(eco_sensitivity)
unique(eco_sensitivity$quantile)
unique(eco_sensitivity$ecoregion)

hist(eco_sensitivity$residual[eco_sensitivity$ecoregion == "COLD DESERTS" & eco_sensitivity$quantile == "5th percentile"])

hist(eco_sensitivity$residual[eco_sensitivity$ecoregion == "MEDITERRANEAN CALIFORNIA" & eco_sensitivity$quantile == "5th percentile"])


eco_sensitivity_summary <- eco_sensitivity %>%
  group_by(ecoregion, quantile) %>%
  summarise(median_residual = median(residual)) %>%
  arrange(median_residual)

##this is the residual-not the sensitivity 
ggplot(eco_sensitivity_summary, aes(x = ecoregion, y = median_residual, fill = ecoregion)) +
  geom_bar(stat = "identity") +
  facet_wrap(~ quantile) +
  labs(x = "Ecoregion", y = "Predicted CQ residual", title = "Ecoregion-level variation in CQ residuals") +
  theme_bw() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1), legend.position = "none")

hist(eco_sensitivity$residual)






####individual direction sensitivities
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
    dsf_unscaled        = (dsf_scaled  * 291.9441) + 557.7047,
    pct_burned_unscaled = (pct_burned  * brn_sd)  + brn_mean,
    across(c(q05, q25, q50, q75, q95), ~ (. * y_sd) + y_mean)
  ) %>%
  # for dsf sensitivity: sort by pct_burned first, then dsf so lag moves along dsf axis
  arrange(pct_burned_unscaled, dsf_unscaled) %>%
  group_by(pct_burned_unscaled) %>%
  mutate(across(c(q05, q25, q50, q75, q95), 
                ~ abs(. - lag(.)) / abs(dsf_unscaled - lag(dsf_unscaled)),
                .names = "dsf_sens_{.col}")) %>%
  ungroup() %>%
  # for burn sensitivity: sort by dsf first, then pct_burned so lag moves along burn axis
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
      burn_sens = "Burnt area"
    ),
    quantile = recode(quantile,
      q05 = "5th percentile",  q25 = "25th percentile",
      q50 = "50th percentile", q75 = "75th percentile",
      q95 = "95th percentile"
    ),
    quantile  = factor(quantile,  levels = c("5th percentile", "25th percentile",
                                              "50th percentile", "75th percentile",
                                              "95th percentile")),
    direction = factor(direction, levels = c("Days since fire", "Burnt area"))
  )

str(heatmap_sensitivity)

heatmap_sensitivity %>% 
  filter(direction == "Days since fire") %>%
  ggplot(aes(x = dsf_unscaled, y = pct_burned_unscaled, fill = sensitivity)) +
  geom_tile() +
  facet_wrap(~ quantile) +
  scale_fill_viridis_c(option = "magma", na.value = "grey90") +
  theme_bw()

heatmap_sensitivity %>% 
  filter(direction == "Burnt area") %>%
  ggplot(aes(x = dsf_unscaled, y = pct_burned_unscaled, fill = sensitivity)) +
  geom_tile() +
  facet_wrap(~ quantile) +
  scale_fill_viridis_c(option = "magma", na.value = "grey90") +
  theme_bw()


# or plot both directions as rows
ggplot(heatmap_sensitivity, aes(x = dsf_unscaled, y = pct_burned_unscaled, fill = sensitivity)) +
  geom_tile() +
  facet_grid(~  quantile + direction) +
  scale_fill_viridis_c(option = "magma", na.value = "grey90") +
  theme_bw()

summary(fit1)
ranef(fit1)
tst <- gather_draws(fit1, r_ecoregion) #, r_ecoregion[ecoregion, term]) 
tst$`ecoregion:usgs_site`[,,"Intercept"]

###extract the ecoregion-level intercepts for plotting
eco_n <- df %>%
  group_by(ecoregion) %>%
  summarise(n = n())

eco_draws <- fit1 %>%
  spread_draws(b_Intercept, `r_ecoregion:usgs_site`) %>%
  #filter(term == "Intercept") %>%
  mutate(
    intercept  = b_Intercept + r_ecoregion,
    ecoregion1  = gsub("\\.", " ", ecoregion),
    ecoregion = recode(ecoregion1, !!!eco_labels)
  ) %>%
  left_join(eco_n, by = "ecoregion") %>%
  mutate(
    ecoregion   = factor(ecoregion, levels = names(eco_colours)),
    eco_n_label = paste0(ecoregion, "\n(n = ", n, ")")
  )
