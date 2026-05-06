#libraries
library(broom)
library(tidyverse)
## ── Attaching core tidyverse packages ──────────────────────── tidyverse 2.0.0 ──
## ✔ dplyr     1.1.4     ✔ readr     2.1.5
## ✔ forcats   1.0.0     ✔ stringr   1.5.1
## ✔ ggplot2   4.0.0     ✔ tibble    3.3.0
## ✔ lubridate 1.9.4     ✔ tidyr     1.3.1
## ✔ purrr     1.0.4     
## ── Conflicts ────────────────────────────────────────── tidyverse_conflicts() ──
## ✖ dplyr::filter() masks stats::filter()
## ✖ dplyr::lag()    masks stats::lag()
## ℹ Use the conflicted package (<http://conflicted.r-lib.org/>) to force all conflicts to become errors
library(here)
## here() starts at /Users/shanthiniode/Documents/SoCalBurn/SoCalBurn
library(lme4)
## Loading required package: Matrix
## 
## Attaching package: 'Matrix'
## 
## The following objects are masked from 'package:tidyr':
## 
##     expand, pack, unpack
library(ggplot2)
library(dplyr)
library(emmeans)
## Welcome to emmeans.
## Caution: You lose important information if you filter this package's results.
## See '? untidy'
library(patchwork)
library(car)
## Loading required package: carData
## 
## Attaching package: 'car'
## 
## The following object is masked from 'package:dplyr':
## 
##     recode
## 
## The following object is masked from 'package:purrr':
## 
##     some
library(MuMIn)
library(brms)
## Loading required package: Rcpp
## Loading 'brms' package (version 2.23.0). Useful instructions
## can be found by typing help('brms'). A more detailed introduction
## to the package is available through vignette('brms_overview').
## 
## Attaching package: 'brms'
## 
## The following object is masked from 'package:MuMIn':
## 
##     loo
## 
## The following object is masked from 'package:lme4':
## 
##     ngrps
## 
## The following object is masked from 'package:stats':
## 
##     ar
library(tidybayes)
## 
## Attaching package: 'tidybayes'
## 
## The following objects are masked from 'package:brms':
## 
##     dstudent_t, pstudent_t, qstudent_t, rstudent_t
options(scipen = 999)
allchems <- read.csv("data/raw/allchems_loq.csv")
#final_Q <- read.csv("Data/final_Q.csv") 
#pca <- read.csv("Data/pca_scores.csv")
# Convert data to long if in wide format to start (solute name in 1 column, solute concentration in 1 column)
allchems_long <- allchems %>% pivot_longer(cols = c(avg_NPOC, avg_TN, SRP_uM, Chloride_mgL, Sulfate_mgL, NitrateN_mgL), names_to = "solute", values_to = "concentration")
#cleaning if no discharge data 
allchems_long <- allchems_long %>%
  filter(!is.na(Q..L.s.))
monthly_sites <- c("arroyohondo", "coldwater", "deep", "mission", "ortega", "rattlesnake", "refugio", 
                   "santaynez", "santiago","sedgwick", "silverado", "stunt", "topanga", "waterman", "zuma") #exclusing trabuco since only 2 points -- not good for CQ slope 

allchems_filtered <- allchems_long %>%
  #  filter(Site %in% monthly_sites) %>%
  filter(concentration > 0, Q..L.s. > 0)

# Choose solutes for analysis, group and nest data. For SoCal burns, group by site and solute
Qchemsitey <- allchems_filtered %>% 
  filter(solute %in% (c("avg_NPOC", "avg_TN", "SRP_uM", "Chloride_mgL", "Sulfate_mgL", "NitrateN_mgL"))) %>%
  group_by(Site, solute) %>% nest()
# function to perform C-Q regression
CQ <- function(df){lm(log(concentration) ~ log(Q..L.s.), data = df)}

# run models on grouped data
CQmods <- Qchemsitey %>% mutate(model = map(data, CQ))
biome_key <- pca %>%
  dplyr::select(Site, biome_pca) %>%
  distinct()

CQ_plot_data <- CQmods %>%
  mutate(augment = map(model, broom::augment)) %>%
  unnest(augment)

CQ_raw_data <- Qchemsitey %>% 
  unnest(data) %>%
  select(-biome_pca) %>%           # drop the existing one first
  left_join(biome_key, by = "Site")

#one plot per solute, faceted by site
solute_labels <- c(
  "avg_NPOC" = "DOC",
  "avg_TN" = "TN",
  "SRP_uM" = "SRP",
  "Chloride_mgL" = "Chloride",
  "Sulfate_mgL" = "Sulfate",
  "NitrateN_mgL" = "Nitrate")

biome_colors <- c("shrub_grass" = "dodgerblue3", "forest" = "darkgreen")
###Adapted Ara Baye’s Code

theme_set(theme_bw() +
            theme(
              plot.background = element_blank()
              ,panel.grid.major = element_blank()
              ,panel.grid.minor = element_blank()
              ,panel.background = element_blank()
              ,panel.border = element_blank()
              ,axis.text.x  = element_text(angle=90, vjust=0.5, size=8)
              ,axis.ticks = element_blank()
              ,strip.background = element_rect()))
scaled <- CQ_raw_data %>% filter(Q..L.s. > 0 & concentration > 0) %>% 
  #Log-only transforms
  mutate(
    log_flow = log(Q..L.s.),           # log Q
    log_concentration = log(concentration)       # log concentration
  )

#get one row per site and compute z-scores there
site_level <- scaled %>%
  group_by(Site) %>%
  slice(1) %>%
  ungroup() %>%
  mutate(
    z_pct_burned   = scale(pct_burned)[,1],
    z_mean_slope = scale(mean_slope)[,1],
    z_time_since = scale(time_since)[,1]
  ) %>%
  select(Site, z_pct_burned, z_mean_slope, z_time_since)

#join back to full dataset
scaled <- scaled %>%
  left_join(site_level, by = "Site")
scaled <- scaled %>%
  mutate(pct_burned = pct_burned / 100)


brms.mod <- bf(log_concentration ~ log_flow*pct_burned + z_time_since + z_mean_slope + (log_flow | Site))
#priors
prior0 <- c(
  prior(normal(0, 1),    class = b),           # fixed effects (incl. cross-level interaction)
  prior(normal(0, 2),    class = Intercept),
  prior(exponential(1),  class = sigma),        # obs-level SD
  prior(exponential(1),  class = sd),           # SD of random effects
  prior(lkj(2),          class = cor)           # correlation of random intercept & slope
)
# Get each ecoregion's actual burn_prop (one value per site)
burn_by_site <- scaled %>%
  group_by(Site) %>%
  summarise(pct_burned = first(pct_burned), .groups = "drop")
solute_labels <- c(
  "avg_NPOC"      = "DOC",
  "avg_TN"        = "TN",
  "SRP_uM"        = "SRP",
  "Chloride_mgL"  = "Chloride",
  "Sulfate_mgL"   = "Sulfate",
  "NitrateN_mgL"  = "Nitrate"
)

# Store results
fits        <- list()
cq_slopes   <- list()
delta_mu    <- list()
delta_site  <- list()
biome_colors <- c("shrub_grass" = "dodgerblue3", "forest" = "darkgreen")
#big loop 
run_cq_analysis <- function(sol, scaled, brms.mod, prior0, solute_labels, fits_env = NULL) {
  
  lab <- solute_labels[[sol]]
  cat("\n\n===== Fitting:", lab, "=====\n")
  
  #Filter data 
  dat <- scaled %>% filter(solute == sol)
  
  #Fit model 
  fit <- brm(brms.mod,
             data    = dat,
             family  = "gaussian",
             prior   = prior0,
             chains  = 4, cores = 8, iter = 2000, backend = "cmdstanr",
             control = list(adapt_delta = 0.95, max_treedepth = 14))
  
  #Summary & pp_check 
  print(summary(fit, prob = 0.5))
  print(pp_check(fit) + ggtitle(paste("pp_check:", lab)))
  
  #Site-level CQ slopes 
  burn_by_site <- dat %>%
    group_by(Site) %>%
    summarise(pct_burned = first(pct_burned), .groups = "drop")
  
  random_slopes <- ranef(fit)$Site[,,"log_flow"] %>%
    as.data.frame() %>%
    rownames_to_column("Site") %>%
    select(Site, r_Site = Estimate)
  
  cq_sl <- fit %>%
    spread_draws(b_log_flow, `b_log_flow:pct_burned`) %>%
    crossing(burn_by_site) %>%
    left_join(random_slopes, by = "Site") %>%
    mutate(
      slope_population = b_log_flow,
      slope_burn       = `b_log_flow:pct_burned` * pct_burned,
      slope_residual   = r_Site,
      slope_total      = slope_population + slope_burn + slope_residual
    ) %>%
    group_by(Site, pct_burned) %>%
    median_qi(slope_total, slope_burn, slope_residual, .width = 0.50) %>%
    ungroup() %>%
    arrange(slope_total) %>%
    mutate(solute = sol)
  
  #Plot: site-level slopes 
  p_slopes <- cq_sl %>%
    left_join(biome_key, by = "Site") %>%
    mutate(Site = fct_reorder(Site, slope_total)) %>%
    ggplot(aes(x = slope_total, y = Site, color = biome_pca)) +
    geom_point() +
    geom_errorbarh(aes(xmin = slope_total.lower, xmax = slope_total.upper), height = 0) +
    geom_vline(xintercept = 0, linetype = "dashed") +
    scale_color_manual(values = biome_colors, name = "Biome") +
    labs(x = "CQ slope (median & 50% CrI)", y = NULL,
         title = paste(lab, "- Site-level CQ slopes"))
  print(p_slopes)
  
  #Plot: CQ slope as function of burn proportion 
  p_burn <- fit %>%
    spread_draws(b_log_flow, `b_log_flow:pct_burned`) %>%
    crossing(pct_burned = seq(0, 1, by = 0.01)) %>%
    mutate(cq_slope = b_log_flow + `b_log_flow:pct_burned` * pct_burned) %>%
    group_by(pct_burned) %>%
    median_qi(cq_slope, .width = c(0.80, 0.95)) %>%
    ggplot(aes(pct_burned, cq_slope)) +
    geom_ribbon(aes(ymin = .lower, ymax = .upper, group = .width,
                    alpha = factor(.width)), fill = "steelblue") +
    scale_alpha_manual(values = c("0.8" = 0.4, "0.95" = 0.15),
                       labels = c("80% CrI", "95% CrI"),
                       name = NULL) +
    geom_line() +
    geom_hline(yintercept = 0, linetype = "dashed", color = "red") +
    annotate("text", x = 0.05, y  = 0.02, label = "mobilization", color = "red", hjust = 0, size = 3) +
    annotate("text", x = 0.05, y = -0.02, label = "dilution",     color = "red", hjust = 0, size = 3) +
    labs(x = "Proportion burned", y = "CQ slope",
         title = paste(lab, "- CQ slope vs burn proportion"),
         caption = "Ribbons = 80% and 95% CrI")
  print(p_burn)
  
  #Plot: overlay on sites 
  cq_sl_biome <- cq_sl %>% left_join(biome_key, by = "Site")
  
  p_overlay <- fit %>%
    spread_draws(b_log_flow, `b_log_flow:pct_burned`) %>%
    crossing(pct_burned = seq(0, 1, by = 0.01)) %>%
    mutate(cq_slope = b_log_flow + `b_log_flow:pct_burned` * pct_burned) %>%
    group_by(pct_burned) %>%
    median_qi(cq_slope, .width = c(0.80, 0.95)) %>%
    ggplot(aes(pct_burned, cq_slope)) +
    geom_ribbon(aes(ymin = .lower, ymax = .upper, group = .width,
                    alpha = factor(.width)), fill = "darkgray") +
    scale_alpha_manual(values = c("0.8" = 0.4, "0.95" = 0.15),
                       labels = c("80% CrI", "95% CrI"),
                       name = NULL) +
    geom_line() +
    geom_hline(yintercept = 0, linetype = "dashed", color = "red") +
    geom_pointrange(data = cq_sl_biome,
                    aes(x = pct_burned, y = slope_total,
                        ymin = slope_total.lower, ymax = slope_total.upper,
                        color = biome_pca), size = 0.4) +
    ggrepel::geom_text_repel(data = cq_sl_biome,
                             aes(x = pct_burned, y = slope_total, 
                                 label = Site, color = biome_pca),
                             size = 2.5) +
    scale_color_manual(values = biome_colors, name = "Biome") +
    labs(x = "Proportion burned", y = "CQ slope",
         title = paste(lab, "- CQ slope vs burn proportion with sites"),
         caption = "Dashed line = chemostatic boundary")
  print(p_overlay)
  
  #Plot: predicted CQ at % burn
  p_pred <- fit %>%
    spread_draws(b_Intercept, b_log_flow, b_pct_burned, `b_log_flow:pct_burned`,
                 b_z_time_since, b_z_mean_slope) %>%
    crossing(pct_burned = c(0, 0.25, 0.50, 0.75, 1.0),
             log_flow   = seq(min(dat$log_flow), max(dat$log_flow), length.out = 50)) %>%
    mutate(log_concentration = b_Intercept + b_log_flow * log_flow +
             b_pct_burned * pct_burned +
             `b_log_flow:pct_burned` * log_flow * pct_burned +
             b_z_time_since * 0 + b_z_mean_slope * 0) %>% #held at 0 since standardized
    group_by(pct_burned, log_flow) %>%
    median_qi(log_concentration, .width = 0.50) %>%
    ggplot(aes(log_flow, log_concentration,
               color = factor(pct_burned), fill = factor(pct_burned), group = factor(pct_burned))) +
    geom_ribbon(aes(ymin = .lower, ymax = .upper), alpha = 0.15, color = NA) +
    geom_line(linewidth = 0.8) +
    scale_color_viridis_d(name = "Burn proportion", option = "plasma") +
    scale_fill_viridis_d(name  = "Burn proportion", option = "plasma") +
    labs(x = "log flow", y = paste0("log ", lab),
         title = paste(lab, "- Predicted CQ across burn levels"),
         caption = "Population-level predictions; ribbons = 50% CrI")
  print(p_pred)
  
  #Delta mu: burn effect at low/mean/high flow 
  sd_x   <- sd(dat$log_flow)
  x_star <- c(-sd_x, 0, sd_x)
  
  dmu <- fit %>%
    spread_draws(b_pct_burned, `b_log_flow:pct_burned`) %>%
    crossing(x = x_star) %>%
    mutate(
      flow_condition = case_when(
        x < 0  ~ "low flow (-1 SD)",
        x == 0 ~ "mean flow",
        x > 0  ~ "high flow (+1 SD)"
      ),
      delta_mu = b_pct_burned + `b_log_flow:pct_burned` * x
    ) %>%
    group_by(flow_condition, x) %>%
    median_qi(delta_mu, .width = 0.50) %>%
    ungroup() %>%
    rename(median = delta_mu, lo50 = .lower, hi50 = .upper) %>%
    arrange(x) %>%
    mutate(solute = sol)
  
  p_dmu <- dmu %>%
    mutate(flow_condition = fct_reorder(flow_condition, x)) %>%
    ggplot(aes(x = median, y = flow_condition)) +
    geom_point() +
    geom_errorbarh(aes(xmin = lo50, xmax = hi50), height = 0) +
    geom_vline(xintercept = 0, linetype = "dashed") +
    labs(x = paste0("Δ log-", lab, " (fully burned vs unburned, median & 50% CrI)"),
         y = NULL, title = paste(lab, "- Burn effect across flow conditions"))
  print(p_dmu)
  
  #Delta mu by site 
  dmu_site <- fit %>%
    spread_draws(b_pct_burned, `b_log_flow:pct_burned`) %>%
    crossing(burn_by_site) %>%
    crossing(x = x_star) %>%
    mutate(
      flow_condition = case_when(
        x < 0  ~ "low flow (-1 SD)",
        x == 0 ~ "mean flow",
        x > 0  ~ "high flow (+1 SD)"
      ),
      delta_mu = (b_pct_burned + `b_log_flow:pct_burned` * x) * pct_burned
    ) %>%
    group_by(Site, flow_condition, x, pct_burned) %>%
    median_qi(delta_mu, .width = 0.50) %>%
    ungroup() %>%
    rename(median = delta_mu, lo50 = .lower, hi50 = .upper) %>%
    arrange(Site, x) %>%
    mutate(solute = sol)
  
  p_dmu_site <- dmu_site %>%
    left_join(biome_key, by = "Site") %>%
    mutate(flow_condition = fct_reorder(flow_condition, x),
           Site = fct_reorder(Site, pct_burned)) %>%
    ggplot(aes(x = median, y = flow_condition, color = biome_pca)) +
    geom_point(size = 1.5) +
    geom_errorbarh(aes(xmin = lo50, xmax = hi50), height = 0) +
    geom_vline(xintercept = 0, linetype = "dashed") +
    scale_color_manual(values = biome_colors, name = "Biome") +
    facet_wrap(~ Site, ncol = 4) +
    labs(x = paste0("Δ log-", lab, " (actual burn vs unburned, median & 50% CrI)"),
         y = NULL, title = paste(lab, "- Burn effect by site and flow condition"),
         caption = "Sites ordered by % burned") +
    theme_classic() +
    theme(strip.text = element_text(size = 7),
          axis.text.y = element_text(size = 6),
          axis.text.x = element_text(size = 6),
          panel.spacing = unit(0.5, "lines"))
  ggsave(paste0("Plots/dmu_site_", sol, ".png"), p_dmu_site,
         width = 16, height = 12, dpi = 150)
  
  print(p_dmu_site)
  
  cat("===== Done:", lab, "=====\n")
  
  #Return all results 
  invisible(list(fit        = fit,
                 cq_slopes  = cq_sl,
                 delta_mu   = dmu,
                 delta_site = dmu_site))
}