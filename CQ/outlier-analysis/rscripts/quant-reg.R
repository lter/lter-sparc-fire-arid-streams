library(quantreg)
library(tidyverse)
library(devtools)
devtools::install_github("strengejacke/sjPlot")
library(sjPlot)
#install.packages("lqmm")
library(lqmm)

setwd("~/Documents/projects/CRASS")
data <- read.csv("nitrate_cq_residuals_with_ecoregion.csv",header=T)

head(data)
unique(data$usgs_site)
#Residual ~ time since fire*cumulative % burn + ecoregion + CQ status (diluting, flushing, chemostatic*) + 1|catchment (threshold of 1 and -1) 

# scale and fix structure
df <- read_csv("/Users/ash/Documents/projects/CRASS/nitrate_cq_residuals_with_ecoregion_.2slope.csv") %>%
  mutate(
    catchment    = factor(usgs_site),
    ecoregion    = factor(ecoregion_name),
    cq_status    = factor(slopedir, levels = c("neutral", "positive", "negative")),
    # "neutral" (chemostatic analog) as reference level
    # scale continuous predictors — strongly recommended at tau=0.95
    rs_scaled    = scale(rs)[, 1],
    slope_scaled = scale(slope)[, 1],
    dsf_scaled   = scale(days_since_last_fire)[, 1],
    burn_scaled  = scale(total_burned)[, 1]
  )
head(df)
nrow(df)

df_postfire <- df %>% filter(days_since_last_fire > 0)
nrow(df_postfire)

df_postfire[is.na(df_postfire$catchment),]

fit_q5_post <- lqmm(rs_scaled ~ dsf_scaled * burn_scaled, random = ~ 1, group = catchment, tau = 0.5, data = df_postfire)
summary(fit_q5_post)

fit_q95_post <- lqmm(rs_scaled ~ dsf_scaled * burn_scaled * cq_status + ecoregion, random = ~ 1, group = catchment, tau = 0.95, data = df_postfire)
summary(fit_q95_post)

library(lme4)

mdl_glm_inter <- lmer(rs_scaled ~ dsf_scaled * burn_scaled * cq_status + (1|ecoregion),
data = df_postfire)

mdl_glm_add <- lmer(rs_scaled ~ dsf_scaled * burn_scaled * cq_status + (1|ecoregion),
data = df_postfire)
hist(df_postfire$slope_scaled)

plot_model(mdl_glm_inter, type="pred")
plot_model(mdl_glm_add, type="pred")
plot_model(mdl_glm_inter,type="int")
plot_model(mdl_glm_add,type="int")
plot_model(mdl_glm_inter,sort.est = TRUE,show.values = TRUE, value.offset = .3,title="All coefficients on residual C-Q relationship",axis.title = "Coefficient estimate (scaled predictors)")
plot_model(mdl_glm_add,sort.est = TRUE,show.values = TRUE, value.offset = .3,title="All coefficients on residual C-Q relationship",axis.title = "Coefficient estimate (scaled predictors)")



summary(mdl_glm_add)

mdl_glm
ranef(mdl_glm)
ints <- ranef(mdl_glm_add)
str(ints)
head(ints)
hist(ints$`ecoregion:catchment`$`(Intercept)`) #,breaks=100) #long lefthand tail


set.seed(42)
boot_q95 <- boot.lqmm(fit_q95_post, R = 500, seed = 42, startQR = TRUE)
summary(boot_q95, alpha = 0.05)

taus <- c(0.05,0.25, 0.50, 0.75, 0.90, 0.95)

multi_fits <- lapply(taus, function(tau) {
  lqmm(
    fixed   = rs ~ dsf_scaled * burn_scaled + ecoregion + cq_status,
    random  = ~ 1,
    group   = catchment,
    tau     = tau,
    data    = df_postfire
  )
})
names(multi_fits) <- paste0("tau_", taus)

coef_table <- map_dfr(taus, function(tau) {
  m <- multi_fits[[paste0("tau_", tau)]]
  tibble(
    tau  = tau,
    coef = coef(m)["dsf_scaled:burn_scaled"]
  )
})

ggplot(coef_table, aes(x = tau, y = coef)) +
  geom_line(colour = "#534AB7") +
  geom_point(size = 3, colour = "#534AB7") +
  geom_hline(yintercept = 0, linetype = "dashed", colour = "grey50") +
  labs(
    x     = "Quantile (τ)",
    y     = "Interaction coefficient\n(days since fire × cumulative % burn)",
    title = "Interaction effect on C-Q residuals across quantiles"
  ) +
  theme_bw()



hist(data$rs)
hist(data$days_since_last_fire)
hist(data$total_burned)
hist(data$slope)
hist(data$ecoregion_code)
