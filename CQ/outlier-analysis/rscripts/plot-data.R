library(tidyverse)


##all the data
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
    #post  = as.integer(segment == "after"), ###I CANT USE THIS BAD BAD BAD
  )
colnames(df)
hist(df$log_n)
hist(df$value_std)
hist(df$Flow)
hist(df$log_q)

df$post[df$days_since_last_fire > 0]




models_nitrate <- scaled_df %>%
  group_by(usgs_site) %>%
  mutate(rs = resid((lm(log_N_sc ~ log_Q_sc))),
         int = coef(lm(log_N_sc ~ log_Q_sc))[1],
         slope = coef(lm(log_N_sc ~ log_Q_sc))[2],
         slopedir = ifelse(slope >= .2, "positive", 
         ifelse(slope <= -.2 , "negative", "neutral"))) %>% ungroup()


##plot residual distributions by ecoregion and CQ status
ggplot(models_nitrate, aes(x = rs, fill = cq_status)) +
  geom_histogram(bins = 30) +
  facet_grid(ecoregion ~ cq_status) +
  labs(x = "Residuals from log-log C-Q model", y = "Count") 

ggsave("residuals_histogram-faceted.png", width = 8, height = 6, dpi = 300)

##plot the residual against days since fire and cumulative burn, colored by ecoregion and CQ status
ggplot(models_nitrate, aes(x = days_since_last_fire, y = rs, color = cq_status)) +
  geom_point(alpha = 0.5) +
  facet_wrap(~ ecoregion) +
  labs(x = "Days Since Last Fire", y = "Residuals from log-log C-Q model") +
  theme_bw() +
  theme(strip.text = element_text(face = "bold"))
ggsave("residuals_vs_dsf.png", width = 8, height = 6, dpi = 300)

ggplot(models_nitrate, aes(x = total_burned, y = rs, color = cq_status)) +
  geom_point(alpha = 0.5) +
  facet_wrap(~ ecoregion) +
  labs(x = "Total Burned Area", y = "Residuals from log-log C-Q model") +
  theme_bw() +
  theme(strip.text = element_text(face = "bold"))

ggsave("residuals_vs_burn.png", width = 8, height = 6, dpi = 300)

##plot number of fires by cumulative burn and days since fire, colored by ecoregion and CQ status
ggplot(models_nitrate, aes(x = days_since_last_fire, y = total_burned, color = cq_status)) +
  geom_point(alpha = 0.5) +
  facet_wrap(~ ecoregion) +
  labs(x = "Days Since Last Fire", y = "Total Burned Area") +
  theme_bw() +
  theme(strip.text = element_text(face = "bold"))
ggsave("burn_vs_dsf.png", width = 8, height = 6, dpi = 300)

ggplot(models_nitrate, aes(x = num_fires, y = total_burned, color = cq_status)) +
  geom_point(alpha = 0.5) +
  facet_wrap(~ ecoregion) +
  labs(x = "Number of Fires", y = "Total Burned Area") +
  theme_bw() +
  theme(strip.text = element_text(face = "bold"))
ggsave("burn_vs_num_fires.png", width = 8, height = 6, dpi = 300)
