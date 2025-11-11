library(googlesheets4)
library(ggplot2)
library(dplyr)
library(tidyr)
library(broom)
library(vroom)
library(lubridate)
library(purrr)

prefire_url <- "https://docs.google.com/spreadsheets/d/1WXfPvt0YZtr2kzkYJPez2rUXioOAbDdHwC4d2zAvTmg"
prefire_data <- read_sheet(prefire_url)
site_ids <- unique(prefire_data$usgs_site)

preflow_data <- read.csv("nitrate_discharge_pre.csv")
postfire_data <- read.csv("nitrate_fires_post.csv")
postflow_data <- read.csv("nitrate_discharge_post.csv")
nitrate_data <- read.csv("nitrate.csv")

##left join based on date and usgs_site

# Make sure both "date" columns are the same name and type
preflow_data <- preflow_data %>%
  rename(date = Date) %>%            # rename Date -> date
  mutate(date = as.Date(date))       # ensure it's a Date class
postflow_data <- postflow_data %>%
  rename(date = Date) %>%            # rename Date -> date
  mutate(date = as.Date(date))  

nitrate_data <- nitrate_data %>%
  mutate(date = as.Date(date))       # ensure same format

# Left join on both keys (preflow)
preflow_nitrate <- nitrate_data %>%
  left_join(preflow_data, by = c("usgs_site", "date"))
postflow_nitrate <- nitrate_data %>%
  left_join(postflow_data, by = c("usgs_site", "date"))

#deleting rows with no flow 
preflow_nitrate <- preflow_nitrate %>%
  filter(!is.na(Flow), Flow > 0,
         !is.na(value_std), value_std > 0)

postflow_nitrate <- postflow_nitrate %>%
  filter(!is.na(Flow), Flow > 0,
         !is.na(value_std), value_std > 0)



#regression model for pre-fire data 
pre_model <- lm(log(value_std) ~ log(Flow), data = preflow_nitrate)

pred_df <- tibble(
  Flow = seq(min(preflow_nitrate$Flow), max(preflow_nitrate$Flow), length.out = 300)
)
pred_ci <- predict(pre_model, newdata = pred_df, interval = "confidence")
pred_plot <- pred_df %>%
  mutate(
    logFlow = log(Flow),
    fit  = pred_ci[, "fit"],
    lwr  = pred_ci[, "lwr"],
    upr  = pred_ci[, "upr"]
  )


# 3) Plot points, regression line, and confidence band
ggplot(preflow_nitrate, aes(x = log(Flow), y = log(value_std))) +
  geom_point(alpha = 0.5) +
  geom_ribbon(data = pred_plot, aes(x = logFlow, ymin = lwr, ymax = upr),
              inherit.aes = FALSE, alpha = 0.2) +
  geom_line(data = pred_plot, aes(x = logFlow, y = fit),
            inherit.aes = FALSE, linewidth = 1) +
  labs(
    title = "C–Q (Pre-fire): log(C) ~ log(Q)",
    x = "log(Q) (discharge)", y = "log(C) (nitrate concentration)"
  ) +
  theme_minimal()



####by site####
# 1) Make a prediction grid per site
grid_by_site <- preflow_nitrate %>%
  group_by(usgs_site) %>%
  summarize(flow_min = min(Flow), flow_max = max(Flow), .groups = "drop") %>%
  mutate(grid = map2(flow_min, flow_max, ~ tibble(Flow = seq(.x, .y, length.out = 300)))) %>%
  select(usgs_site, grid) %>%
  unnest(grid)

# 2) Fit per-site models and predict on each site's grid
models_by_site <- preflow_nitrate %>%
  group_by(usgs_site) %>%
  group_map(~ lm(log(value_std) ~ log(Flow), data = .x), .keep = TRUE)

names(models_by_site) <- preflow_nitrate %>% distinct(usgs_site) %>% pull()

pred_by_site <- grid_by_site %>%
  group_split(usgs_site) %>%
  imap_dfr(function(df, i) {
    m <- models_by_site[[i]]
    ci <- predict(m, newdata = df, interval = "confidence")
    df %>%
      mutate(
        usgs_site = unique(df$usgs_site),
        logFlow = log(Flow),
        fit = ci[, "fit"], lwr = ci[, "lwr"], upr = ci[, "upr"]
      )
  })

# 3) Plot per-site points + bands
cq_plot <- ggplot() +
  geom_point(data = preflow_nitrate,
             aes(x = log(Flow), y = log(value_std)), alpha = 0.5) +
  geom_ribbon(data = pred_by_site,
              aes(x = logFlow, ymin = lwr, ymax = upr), alpha = 0.2) +
  geom_line(data = pred_by_site,
            aes(x = logFlow, y = fit), linewidth = 1) +
  facet_wrap(~ usgs_site, scales = "free") +
  labs(
    title = "Nitrate C–Q (preflow)",
    x = "log(Q)", y = "log(C)"
  ) +
  theme_minimal()

ggsave("~/Desktop/CQ_preflow.png", cq_plot,
       width = 12, height = 8, dpi = 300, bg = "white")

#faceted sites post-flow points
# Get sites present in both datasets
common_sites <- intersect(unique(preflow_nitrate$usgs_site),
                          unique(postflow_nitrate$usgs_site))

# Keep only those sites in postflow_nitrate
postflow_common <- postflow_nitrate %>%
  filter(usgs_site %in% common_sites)
#log for postflow points (shared with preflow)
postflow_common <- postflow_common %>%
  mutate(logFlow = log(Flow),
         logC = log(value_std))

# --- Classify post-fire points relative to pre-fire CI ---
post_check <- postflow_common %>%
  group_by(usgs_site) %>%
  group_modify(~ {
    preds <- pred_by_site %>% filter(usgs_site == unique(.x$usgs_site))
    
    # for each post point, find nearest logFlow in pre predictions
    .x %>%
      rowwise() %>%
      mutate(
        idx = which.min(abs(logFlow - preds$logFlow)),
        fit = preds$fit[idx],
        lwr = preds$lwr[idx],
        upr = preds$upr[idx],
        outside = ifelse(logC < lwr | logC > upr, "Outside CI", "Inside CI")
      ) %>%
      ungroup()
  }) %>%
  ungroup()

# --- Plot per-site ---
cq_plot_compare <- ggplot() +
  geom_point(data = preflow_nitrate,
             aes(x = log(Flow), y = log(value_std)),
             alpha = 0.4, color = "blue") +
  geom_ribbon(data = pred_by_site,
              aes(x = logFlow, ymin = lwr, ymax = upr),
              alpha = 0.2, fill = "grey70") +
  geom_line(data = pred_by_site,
            aes(x = logFlow, y = fit),
            linewidth = 1, color = "black") +
  geom_point(data = post_check_common,
             aes(x = logFlow, y = logC, color = outside),
             size = 2) +
  scale_color_manual(values = c("Outside CI" = "red", "Inside CI" = "darkgreen")) +
  facet_wrap(~ usgs_site, scales = "free") +
  labs(
    title = "Nitrate C–Q: Pre-fire fit with Post-fire overlay",
    subtitle = "Red = post-fire points outside pre-fire 95% CI",
    x = "log(Q)", y = "log(C)"
  ) +
  theme_minimal() +
  theme(
    panel.background = element_rect(fill = "white"),
    plot.background  = element_rect(fill = "white")
  )

# Save
ggsave("~/Desktop/CQ_pre_vs_post.png", cq_plot_compare,
       width = 14, height = 9, dpi = 300, bg = "white")













post_check_site <- map_dfr(unique(postflow_common$usgs_site), function(site_id) {
  
  post_site <- postflow_common %>% filter(usgs_site == site_id)
  preds_site <- pred_by_site %>% filter(usgs_site == site_id)
  
  if(nrow(preds_site) == 0){
    # No pre-fire regression for this site
    post_site %>% mutate(outside = NA_character_)
  } else {
    post_site %>%
      rowwise() %>%
      mutate(
        idx = which.min(abs(logFlow - preds_site$logFlow)),
        outside = ifelse(logC < preds_site$lwr[idx] | logC > preds_site$upr[idx],
                         "Outside CI", "Inside CI")
      ) %>%
      ungroup()
  }
})


ggplot() +
  # pre-fire points
  geom_point(data = preflow_nitrate,
             aes(x = log(Flow), y = log(value_std)),
             color = "blue", alpha = 0.5) +
  # pre-fire regression ribbon
  geom_ribbon(data = pred_by_site,
              aes(x = logFlow, ymin = lwr, ymax = upr),
              alpha = 0.2) +
  # pre-fire regression line
  geom_line(data = pred_by_site,
            aes(x = logFlow, y = fit),
            linewidth = 1) +
  # post-fire points, only color by CI status
  geom_point(data = post_check_site,
             aes(x = logFlow, y = logC, color = outside),
             size = 2) +
  # CI color mapping
  scale_color_manual(values = c("Outside CI" = "red", "Inside CI" = "black")) +
  facet_wrap(~ usgs_site, scales = "free") +  # site indicated in facet title
  labs(title = "Per-site C–Q: Pre-fire regression with Post-fire overlay",
       x = "log(Q) (discharge)",
       y = "log(C) (nitrate concentration)",
       color = "Post-fire vs Pre-fire CI") +  # only one legend
  theme_minimal() +
  theme(legend.position = "top")  # optional: move legend to top for compactness


# Keep pre-flow rows only for sites that are in postflow_common
preflow_common <- preflow_nitrate %>%
  filter(usgs_site %in% unique(postflow_common$usgs_site))









#confidence band
pre_confidence <- augment(pre_model, interval = "confidence")

postflow_nitrate <- postflow_nitrate %>%
  mutate(logQ = log(Flow),
         logC = log(value_std))


#merge pre-fire
#combining data
# Convert Date in flow datasets to Date type
preflow_data <- preflow_data %>%
  mutate(Date = as.Date(Date))

postflow_data <- postflow_data %>%
  mutate(Date = as.Date(Date))

nitrate_data <- nitrate_data %>%
  filter(analyte == "nitrate") %>%        # keep only nitrate
  rename(Date = date,
         nitrate_mgL = value_std) %>%     # rename for clarity
  mutate(Date = as.Date(Date))            # make sure it's a Date object

combined_predata <- preflow_data %>%
  left_join(prefire_data, by = "usgs_site") %>%
  left_join(nitrate_data, by = c("usgs_site", "Date"))

combined_postdata <- postflow_data %>%
  left_join(postfire_data, by = "usgs_site") %>%
  left_join(nitrate_data, by = c("usgs_site", "Date"))




# 1. Fit pre-fire models per site
models <- combined_predata %>%
  group_by(usgs_site) %>%
  nest() %>%
  mutate(model = map(data, ~ lm(nitrate_mgL ~ Flow, data = .)))

# 2. Create prediction grid per site for plotting regression + CI
pred_df <- models %>%
  mutate(preds = map2(model, data, ~ {
    flow_seq <- seq(min(.y$Flow, na.rm = TRUE),
                    max(.y$Flow, na.rm = TRUE),
                    length.out = 100)
    aug <- augment(.x, newdata = data.frame(Flow = flow_seq), se_fit = TRUE)
    aug %>%
      mutate(lower = .fitted - 1.96 * .se.fit,
             upper = .fitted + 1.96 * .se.fit)
  })) %>%
  select(usgs_site, preds) %>%
  unnest(preds)

# 3. Predict for post-fire points & flag outside CI
post_with_flags <- combined_postdata %>%
  left_join(models %>% select(usgs_site, model), by = "usgs_site") %>%
  mutate(pred = map2_dbl(model, Flow, ~ predict(.x, newdata = data.frame(Flow = .y),
                                                se.fit = TRUE)$fit),
         se = map2_dbl(model, Flow, ~ predict(.x, newdata = data.frame(Flow = .y),
                                              se.fit = TRUE)$se.fit),
         lower = pred - 1.96 * se,
         upper = pred + 1.96 * se,
         outside_CI = nitrate_mgL < lower | nitrate_mgL > upper)

# 4. Plot
ggplot() +
  # Confidence band
  geom_ribbon(data = pred_df, aes(x = Flow, ymin = lower, ymax = upper),
              fill = "lightblue", alpha = 0.3) +
  # Pre-fire points
  geom_point(data = combined_predata, aes(x = Flow, y = nitrate_mgL),
             color = "blue", size = 1.5) +
  # Regression line
  geom_line(data = pred_df, aes(x = Flow, y = .fitted), color = "darkblue") +
  # Post-fire points
  geom_point(data = post_with_flags,
             aes(x = Flow, y = nitrate_mgL, color = outside_CI),
             size = 2) +
  scale_color_manual(values = c("FALSE" = "orange", "TRUE" = "red")) +
  facet_wrap(~ usgs_site, scales = "free") +
  labs(title = "C-Q Relationships: Pre-fire regression + Post-fire points",
       x = "Discharge (Flow)", y = "Nitrate (mg/L)",
       color = "Outside 95% CI?") +
  theme_minimal()






# Add fire status labels
combined_predata <- combined_predata %>% mutate(fire_status = "Pre-fire")
combined_postdata <- combined_postdata %>% mutate(fire_status = "Post-fire")

combined_predata  <- combined_predata  %>% mutate(start_date = as.Date(start_date))
combined_postdata <- combined_postdata %>% mutate(start_date = as.Date(start_date))

# Combine into one dataframe
combined_data <- bind_rows(combined_predata, combined_postdata)

# Plot C–Q relationship
ggplot(combined_data, aes(x = Flow, y = nitrate_mgL, color = fire_status)) +
  geom_point(alpha = 0.7) +
  scale_x_log10() +
  scale_y_log10() +
  facet_wrap(~ usgs_site, scales = "free") +
  labs(title = "Nitrate Concentration–Discharge (C–Q) Relationship",
       x = "Discharge (cfs, log scale)",
       y = "Nitrate (mg/L, log scale)",
       color = "Fire Status") +
  theme_minimal()



#combining data
combined_predata <- left_join(preflow_data, prefire_data, by = "usgs_site")
combined_postdata <- left_join(postflow_data, postfire_data, by = "usgs_site")

colnames(nitrate_data)


# Plot discharge over time, colored by site
ggplot(combined_data, aes(x = Date, y = Flow, color = usgs_site)) +
  geom_line() +
  labs(title = "Discharge Over Time by USGS Site",
       x = "Date",
       y = "Discharge (cfs)",
       color = "USGS Site") +
  theme_minimal()
