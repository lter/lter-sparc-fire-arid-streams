library(ggplot2)
library(dplyr)
library(tidyr) 
library(broom) 
library(vroom)
library(googlesheets4)

prefire_nitrate_url <- "https://docs.google.com/spreadsheets/d/1Y_pVScjUmpmBtpnK8SGSWPijZhaTzLFOkdnITJFuRps/edit?gid=1194451891#gid=1194451891"
prefire_nitrate <- read_sheet(prefire_nitrate_url)
filteredfires_nitrate_url <-"https://docs.google.com/spreadsheets/d/19o_wQaLYJm_n1vQg_ECbJ8I8ZFnoAPr1_JSujDPvZjU/edit?gid=42808220#gid=42808220"
filteredfires_nitrate <- read_sheet(filteredfires_nitrate_url)
nitratefires_url <- "https://docs.google.com/spreadsheets/d/1rfVYsvFIdzP4vT7xhIOKxInOdqFezYLSKET1SG3Iz5k/edit?gid=676161325#gid=676161325"
nitratefires <- read_sheet(nitratefires_url) 
prefire_ammonium_url <- "https://docs.google.com/spreadsheets/d/1l_cADBLUbLu_XfKlfNTRNYSEkXSwGUi8FXi8p6yDzDM/edit?gid=1949068225#gid=1949068225"
prefire_ammonium <- read_sheet(prefire_ammonium_url)
filteredfires_ammonium_url <- "https://docs.google.com/spreadsheets/d/1Jr6TfguHXzJfbYDhrNPYr1OPoxelgWV5t8Tyh0HVvlQ/edit?gid=1860307665#gid=1860307665"
filteredfires_ammonium <- read_sheet(filteredfires_ammonium_url)
ammoniumfires_url <- "https://docs.google.com/spreadsheets/d/1hKuHVEREtt1mMQqPmysEN7iyx79bf1OtWD1Q61TmKnU/edit?gid=1427499502#gid=1427499502"
ammoniumfires <- read_sheet(ammoniumfires_url)
prefire_orthop_url <- "https://docs.google.com/spreadsheets/d/1Tc3M4RegL3m5S71RUrwrVBWuDBFPNdgrVu7vYtDn3AA/edit?gid=1285577098#gid=1285577098"
prefire_orthop <- read_sheet(prefire_orthop_url)
orthopfires_url <- "https://docs.google.com/spreadsheets/d/1AGESDPDOeBHQauvVSIOOK49IjMt_FOuaxA4UnsPMmQA/edit?gid=38739842#gid=38739842"
orthopfires <- read_sheet(orthopfires_url)

#practice####
#get rid of negative concentration values
prefire_nitrate_cleaned <- prefire_nitrate %>%
  filter(!is.na(value_std), !is.na(Flow),
         value_std > 0, Flow > 0,
         segment  == "before") %>%
  mutate(fire_period = "Prefire")


postfire_nitrate_cleaned <- prefire_nitrate %>%
  filter(!is.na(value_std), !is.na(Flow),
         value_std > 0, Flow > 0,
         segment == "after") %>%
  mutate(fire_period = "Postfire")

#only keep before points
# prefire_data_cleaned <- prefire_data_cleaned %>%
#   filter(segment == "before")
# postfire_data_cleaned <- postfire_data_cleaned %>%
#   filter(segment == "after")

#pre_model <- lm(log(value_std) ~ log(Flow), data = prefire_data_cleaned)

combined_data_nitrate <- bind_rows(prefire_nitrate_cleaned, postfire_nitrate_cleaned)

# 1. Fit prefire models per site
pre_models_nitrate <- prefire_nitrate_cleaned %>%
  group_by(usgs_site) %>%
  summarise(model = list(lm(log(value_std) ~ log(Flow), data = cur_data())), .groups = "drop")

# 2. Predict prefire CI for all postfire points by site
postfire_nitrate_flagged <- postfire_nitrate_cleaned %>%
  left_join(pre_models_nitrate, by = "usgs_site") %>%
  group_by(usgs_site) %>%
  mutate(
    # predict log-space CI for all postfire points of this site
    pred = predict(model[[1]], newdata = tibble(Flow = Flow), interval = "confidence"),
    # convert back to raw space
    lwr = exp(pred[, "lwr"]),
    upr = exp(pred[, "upr"]),
    status = ifelse(value_std < lwr | value_std > upr, "Outside CI", "Inside CI")
  ) %>%
  ungroup() %>%
  select(-model, -pred)


# 3. Postfire points outside CI
postfire_n_outside_df <- postfire_nitrate_flagged %>%
  filter(status == "Outside CI")

# 4. Plot
postfireCIplot <- ggplot() +
  geom_point(data = prefire_data_cleaned,
             aes(x = Flow, y = value_std),
             alpha = 0.6, color = "gray40") +
  geom_smooth(data = prefire_data_cleaned,
              aes(x = Flow, y = value_std),
              method = "lm", formula = y ~ x,
              se = TRUE, color = "blue") +
  geom_point(data = postfire_flagged,
             aes(x = Flow, y = value_std, color = status, shape = status),
             size = 3) +
  scale_color_manual(values = c("Inside CI" = "gray70", "Outside CI" = "red")) +
  scale_shape_manual(values = c("Inside CI" = 16, "Outside CI" = 17)) +
  scale_x_log10() +
  scale_y_log10() +
  facet_wrap(~usgs_site, scales = "free") +
  labs(x = "Discharge (Flow)",
       y = "Nitrate concentration",
       title = "Prefire C-Q with Postfire Points Highlighted",
       color = "Postfire Status",
       shape = "Postfire Status") +
  theme_minimal(base_size = 14)

ggsave("postfireCIplot.png", postfireCIplot, width = 10, height = 6, dpi = 300)


# Find sites with both prefire and postfire data
sites_with_both_n <- postfire_nitrate_cleaned %>%
  distinct(usgs_site) %>%      # sites with postfire data
  inner_join(prefire_nitrate_cleaned %>% distinct(usgs_site), by = "usgs_site") %>%
  pull(usgs_site)

# Filter data for plotting
prefire_filtered_n <- prefire_nitrate_cleaned %>%
  filter(usgs_site %in% sites_with_both_n)

postfire_filtered_n <- postfire_flagged %>%
  filter(usgs_site %in% sites_with_both_n)

# Re-plot with only sites that have both pre and post fire data
NitrateCIplotfiltered <- ggplot() +
  geom_point(data = prefire_filtered_n,
             aes(x = Flow, y = value_std),
             alpha = 0.6, color = "black") +
  geom_smooth(data = prefire_filtered_n,
              aes(x = Flow, y = value_std),
              method = "lm", formula = y ~ x,
              se = TRUE, color = "blue") +
  geom_point(data = postfire_filtered_n,
             aes(x = Flow, y = value_std, color = status, shape = status),
             size = 3) +
  scale_color_manual(values = c("Inside CI" = "gray", "Outside CI" = "red")) +
  scale_shape_manual(values = c("Inside CI" = 16, "Outside CI" = 17)) +
  scale_x_log10() +
  scale_y_log10() +
  facet_wrap(~usgs_site, scales = "free") +
  labs(x = "Discharge",
       y = "Nitrate concentration",
       title = "Prefire C-Q with Postfire Points",
       color = "Postfire Status",
       shape = "Postfire Status") +
  theme_minimal(base_size = 14) +
  theme(
    plot.background = element_rect(fill = "white", color = NA),
    panel.background = element_rect(fill = "white", color = NA),
    strip.background = element_rect(fill = "white"),
    strip.text = element_text(size = 8)
  )

ggsave("NitratepostfireCIplot.png", NitrateCIplotfiltered, width = 10, height = 6, dpi = 300)



#Nitrate####
prefire_nitrate_cleaned <- prefire_nitrate %>% #filter out negative and null
  filter(!is.na(value_std), !is.na(Flow),
         value_std > 0, Flow > 0,
         segment  == "before") %>% #pre
  mutate(fire_period = "Prefire")
postfire_nitrate_cleaned <- prefire_nitrate %>%
  filter(!is.na(value_std), !is.na(Flow),
         value_std > 0, Flow > 0,
         segment == "after") %>% #post
  mutate(fire_period = "Postfire")
#combine cleaned data
combined_data_nitrate <- bind_rows(prefire_nitrate_cleaned, postfire_nitrate_cleaned)
#Fit prefire models per site
pre_models_nitrate <- prefire_nitrate_cleaned %>%
  group_by(usgs_site) %>%
  summarise(model = list(lm(log(value_std) ~ log(Flow), data = cur_data())), .groups = "drop") #log-log linear regression
#Predict prefire CI for all postfire points by site
postfire_nitrate_flagged <- postfire_nitrate_cleaned %>%
  inner_join(pre_models_nitrate, by = "usgs_site") %>%
  group_by(usgs_site) %>%
  mutate(
    pred = predict(model[[1]], newdata = tibble(Flow = Flow), interval = "confidence"),
    lwr = exp(pred[, "lwr"]),     # back transform 
    upr = exp(pred[, "upr"]),
    status = ifelse(value_std < lwr | value_std > upr, "Outside CI", "Inside CI")) %>%
  ungroup() %>%
  select(-model, -pred) #remove temp columns
#Postfire points outside CI
postfire_nitrate_outside_df <- postfire_nitrate_flagged %>%
  filter(status == "Outside CI")
sites_with_both_nitrate <- postfire_nitrate_cleaned %>%
  distinct(usgs_site) %>%      # sites with postfire data
  inner_join(prefire_nitrate_cleaned %>% distinct(usgs_site), by = "usgs_site") %>%
  pull(usgs_site)
# Filter data for plotting
prefire_filtered_nitrate <- prefire_nitrate_cleaned %>%
  filter(usgs_site %in% sites_with_both_nitrate)
postfire_filtered_nitrate <- postfire_nitrate_flagged %>%
  filter(usgs_site %in% sites_with_both_nitrate)
# Re-plot with only sites that have both pre and post fire data
NitrateCIplotfiltered <- ggplot() +
  geom_point(data = prefire_filtered_nitrate,
             aes(x = Flow, y = value_std),
             alpha = 0.6, color = "black") +
  geom_smooth(data = prefire_filtered_nitrate,
              aes(x = Flow, y = value_std),
              method = "lm", formula = y ~ x,
              se = TRUE, color = "blue") +
  geom_point(data = postfire_filtered_nitrate,
             aes(x = Flow, y = value_std, color = status, shape = status),
             size = 3) +
  scale_color_manual(values = c("Inside CI" = "gray70", "Outside CI" = "red")) +
  scale_shape_manual(values = c("Inside CI" = 16, "Outside CI" = 17)) +
  scale_x_log10() +
  scale_y_log10() +
  facet_wrap(~usgs_site, scales = "free") +
  labs(x = "Discharge",
       y = "Nitrate concentration",
       title = "Nitrate Prefire C-Q with Postfire Points(sits with both data points)",
       color = "Postfire Status",
       shape = "Postfire Status") +
  theme_minimal(base_size = 14) +
  theme(
    plot.background = element_rect(fill = "white", color = NA),
    panel.background = element_rect(fill = "white", color = NA),
    strip.background = element_rect(fill = "white"),
    strip.text = element_text(size = 8)
  )

ggsave("NitratepostfireCIplot.png", NitrateCIplotfiltered, width = 10, height = 6, dpi = 300)

#Ammonium####
#get rid of negative concentration values
prefire_ammonium_cleaned <- prefire_ammonium %>%
  filter(!is.na(value_std), !is.na(Flow),
         value_std > 0, Flow > 0,
         segment  == "before") %>% #pre
  mutate(fire_period = "Prefire")
postfire_ammonium_cleaned <- prefire_ammonium %>%
  filter(!is.na(value_std), !is.na(Flow),
         value_std > 0, Flow > 0,
         segment == "after") %>% #post
  mutate(fire_period = "Postfire")
#combine cleaned data
combined_data_ammonium <- bind_rows(prefire_ammonium_cleaned, postfire_ammonium_cleaned)
#Fit prefire models per site
pre_models_ammonium <- prefire_ammonium_cleaned %>%
  group_by(usgs_site) %>%
  summarise(model = list(lm(log(value_std) ~ log(Flow), data = cur_data())), .groups = "drop")
#Predict prefire CI for all postfire points by site
postfire_ammonium_flagged <- postfire_ammonium_cleaned %>%
  inner_join(pre_models_ammonium, by = "usgs_site") %>%
  group_by(usgs_site) %>%
  mutate(
    pred = predict(model[[1]], newdata = tibble(Flow = Flow), interval = "confidence"),
    lwr = exp(pred[, "lwr"]),     # convert back to raw space
    upr = exp(pred[, "upr"]),
    status = ifelse(value_std < lwr | value_std > upr, "Outside CI", "Inside CI")) %>%
  ungroup() %>%
  select(-model, -pred)
#Postfire points outside CI
postfire_ammonium_outside_df <- postfire_ammonium_flagged %>%
  filter(status == "Outside CI")
sites_with_both_ammonium <- postfire_ammonium_cleaned %>%
  distinct(usgs_site) %>%      # sites with postfire data
  inner_join(prefire_ammonium_cleaned %>% distinct(usgs_site), by = "usgs_site") %>%
  pull(usgs_site)
# Filter data for plotting
prefire_filtered_ammonium <- prefire_ammonium_cleaned %>%
  filter(usgs_site %in% sites_with_both_ammonium)
postfire_filtered_ammonium <- postfire_ammonium_flagged %>%
  filter(usgs_site %in% sites_with_both_ammonium)
# Re-plot with only sites that have both pre and post fire data
AmmoniumCIplotfiltered <- ggplot() +
  geom_point(data = prefire_filtered_ammonium,
             aes(x = Flow, y = value_std),
             alpha = 0.6, color = "gray40") +
  geom_smooth(data = prefire_filtered_ammonium,
              aes(x = Flow, y = value_std),
              method = "lm", formula = y ~ x,
              se = TRUE, color = "blue") +
  geom_point(data = postfire_filtered_ammonium,
             aes(x = Flow, y = value_std, color = status, shape = status),
             size = 3) +
  scale_color_manual(values = c("Inside CI" = "gray70", "Outside CI" = "red")) +
  scale_shape_manual(values = c("Inside CI" = 16, "Outside CI" = 17)) +
  scale_x_log10() +
  scale_y_log10() +
  facet_wrap(~usgs_site, scales = "free") +
  labs(x = "Discharge (Flow)",
       y = "Ammonium concentration",
       title = "Ammonium Prefire C-Q with Postfire Points(sits with both data points)",
       color = "Postfire Status",
       shape = "Postfire Status") +
  theme_minimal(base_size = 14) +
  theme(
    plot.background = element_rect(fill = "white", color = NA),
    panel.background = element_rect(fill = "white", color = NA),
    strip.background = element_rect(fill = "white"),
    strip.text = element_text(size = 8)
  )

ggsave("AmmoniumpostfireCIplot.png", AmmoniumCIplotfiltered, width = 10, height = 6, dpi = 300)



#Ortho####
#get rid of negative concentration values
prefire_orthop_cleaned <- prefire_orthop %>%
  filter(!is.na(value_std), !is.na(Flow),
         value_std > 0, Flow > 0,
         segment  == "before") %>% #pre
  mutate(fire_period = "Prefire")
postfire_orthop_cleaned <- prefire_orthop %>%
  filter(!is.na(value_std), !is.na(Flow),
         value_std > 0, Flow > 0,
         segment == "after") %>% #post
  mutate(fire_period = "Postfire")
#combine cleaned data
combined_orthop <- bind_rows(prefire_orthop_cleaned, postfire_orthop_cleaned)
#Fit prefire models per site
pre_models_orthop <- prefire_orthop_cleaned %>%
  group_by(usgs_site) %>%
  summarise(model = list(lm(log(value_std) ~ log(Flow), data = cur_data())), .groups = "drop")
#Predict prefire CI for all postfire points by site
postfire_orthop_flagged <- postfire_orthop_cleaned %>%
  inner_join(pre_models_ammonium, by = "usgs_site") %>%
  group_by(usgs_site) %>%
  mutate(
    pred = predict(model[[1]], newdata = tibble(Flow = Flow), interval = "confidence"),
    lwr = exp(pred[, "lwr"]),     # convert back to raw space
    upr = exp(pred[, "upr"]),
    status = ifelse(value_std < lwr | value_std > upr, "Outside CI", "Inside CI")) %>%
  ungroup() %>%
  select(-model, -pred)
#Postfire points outside CI
postfire_orthop_outside_df <- postfire_orthop_flagged %>%
  filter(status == "Outside CI")
sites_with_both_orthop <- postfire_orthop_cleaned %>%
  distinct(usgs_site) %>%      # sites with postfire data
  inner_join(prefire_orthop_cleaned %>% distinct(usgs_site), by = "usgs_site") %>%
  pull(usgs_site)
# Filter data for plotting
prefire_filtered_orthop <- prefire_orthop_cleaned %>%
  filter(usgs_site %in% sites_with_both_orthop)
postfire_filtered_orthop <- postfire_orthop_flagged %>%
  filter(usgs_site %in% sites_with_both_orthop)
# Re-plot with only sites that have both pre and post fire data
OrthopCIplotfiltered <- ggplot() +
  geom_point(data = prefire_filtered_orthop,
             aes(x = Flow, y = value_std),
             alpha = 0.6, color = "black") +
  geom_smooth(data = prefire_filtered_orthop,
              aes(x = Flow, y = value_std),
              method = "lm", formula = y ~ x,
              se = TRUE, color = "blue") +
  geom_point(data = postfire_filtered_orthop,
             aes(x = Flow, y = value_std, color = status, shape = status),
             size = 3) +
  scale_color_manual(values = c("Inside CI" = "gray70", "Outside CI" = "red")) +
  scale_shape_manual(values = c("Inside CI" = 16, "Outside CI" = 17)) +
  scale_x_log10() +
  scale_y_log10() +
  facet_wrap(~usgs_site, scales = "free") +
  labs(x = "Discharge",
       y = "Orthophosphate concentration",
       title = "Orthophosphate Prefire C-Q with Postfire Points(sites with both data points)",
       color = "Postfire Status",
       shape = "Postfire Status") +
  theme_minimal(base_size = 14) +
  theme(
    plot.background = element_rect(fill = "white", color = NA),
    panel.background = element_rect(fill = "white", color = NA),
    strip.background = element_rect(fill = "white"),
    strip.text = element_text(size = 8)
  )

ggsave("OrthoppostfireCIplot.png", OrthopCIplotfiltered, width = 10, height = 6, dpi = 300)


#prediction grid for each site####
pre_predictions <- pre_models %>%
  group_by(usgs_site) %>%
  do({
    data_site <- prefire_data_cleaned %>% filter(usgs_site == unique(.$usgs_site))
    model <- .$model[[1]]
    preds <- predict(model, newdata = data_site, interval = "confidence")
    tibble(Flow = data_site$Flow,
           lwr = exp(preds[, "lwr"]),
           upr = exp(preds[, "upr"]))
  })

#adding predictions back to prefire data 
prefire_CI <- prefire_data_cleaned %>%
  left_join(pre_predictions, by = "Flow")

#which postfire points are outside the CI?
flagged_postfire <- postfire_data_cleaned %>%
  left_join(pre_predictions, by = c("usgs_site", "Flow")) %>%
  mutate(outside_CI = ifelse(value_std < lwr | value_std > upr, "Outside CI", "Inside CI"))

# Create a separate df listing only points outside CI
postfire_outside_df <- flagged_postfire %>% filter(status == "Outside CI")


ggplot() +
  # Prefire points
  geom_point(data = prefire_data_cleaned,
             aes(x = Flow, y = value_std),
             alpha = 0.6, color = "gray40") +
  # Prefire regression + CI band
  geom_smooth(data = prefire_data_cleaned,
              aes(x = Flow, y = value_std),
              method = "lm", formula = y ~ x,
              se = TRUE, color = "blue") +
  # Postfire points (default red)
  geom_point(data = flagged_postfire,
             aes(x = Flow, y = value_std, color = outside_CI, shape = outside_CI),
             size = 3) +
  scale_color_manual(values = c("FALSE" = "red", "TRUE" = "black")) +
  scale_shape_manual(values = c("FALSE" = 16, "TRUE" = 17)) +
  scale_x_log10() +
  scale_y_log10() +
  facet_wrap(~usgs_site, scales = "free") +
  labs(x = "Discharge (Flow)",
       y = "Nitrate concentration",
       title = "Prefire C-Q with Postfire Points Highlighted",
       color = "Outside CI",
       shape = "Outside CI") +
  theme_minimal(base_size = 14)



ggplot(combined_data, aes(x = Flow, y = value_std, color = fire_period)) +
  geom_point(alpha = 0.6) +
  geom_smooth(method = "lm", formula = y ~ x, se = FALSE) +
  scale_x_log10() +
  scale_y_log10() +
  facet_wrap(~usgs_site, scales = "free") +
  labs(x = "Discharge (Flow)",
       y = "Nitrate concentration",
       title = "C-Q Relationships by USGS Site",
       color = "Fire Period") +
  theme_minimal(base_size = 14)

common_sites <- combined_data %>%
  group_by(usgs_site, fire_period) %>%
  summarise(n = n(), .groups = "drop") %>%
  pivot_wider(names_from = fire_period, values_from = n, values_fill = 0) %>%
  filter(Prefire > 0, Postfire > 0) %>%
  pull(usgs_site)

common_sites <- combined_data %>%
  filter(usgs_site %in% common_sites)



ggplot(common_sites, aes(x = Flow, y = value_std, color = fire_period)) +
  geom_point(alpha = 0.6) +
  geom_smooth(method = "lm", formula = y ~ x, se = FALSE) +
  scale_x_log10() +
  scale_y_log10() +
  facet_wrap(~usgs_site, scales = "free") +
  labs(x = "Discharge (Flow)",
       y = "Nitrate concentration",
       title = "C-Q Relationships (Sites with Both Prefire & Postfire Data)",
       color = "Fire Period") +
  theme_minimal(base_size = 14)



#facet wrapped pre-fire data 
ggplot(prefire_data_cleaned, aes(x = Flow, y = value_std)) +
  geom_point(alpha = 0.6, color = "gray40") +
  geom_smooth(method = "lm", formula = y ~ x, 
              se = TRUE, color = "blue") +
  scale_x_log10() +
  scale_y_log10() +
  facet_wrap(~usgs_site, scales = "free") +
  labs(x = "Discharge (Flow)",
       y = "Nitrate concentration",
       title = "Prefire C-Q Relationships by USGS Site") +
  theme_minimal(base_size = 14)


#facet wrapped post-fire data 
ggplot(postfire_data, aes(x = Flow, y = value_std)) +
  geom_point(alpha = 0.6, color = "red") +
  geom_smooth(method = "lm", formula = y ~ x, 
              se = TRUE, color = "blue") +
  scale_x_log10() +
  scale_y_log10() +
  facet_wrap(~usgs_site, scales = "free") +
  labs(x = "Discharge (Flow)",
       y = "Nitrate concentration",
       title = "Prefire C-Q Relationships by USGS Site") +
  theme_minimal(base_size = 14)








ggplot(prefire_data_cleaned, aes(x = Flow, y = value_std)) +
  geom_point(alpha = 0.6) +
  geom_smooth(method = "lm", formula = y ~ x, se = TRUE, color = "blue") +
  scale_x_log10() +
  scale_y_log10() +
  labs(x = "Discharge",
       y = "Nitrate concentration",
       title = "C-Q Relationship (Prefire)") +
  theme_minimal(base_size = 14)


#adding postfire
ggplot(prefire_data_cleaned, aes(x = Flow, y = value_std)) +
  geom_point(alpha = 0.6, color = "gray40") +   # prefire points
  geom_smooth(method = "lm", formula = y ~ x, se = TRUE, color = "blue") +
  # add nitratefires points
  geom_point(data = postfire_data %>%
               filter(!is.na(value_std), !is.na(Flow),
                      value_std > 0, Flow > 0),
             aes(x = Flow, y = value_std),
             color = "red", size = 2, alpha = 0.8) +
  scale_x_log10() +
  scale_y_log10() +
  labs(x = "Discharge",
       y = "Nitrate concentration",
       title = "C-Q Relationship (Prefire + Postfire)") +
  theme_minimal(base_size = 14)

#combined data frames
common_points <- inner_join(
  prefire_data_cleaned %>%
    select(usgs_site, date, value_std, Flow),
  postfire_data %>%
    select(usgs_site, date, value_std, Flow),
  by = c("usgs_site", "date", "value_std", "Flow")
)

unique_points <- anti_join(
  filteredfires_data, 
  common_points,
  by = c("usgs_site", "date", "value_std", "Flow")
)


