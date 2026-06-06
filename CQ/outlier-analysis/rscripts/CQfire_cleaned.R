#post/pre outlier analysis crass#
install.packages("httpgd")
library(httpgd)
#library(ggplot2)
#library(dplyr)
library(tidyverse)
library(tidyr) 
library(broom) 
library(vroom)
library(googlesheets4)
library(ggpubr)
library(here)

ed <- gs4_endpoints()

gs4_scopes()


# prefire_nitrate_url <- "https://docs.google.com/spreadsheets/d/1Y_pVScjUmpmBtpnK8SGSWPijZhaTzLFOkdnITJFuRps/edit?gid=1194451891#gid=1194451891"
# prefire_nitrate <- read_sheet(prefire_nitrate_url)

#prefire_nitrate <- read.csv("/Users/ash/Documents/projects/CRASS/lter-sparc-fire-arid-streams/CQ/outlier-analysis/data/nitrate_discharge_before_quartiles_234_max_fire - nitrate_discharge_before_quartiles_234_max_fire.csv")

prefire_nitrate <- read.csv(here("CQ", "outlier-analysis", "data", "nitrate_discharge_before_quartiles_234_max_fire - nitrate_discharge_before_quartiles_234_max_fire.csv"))

#head(prefire_nitrate)
#unique(prefire_nitrate$usgs_site)

# filteredfires_nitrate_url <-"https://docs.google.com/spreadsheets/d/19o_wQaLYJm_n1vQg_ECbJ8I8ZFnoAPr1_JSujDPvZjU/edit?gid=42808220#gid=42808220"
# filteredfires_nitrate <- read_sheet(filteredfires_nitrate_url)

#filteredfires_nitrate <- read.csv("/Users/ash/Documents/projects/CRASS/lter-sparc-fire-arid-streams/CQ/outlier-analysis/data/nitrate_discharge_quartiles_234_max_fire - nitrate_discharge_quartiles_234_max_fire.csv")

filteredfires_nitrate <- read.csv(here("CQ", "outlier-analysis", "data", "nitrate_discharge_quartiles_234_max_fire - nitrate_discharge_quartiles_234_max_fire.csv"))


#head(filteredfires_nitrate)
#unique(filteredfires_nitrate$usgs_site)

# nitratefires_url <- "https://docs.google.com/spreadsheets/d/1rfVYsvFIdzP4vT7xhIOKxInOdqFezYLSKET1SG3Iz5k/edit?gid=676161325#gid=676161325"
# nitratefires <- read_sheet(nitratefires_url) 
# nitratefires <- read.csv("/Users/ash/Documents/projects/CRASS/cq.1.26.26/nitrate_sites_fires - nitrate_sites_fires.csv")
# head(nitratefires)

# prefire_ammonium_url <- "https://docs.google.com/spreadsheets/d/1l_cADBLUbLu_XfKlfNTRNYSEkXSwGUi8FXi8p6yDzDM/edit?gid=1949068225#gid=1949068225"
# prefire_ammonium <- read_sheet(prefire_ammonium_url)
# filteredfires_ammonium_url <- "https://docs.google.com/spreadsheets/d/1Jr6TfguHXzJfbYDhrNPYr1OPoxelgWV5t8Tyh0HVvlQ/edit?gid=1860307665#gid=1860307665"
# filteredfires_ammonium <- read_sheet(filteredfires_ammonium_url)
# ammoniumfires_url <- "https://docs.google.com/spreadsheets/d/1hKuHVEREtt1mMQqPmysEN7iyx79bf1OtWD1Q61TmKnU/edit?gid=1427499502#gid=1427499502"
# ammoniumfires <- read_sheet(ammoniumfires_url)
# prefire_orthop_url <- "https://docs.google.com/spreadsheets/d/1Tc3M4RegL3m5S71RUrwrVBWuDBFPNdgrVu7vYtDn3AA/edit?gid=1285577098#gid=1285577098"
# prefire_orthop <- read_sheet(prefire_orthop_url)
# orthopfires_url <- "https://docs.google.com/spreadsheets/d/1AGESDPDOeBHQauvVSIOOK49IjMt_FOuaxA4UnsPMmQA/edit?gid=38739842#gid=38739842"
# orthopfires <- read_sheet(orthopfires_url)


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

sites_with_both_nitrate <- postfire_nitrate_cleaned %>%
  distinct(usgs_site) %>%      # sites with postfire data
  inner_join(prefire_nitrate_cleaned %>% distinct(usgs_site), by = "usgs_site") %>%
  pull(usgs_site)

unique(sites_with_both_nitrate)

combined_data_nitrate_filt <- combined_data_nitrate %>% filter(usgs_site %in% sites_with_both_nitrate)

hist(combined_data_nitrate$value_std)
#Fit prefire models per site
pre_models_nitrate <- prefire_nitrate_cleaned %>% filter(usgs_site %in% sites_with_both_nitrate) %>%
  group_by(usgs_site) %>% 
  summarise(model = list(lm(log(value_std) ~ log(Flow)))) %>% 
  ungroup()
  #slope= coef(model)[2])
  # slopedir = ifelse(slope >= .2, "positive", ifelse(slope <= -.2 , "negative", "neutral"))) %>% 
  # ungroup() #log-log linear regression
colnames(combined_data_nitrate)
unique(combined_data_nitrate$fire_period)

#Predict prefire CI for all points by site
sts <- unique(combined_data_nitrate_filt$usgs_site)
comb_flagged <- c()
for (j in sts){
    one <- combined_data_nitrate_filt[combined_data_nitrate_filt$usgs_site == j,]
    ##build the model on prefire 
    onepre <- one[one$fire_period == "Prefire",]
    modl <- lm(log(value_std) ~ log(Flow), data=onepre)
    pred <- predict(modl, newdata=one, interval="prediction")#interval = "confidence")
    one$lwr   = exp(pred[, "lwr"])
    one$upr    = exp(pred[, "upr"])
    one$fit = pred[, "fit"]
    #keep in log
    one$rs <- log(one$value_std) - one$fit
    one$status <- ifelse(one$value_std < one$lwr | one$value_std > one$upr, "Outside PI", "Inside PI")
    one$slopedir <- ifelse(modl$coefficients[2] >= .2, "positive", ifelse(modl$coefficients[2] <= -.2 , "negative", "neutral"))
    comb_flagged <- rbind(comb_flagged, one)
}


str(combined_data_nitrate_filt)
str(as.data.frame(comb_flagged))

#write.csv(as.data.frame(comb_flagged), "../data/nitrate_working_prediction.csv")
write.csv(as.data.frame(comb_flagged), here("CQ", "outlier-analysis", "data", "nitrate_working_prediction.csv"), row.names = FALSE)

# comb_flagged <- combined_data_nitrate_filt  %>%
#      inner_join(pre_models_nitrate, by="usgs_site") %>% 
#      group_by(usgs_site) %>%
#      mutate(pred=predict(model[[1]], newdata=tibble(Flow=Flow),
#      interval = "confidence"),
#      lwr= exp(pred[,"lwr"]),
#      upr = exp(pred[,"upr"]),
#      status = ifelse(value_std < lwr | value_std > upr, "Outside CI", "Inside CI"),
#      obs=n(),
#      fire_period = fire_period) %>%
#      ungroup()

# str(comb_flagged)

NitrateCIplotfiltered <- ggplot() +
  geom_point(data = comb_flagged,
             aes(x = Flow, y = value_std, color=status, shape=fire_period)) +
             #alpha = 0.6, shape= 18) +
  geom_ribbon(data=comb_flagged, aes(x= Flow, ymin=lwr, ymax=upr), alpha=.3) +
  geom_smooth(data = comb_flagged[comb_flagged$fire_period == "Prefire",],
              aes(x = Flow, y = value_std),
              method = "lm", formula = y ~ x,
              se = FALSE, color = "blue") +
  #geom_point(data = postfire_nitrate_flagged,
  #           aes(x = Flow, y = value_std, color = status), shape = 13, size = 3) +
  scale_color_manual(values = c("Inside PI" = "gray70", "Outside PI" = "red")) +
  scale_shape_manual(values = c("Prefire" = 16, "Postfire" = 17)) +
  scale_x_log10() +
  scale_y_log10() +
  facet_wrap(~usgs_site, scales = "free") +
  labs(x = "Discharge",
       y = "Nitrate concentration",
       title = "Nitrate Prefire C-Q with Postfire Points",
       color = "Postfire Status",
       shape = "Postfire Status") +
  theme_minimal(base_size = 14) +
  theme(
    plot.background = element_rect(fill = "white", color = NA),
    panel.background = element_rect(fill = "white", color = NA),
    strip.background = element_rect(fill = "white"),
    strip.text = element_text(size = 8)
  )
NitrateCIplotfiltered

#ggsave("../figures/NitratepostfireCIplot-prediction.png", NitrateCIplotfiltered, width = 20, height = 24, dpi = 300)

ggsave(NitrateCIplotfiltered, path = here("CQ", "outlier-analysis", "figures"), file = "NitratepostfireCIplot-prediction.png", width = 20, height = 24, units = "in", dpi = 300)

# single site demo: sbc_lter_mis
sbc_mis <- comb_flagged %>% filter(usgs_site == "sbc_lter_mis")
sbc_mis_nitratePI <- sbc_mis %>% ggplot() +
                                    geom_ribbon(aes(x= Flow, ymin = lwr, ymax = upr), alpha = 0.2) +
                                    geom_smooth(data = sbc_mis[sbc_mis$fire_period == "Prefire",],
                                                    aes(x = Flow, y = value_std),
                                                    method = "lm", formula = y ~ x, se = FALSE, color = "blue") +
                                    geom_point(aes(x = Flow, y = value_std, color = status, shape = fire_period), size = 5) +
  #alpha = 0.6, shape= 18) +
  #geom_point(data = postfire_nitrate_flagged,
  #           aes(x = Flow, y = value_std, color = status), shape = 13, size = 3) +
                                                    scale_color_manual(labels = c("predicted", "outliers"), values = c("Inside PI" = "gray70", "Outside PI" = "darkred")) +
                                                    scale_shape_manual(labels = c("post-fire", "pre-fire"), values = c("Prefire" = 16, "Postfire" = 17)) +
                                        scale_x_log10() +
                                        scale_y_log10() +
                                      labs(x = "Discharge (L/s)",
                                           y = "Nitrate (mg N/L)") +
                                      theme_minimal(base_size = 14) +
                                      theme(panel.grid.major = element_blank(),
                                            panel.grid.minor = element_blank(),
                                            panel.background = element_blank(),
                                            panel.border = element_rect(colour = "black", fill = NA, linewidth = 2),
                                            legend.title = element_blank(),
                                            legend.box.background = element_rect(colour = "black"),
                                            legend.text = element_text(size = 20),
                                            legend.position = c(0.85, 0.175),
                                            axis.text = element_text(size = 20),
                                            axis.title = element_text(size = 20)
)

ggsave(sbc_mis_nitratePI, path = here("CQ", "outlier-analysis", "figures"), file = "SBC_MIS_nitrate_PI.pdf", width = 9, height = 8.5, units = "in")

# ####get slopes of each site from prefire models 
# pre_models_nitrate_slp <- pre_models_nitrate %>% group_by(usgs_site) %>%
#     summarise(slope = as.numeric(model[[1]][1]$coefficients[2]),
#     slopedir = ifelse(slope >= .2, "positive", ifelse(slope <= -.2 , "negative", "neutral")))

# str(pre_models_nitrate_slp)

# ##combined data and merge with the slope 
# box_nit_dat <- merge(pre_models_nitrate_slp, comb_flagged, by="usgs_site")

# colnames(box_nit_dat)
# str(box_nit_dat)
# nit_sites <- unique(box_nit_dat$usgs_site)

comb_flagged2 <- comb_flagged 
comb_flagged2$inx <- 1
box_nit_lk <- comb_flagged2 %>% group_by(usgs_site, fire_period) %>%
          summarise(inci = sum(inx[status == "Inside PI"]),
          outci= sum(inx[status == "Outside PI"]),
          #prein= sum(i == "Prefire"]),
          #postin= sum(inx[status == "Inside PI" & fire_period == "Postfire"]),
          total = sum(inx), 
          wgt = outci/total,
          slopedir = slopedir[1]) %>% ungroup()
          #postweight= postout/sum(inx[fire_period=="Postfire"]),
          #totalobs = sum(inx), slopedir = slopedir[1])
str(box_nit_lk)

box_nit_wide <- box_nit_lk %>% pivot_wider(names_from =fire_period, values_from = c(wgt, inci,outci,total))

str(box_nit_wide)

#write.csv(box_nit_lk,"../data/nitrate-summary.csv")
##this is the percentage because there were more total obs in the pre and it was messing it up

write.csv(box_nit_wide, here("CQ", "outlier-analysis", "data", "nitrate-summary.csv"), row.names = FALSE)

##boxplots of outliers by slopedir
bx_nit <- ggplot(data=box_nit_lk)+
  geom_boxplot(aes(y=wgt, x=fire_period, fill=slopedir)) +
  #geom_boxplot(aes(y=postweight,x =.5, fill=slopedir)) +
  facet_wrap(~slopedir)
bx_nit


# debug needed here
box_nit_dat_sum <- box_nit_dat %>% group_by(usgs_site) %>% 
                summarize(pre_out=n(value_std[status == "Prefire"]) ,post_out=n(value_std[status == "Postfire"]), slopedir = slopedir[1]) %>% ungroup()

sig_dif <- box_nit_dat_sum %>% group_by(slopedir) %>% 
            summarise(ttst = t.test(obs[fire_period == "Prefire"], obs[fire_period == "Postfire"], paired = TRUE))

hist(box_nit_dat_sum$obs)
str(box_nit_dat_sum)


str(comb_flagged)



##magnitude 

##predict the prefire fire to get lower and upper bounds
prefire_nitrate_flagged <- prefire_nitrate_cleaned %>%
     inner_join(pre_models_nitrate, by="usgs_site") %>% 
     group_by(usgs_site) %>%
     mutate(pred=predict(model[[1]], newdata=tibble(Flow=Flow),
     interval = "confidence"),
     lwr= exp(pred[,"lwr"]),
     upr = exp(pred[,"upr"]),
     status = ifelse(value_std < lwr | value_std > upr, "Outside CI", "Inside CI"),
     obs=n()) %>%
     ungroup()


#Predict prefire CI for all postfire points by site
postfire_nitrate_flagged <- postfire_nitrate_cleaned %>%
  inner_join(pre_models_nitrate, by = "usgs_site") %>%
  group_by(usgs_site) %>%
  mutate( 
    pred = predict(model[[1]], newdata = tibble(Flow = Flow), interval = "confidence"),
    lwr = exp(pred[, "lwr"]),     # back transform-exp to turn back into raw value, not log transformed
    upr = exp(pred[, "upr"]),
    #slope = coef(model)[2],
    rs = (((value_std) - exp(pred[, "fit"]))^2), ##residual squares exp to transform back
    status = ifelse(value_std < lwr | value_std > upr, "Outside CI", "Inside CI"),
   #slopedir = ifelse(slope >= .2, "positive", ifelse(slope <= -.2 , "negative", "neutral")),
    obs=n()
  ) %>%
  ungroup() #%>%
  #select(-model,-pred) #remove temp columns



#Postfire points outside CI
postfire_nitrate_outside_df <- postfire_nitrate_flagged %>%
  filter(status == "Outside CI")
sites_with_both_nitrate <- postfire_nitrate_cleaned %>%
  distinct(usgs_site) %>%      # sites with postfire data
  inner_join(prefire_nitrate_cleaned %>% distinct(usgs_site), by = "usgs_site") %>%
  pull(usgs_site)

# Filter data for plotting
prefire_filtered_nitrate <- prefire_nitrate_flagged %>%
  filter(usgs_site %in% sites_with_both_nitrate)
postfire_filtered_nitrate <- postfire_nitrate_flagged %>%
  filter(usgs_site %in% sites_with_both_nitrate)
head(postfire_filtered_nitrate)
head(prefire_filtered_nitrate)
unique(postfire_filtered_nitrate$status)
str(postfire_filtered_nitrate)


# Re-plot with only sites that have both pre and post fire data
NitrateCIplotfiltered <- ggplot() +
  geom_point(data = prefire_filtered_nitrate,
             aes(x = Flow, y = value_std, color=status),
             alpha = 0.6, shape= 18) +
  geom_smooth(data = postfire_filtered_nitrate,
              aes(x = Flow, y = value_std),
              method = "lm", formula = y ~ x,
              se = TRUE, color = "blue") +
  geom_point(data = postfire_nitrate_flagged,
             aes(x = Flow, y = value_std, color = status), shape = 13, size = 3) +
  scale_color_manual(values = c("Inside CI" = "gray70", "Outside CI" = "red")) +
  #scale_shape_manual(values = c("Inside CI" = 16, "Outside CI" = 17)) +
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
NitrateCIplotfiltered

ggsave("NitratepostfireCIplot.png", NitrateCIplotfiltered, width = 20, height = 24, dpi = 300)

###generate nitrate/dischare values then plot them over time with outliers marked

#add prefire proportion data to postfire for time plot

Nitrate_timeplotfiltered <- ggplot() +
  geom_point(data = postfire_filtered_nitrate,
             aes(x = date, y = rs, color = status, shape = status)) +
  geom_smooth(data = postfire_filtered_nitrate,method = "loess", formula = y ~ x,
            aes(x = date, y = rs, group = usgs_site)) +
  scale_color_manual(values = c("Inside CI" = "gray70", "Outside CI" = "red")) +
  scale_shape_manual(values = c("Inside CI" = 16, "Outside CI" = 17)) +
  facet_wrap(c(~slopedir,~usgs_site), scales = "free") +
  labs(x = "Date",
       y = "Nitrate residual squares",
       title = "Nitrate residual squares Over Time (sites with both pre and post fire data)",
       color = "Postfire Status",
       shape = "Postfire Status") +
  theme_minimal(base_size = 14) +
  theme(
    plot.background = element_rect(fill = "white", color = NA),
    panel.background = element_rect(fill = "white", color = NA),
    strip.background = element_rect(fill = "white"),
    strip.text = element_text(size = 8)
)
Nitrate_timeplotfiltered

ggsave("Nitrate_timeplotfiltered2.png", Nitrate_timeplotfiltered, width = 20, height = 24, dpi = 300)

##histogram of obs per site facet wrapped by slopedir
hpt <- ggplot(postfire_filtered_nitrate, aes(x=obs)) +
  geom_histogram(binwidth=5, fill="lightblue", color="black") +
  facet_wrap(~slopedir) +
  labs(title="Histogram of Observations per Site by Slope Direction",
       x="Number of Observations",
       y="Count of Sites") +
  theme_minimal()
hpt

nitratesited <- unique(postfire_filtered_nitrate$usgs_site)
library(dataRetrieval)
##get lat long using nwisites data
sites <- nitratesited
met <- nwis_sites(sites)


histdat <- postfire_filtered_nitrate %>%
  group_by(usgs_site) %>%
  summarise(mean_rs = mean(rs), .groups = "drop",obs=n())

scipen(-999)
hist(histdat$mean_rs)
hist(histdat$obs,binwidth=5)


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

