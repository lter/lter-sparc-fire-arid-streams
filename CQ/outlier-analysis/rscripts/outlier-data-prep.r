library(tidyverse)
library(lubridate)
library(VedicDateTime)
setwd("/Users/ash/Documents/projects/CRASS")


pre <- read.csv("/Users/ash/Documents/projects/CRASS/cq.1.26.26/nitrate_discharge_before_quartiles_234_max_fire - nitrate_discharge_before_quartiles_234_max_fire.csv")
post <- read.csv("/Users/ash/Documents/projects/CRASS/cq.1.26.26/nitrate_discharge_quartiles_234_max_fire - nitrate_discharge_quartiles_234_max_fire.csv")

nitrate <- rbind(pre, post)

nitrate <- nitrate %>% arrange(usgs_site,year, date) ###year is the fire year 
nrow(nitrate) #6151 observations
 ##this is the only one where before and after overlap between fires so removing it for now

##check to eliminate overlapping pre and post fire data in same year
#site_yrs_combos <- distinct(nitrate, usgs_site, year)
#multifirebasins <- site_yrs_combos[duplicated(site_yrs_combos$usgs_site),]

#multi <- nitrate %>% filter(usgs_site %in% multifirebasins$usgs_site)
#multi <- multi %>% arrange(usgs_site, year)
#head(multi, 50)
#osit <- unique(nitrate$usgs_site)[1]
#nitrate <- nitrate[!(nitrate$usgs_site == osit), ] 
#length(unique(nitrate$usgs_site)) #74 sites
#nrow(nitrate) # 6067 observations

#osit2 <- "USGS-07311783"
#look <- nitrate %>% filter(usgs_site == osit2)

##there are zeros which returns undefined log values, so removing those
nitrate <- nitrate %>% filter(value_std > 0, Flow > 0)
nrow(nitrate) #5488 observations
head(nitrate)
nrow(nitrate[nitrate$usgs_site=="syca",])
allfires <- read.csv("nitrate_dd_area.csv", header = T)
head(allfires)

###for each observation sum the total area burned in the previous 10 years and calculate the number of days since the last fire and the number of fires in the previous 10 years.
head(nitrate)
i <- 2
for (i in 1:nrow(nitrate)) {
  site <- nitrate$usgs_site[i]
  date <- nitrate$date[i]
  date <- as.Date(date)
  start_date <- date - years(10)
  
  total_burned <- allfires %>%
    filter(usgs_site == site, ignition_date >= start_date, ignition_date <= date) %>%
    summarise(total_burned = sum(per_cent_burned),
              num_fires = n(),
              last_fire_date = max(ignition_date)) %>%
     mutate(days_since_last_fire = ifelse(is.na(last_fire_date), 0, as.integer((difftime(date, last_fire_date, units = "days")))))
  
  nitrate$total_burned[i] <- total_burned$total_burned
  nitrate$num_fires[i] <- total_burned$num_fires
  nitrate$days_since_last_fire[i] <- total_burned$days_since_last_fire


}

head(nitrate[is.na(nitrate$days_since_last_fire),])
###adding step to standardize to t distribution the value_std and flow so sites are comparable and can be put in the same model


nitrate <- nitrate %>%
  group_by(usgs_site) %>%
  mutate(scale(value_std),
         scale(Flow)) %>%
  ungroup() 
  
hist(nitrate$value_std)
hist(nitrate$Flow)

# look <- allfires %>% filter(usgs_site == "USGS-06635000") %>% arrange(ignition_date)
########build the c-q model #######
models_nitrate <- nitrate %>%
  group_by(usgs_site) %>%
  mutate(rs = resid((lm(log(value_std) ~ log(Flow)))),
         int = coef(lm(log(value_std) ~ log(Flow)))[1],
         slope = coef(lm(log(value_std) ~ log(Flow)))[2],
         slopedir = ifelse(slope >= .2, "positive", ifelse(slope <= -.2 , "negative", "neutral"))) %>% ungroup()


models_nitrate <- as.data.frame(models_nitrate)
str(models_nitrate)
length(unique(models_nitrate$usgs_site))
unique(models_nitrate$slopedir)
########get predictions and residual squares ######
##put this in above 

# nitrate_resids <- nitrate %>%
#   left_join(models_nitrate, by = "usgs_site") %>%
#   group_by(usgs_site) %>%
#   mutate(
#     pred = predict(model[[1]], newdata = data.frame(Flow = Flow), interval = "confidence"),
#     fitted = exp(pred[, "fit"]),
#     lwr = exp(pred[, "lwr"]), 
#     upr = exp(pred[, "upr"]),
#     rs=resid(model), ##residuals from the model
#     rs = (((value_std) - exp(pred[, "fit"]))), ##residual squares exp to transform back
#     status = ifelse(value_std < lwr | value_std > upr, "Outside CI", "Inside CI"),
#     slopedir = ifelse(model[[1]]$coefficients[2] >= 1, "positive", ifelse(model[[1]]$coefficients[2] <= -1 , "negative", "neutral"))
#   ) %>%
#   ungroup() #%>%

# head(nitrate_resids)


####just extract the slope and intercept for each site 
# nitrate_resids_df <- nitrate_resids %>% group_by(usgs_site) %>%
#               mutate(slope = model[[1]]$coefficients[2], intercept = model[[1]]$coefficients[1]) %>% 
#               select(-model)
# head(nitrate_resids_df)

# unique(nitrate_resids_df$slope)

# nitrate_resids_df <- as.data.frame(nitrate_resids_df)
# head(nitrate_resids_df)

# nitrate_resids_df <- nitrate_resids_df %>% select(-pred)
# head(nitrate_resids_df)

# nitrate_resids_df$days_since_last_fire <- trunc(nitrate_resids_df$days_since_last_fire)


head(nitrate_resids$model)
head(nitrate_resids)
head(nitrate_resids$fitted)
# ###grab the good stuff 
# nitratedata <- nitrate_resids %>%
#   select(usgs_site, date, value_std, Flow, fireyear=year, rs, status, fitted=fitted)
# head(nitratedata)
# nrow(nitratedata) #5404
# addsegment <- nitrate %>%
#   select(usgs_site, date, segment)
# head(addsegment)

# nitratedata <- merge(nitratedata, addsegment, by = c("usgs_site", "date"))
# head(nitratedata)
# nrow(nitratedata) #5404
# ###need to get precipitation for date of observation##
# prcp <- read.csv("fire-arid_precipitation.csv",header = T)
# str(prcp)
# nitratedata$year <- year(nitratedata$date)
# nitratedata$day <- format(as.Date(nitratedata$date), format = "%j")
# nitratedata$day <- as.integer(nitratedata$day)
# str(nitratedata)

# ##join by usgs_site, year, day and sum prcp for that day and the previous 2 days
# prcp_3day <- prcp %>% mutate(prcp_3day = precip_mm + lag(precip_mm,1) + lag(precip_mm,2))
# head(prcp_3day)

# nitratedata_prcp <- nitratedata %>%
#   left_join(prcp_3day, by = c("usgs_site", "year", "day"))
# nrow(nitratedata_prcp) #5404 should be same as nitratedata
##add ecoregion ###
##using ecoregion level 2 here##
ecoregions <- read.csv("study_sites_ecoregions.csv",header = T)
head(ecoregions)

sbeco <- ecoregions[ecoregions$na_l2code == "11.1",]
tomerge <- sbeco %>%
  select(na_l2code, na_l2name)

ecoregions <- ecoregions %>%
  select(usgs_site, ecoregion_code = na_l2code, ecoregion_name = na_l2name)
nrow(ecoregions)

wantedsites <- unique(models_nitrate$usgs_site)
length(wantedsites) #74

ecoregions <- ecoregions %>%
  filter(usgs_site %in% wantedsites)
nrow(ecoregions) #123

ecoregions <- ecoregions %>%
  distinct(usgs_site, .keep_all = TRUE) %>%
  select(usgs_site, ecoregion_code, ecoregion_name)

nitratedata_ecoregion <- merge(models_nitrate, ecoregions, by = "usgs_site", all.x = TRUE)

nrow(nitratedata_ecoregion) #5504 should be same as nitratedata_prcp
##some didn't have matching ecoregions, add those manually 
problems <- nitratedata_ecoregion %>%
 filter(is.na(ecoregion_code) == TRUE)

##just the sbc_lter sites and should be 11.1 
problems_usgs <- unique(problems$usgs_site)
head(problems)
problems$ecoregion_code <- "11.1"
problems$ecoregion_name <- "MEDITERRANEAN CALIFORNIA"

#now merge back
nitratedata_ecoregion_fixed <- rbind(nitratedata_ecoregion %>% filter(is.na(ecoregion_code) == FALSE), problems)
nrow(nitratedata_ecoregion_fixed) #5488 yay!

dup_rm <- nitratedata_ecoregion_fixed %>%
  distinct(usgs_site, date, .keep_all = TRUE)

nrow(dup_rm) #3183 
length(unique(dup_rm$usgs_site)) #74 sites yay!


head(dup_rm)
hist(dup_rm$rs)

write.csv(dup_rm, "nitrate_cq_residuals_with_ecoregion_.2slope.csv", row.names = FALSE)

