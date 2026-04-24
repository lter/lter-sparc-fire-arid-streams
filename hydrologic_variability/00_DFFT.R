### Discrete fast-fourier analysis of daily discharge ###
## DFFT approach: Sabo & Post 2008 Ecology

## Tamara Harms, 4/26
## Most recent 25-y of daily discharge (or longest record possible, if less than 25 y), regardless of ignition date

library(here)
library(tidyverse)
library(googledrive)
library(discharge)
library(data.table)
library(lubridate)
library(zoo)
library(duckdb)

######################
### Discharge data ###
######################
qURL <- "https://drive.google.com/drive/folders/11VAgmvuxp2Xj5fqFi8qkq9B6VhdxlWKj"

Q_new <- drive_get(as_id(qURL))

Q_glist <- drive_ls(Q_new, pattern = "wildfire_discharge.duckdb")

#I know this is hinky with the switching of working directory. The drive_download command does not currently allow downloading into subdirectories.
setwd(here("data"))
walk(Q_glist$id, ~ drive_download(as_id(.x), overwrite = TRUE))

setwd(here())

duck_db <- DBI::dbConnect(duckdb::duckdb(), here("data", "wildfire_discharge.duckdb"))

dat <- DBI::dbReadTable(duck_db, "daily_discharge_complete")

# Format dates & set up column names for DFFT
dat <- dat %>% mutate(time = as.Date(time, format = "%Y-%m-%d")) %>%
               setNames(c("site", "date", "cfs")) %>%
               mutate(discharge = cfs*0.028316846592) %>% # convert to m^3 s-1
               select(-cfs)

##############################
### Prep data for analysis ###
##############################
## summary stats on length of record for each site
summ.dat <- dat %>% mutate(year = year(date)) %>%
                    group_by(site) %>%
                    summarize(n_years = n_distinct(year))

## Pare data to 25 y
## Fill in NA rows for all missing daily time steps
# First remove all NA to remove trailing and leading NAs
dat.na <- dat %>% filter(is.na(discharge) == FALSE)
  
# Fill in all missing daily timesteps
dat.na <- dat.na %>% filter(is.na(date) == FALSE) %>%
                     group_by(site) %>% 
                     complete(date = seq(min(date), max(date), by = "day"))

t.dat <- dat.na %>% group_by(site) %>%
                    filter(date >= max(date, na.rm = TRUE) %m-% years(25))

# Streams with < 10 years data:
  # USGS-10299300 4
  # USGS-07110400 3
  # USGS-13155700 3
  # USGS-13155620 4
  # USGS-13154400 9

# short records
short.dat <- t.dat %>% mutate(year = year(date)) %>%
                       group_by(site) %>%
                       summarize(n_years = n_distinct(year)) %>%
                       filter(n_years < 10)

## Summarize NAs within each year
# display >25 NAs per year*site
summ.na <- t.dat %>% group_by(site) %>%
                     # Fill in all dates between the min and max date for each group
                     complete(date = seq.Date(min(date), max(date), by = "day")) %>%
                     mutate(year = year(date)) %>%
                     group_by(site, year) %>%
                     summarize(missing_Q = sum(is.na(discharge))) %>%
                     filter(missing_Q > 25) %>%
                     arrange(site, year) 

# Problematic sites due to NAs:
sig.na <- t.dat %>% group_by(site) %>%
                    # Fill in all dates between the min and max date for each group
                    complete(date = seq.Date(min(date), max(date), by = "day")) %>%
                    mutate(year = year(date)) %>%
                    group_by(site, year) %>%
                    summarize(missing_Q = sum(is.na(discharge))) %>%
                    filter(missing_Q > 25) %>%
                    ungroup() %>%
                    distinct(site)

## Remove problematic sites and convert to list
# problematic sites (45)
probs <- bind_rows(sig.na, short.dat) %>%
         select(site) %>%
         unique()

# Remaining sites (>10 y, <25 NA/month) = 87
tdat.suff <- t.dat %>% filter(!site %in% probs$site) %>%
                       arrange(date) %>%
                       ungroup()

# to list
tdat.lst <- split(tdat.suff, tdat.suff$site)

# convert all Q to numeric
tdat.lst <- lapply(tdat.lst, function(x) {
                    x$discharge <- as.numeric(x$discharge)
  return(x)
})

## Remove site column, required for DFFT
tdat.lst <- lapply(tdat.lst, select, -"site")

## convert from tibble to dataframe
tdat.lst <- lapply(tdat.lst, data.frame)

## troubleshooting
u9000 <- data.frame(tdat.lst["USGS-06259000"]) # no NAs
u4300 <- data.frame(tdat.lst["USGS-06274300"]) # 9 NAs
u9500 <- data.frame(tdat.lst["USGS-06279500"]) # 10 NAs
u4000 <- data.frame(tdat.lst["USGS-06635000"]) # no NAs

u9000 <- setNames(u9000, c("date", "discharge"))
u4000 <- setNames(u4000, c("date", "discharge"))
u4300 <- setNames(u4300, c("date", "discharge"))
u9500 <- setNames(u9500, c("date", "discharge"))

u9000$site <- "u9000"
u4000$site <- "u4000"
u9500$site <- "u9500"
u4300$site <- "u4300"

nona <- bind_rows(u9000, u4000)
nona.lst <- split(nona, nona$site)

#####################
### DFFT analysis ###
#####################
flow.list <- lapply(tdat.lst, asStreamflow, max.na = 5000)
seas.list <- lapply(flow.list, fourierAnalysis)

## DFFT fit plots
lapply(names(seas.list), function(x) {
  pdf(file = paste0("plots/", "DFFT-", x, ".pdf"))
  print(plot(seas.list[[x]]))
  title(paste0("\n", x))
  dev.off()
})

## troubleshoot
flows <- asStreamflow(u4000)
flows1 <- asStreamflow(u9000)
seas <- fourierAnalysis((flows)) # x & y differ; plots time series without seasonal signal
seas1 <- fourierAnalysis(flows1) # seas signal is flat w/ shark fins

flow2 <- asStreamflow(u4300)
flows3 <- asStreamflow(u9500)
seas2 <- fourierAnalysis((flow2)) # x & y differ
seas3 <- fourierAnalysis(flows3) # x & y differ

### USGS gauge 06259000 
## mean daily Q

u9000d <- read_waterdata_daily(monitoring_location_id = "USGS-06259000", 
                               parameter_code="00060", 
                               statistic_id = "00003", 
                               time = c("1923-10-01", edate = "2025-09-31"))

# Time series plot
u9000d.ts <- u9000d %>% ggplot(aes(x = time, y = value)) +
  geom_line() +
  labs(y = expression("discharge ("*ft^3*s^{-1}*")")) +
  #scale_y_log10() +
  theme_bw() +
  theme(panel.grid = element_blank(),
        panel.border = element_rect(color = "black", linewidth = 2),
        axis.text = element_text(size = 20),
        axis.title.y = element_text(size = 20),
        axis.title.x = element_blank())

#format for discharge package: Date (YYY-MM-DD) in column1, CFS (val) in column 2
u9000d <- data.frame(u9000d)
u9000dQ <- u9000d %>% select(Date = time, CFS = value) 

u9000d.flow <- asStreamflow(u9000dQ, river.name = "06259000", max.na = 50)
u9000d.seas <- fourierAnalysis(u9000d.flow)

pdf(file = "plots/u9000DFFT.pdf", width = 8.5, height = 6.5, pointsize = 20)
plot(u9000d.seas)
dev.off()
# all good

# now filter to last 25 years
u9000.25 <- u9000dQ %>% filter(Date >= max(Date, na.rm = TRUE) %m-% years(25))
  
u9000d.25.flow <- asStreamflow(u9000.25, river.name = "06259000", max.na = 50)
u9000d.25.seas <- fourierAnalysis(u9000d.25.flow)

pdf(file = "plots/u9000.25.DFFT.pdf", width = 8.5, height = 6.5, pointsize = 20)
plot(u9000d.25.seas)
dev.off()

# 06279500
u9500d <- read_waterdata_daily(monitoring_location_id = "USGS-06279500", 
                               parameter_code="00060", 
                               statistic_id = "00003", 
                               time = c("1923-10-01", edate = "2025-09-31"))

# Time series plot
u9500d.ts <- u9500d %>% ggplot(aes(x = time, y = value)) +
  geom_line() +
  labs(y = expression("discharge ("*ft^3*s^{-1}*")")) +
  #scale_y_log10() +
  theme_bw() +
  theme(panel.grid = element_blank(),
        panel.border = element_rect(color = "black", linewidth = 2),
        axis.text = element_text(size = 20),
        axis.title.y = element_text(size = 20),
        axis.title.x = element_blank())

#format for discharge package: Date (YYY-MM-DD) in column1, CFS (val) in column 2
u9500d <- data.frame(u9500d)
u9500dQ <- u9500d %>% select(Date = time, CFS = value) 

u9500d.flow <- asStreamflow(u9500dQ, river.name = "06259500", max.na = 50)
u9500d.seas <- fourierAnalysis(u9500d.flow)

pdf(file = "plots/u9500DFFT.pdf", width = 8.5, height = 6.5, pointsize = 20)
plot(u9500d.seas)
dev.off()
# all good

# now filter to last 25 years
u9500.25 <- u9500dQ %>% filter(Date >= max(Date, na.rm = TRUE) %m-% years(25))

u9500d.25.flow <- asStreamflow(u9500.25, river.name = "06259500", max.na = 50)
u9500d.25.seas <- fourierAnalysis(u9500d.25.flow)

pdf(file = "plots/u9500.25.DFFT.pdf", width = 8.5, height = 6.5, pointsize = 20)
plot(u9500d.25.seas)
dev.off()

# now let's compare record for 06259000 when downloaded directly and from duckdb
u9500 <- u9500 %>% select(c("date", "discharge")) %>%
                   rename(Date = date)

test <- full_join(u9500.25, u9500)

test %>% ggplot(aes(x = CFS, y = discharge)) +
          geom_point()

#########################
## test on direct download
#### next step is to put the 2 downloaded records into same dataframe and pull through processing steps above
test.dat <- bind_rows(u9500d, u9000d) %>%
            select(c("monitoring_location_id", "time", "value"))

test.dat <- test.dat %>% mutate(time = as.Date(time, format = "%Y-%m-%d")) %>%
                         setNames(c("site", "date", "cfs")) %>%
                         mutate(discharge = cfs*0.028316846592) %>% # convert to m^3 s-1
                         select(-cfs)

##############################
### Prep data for analysis ###
##############################
## summary stats on length of record for each site
summ.dat <- test.dat %>% mutate(year = year(date)) %>%
  group_by(site) %>%
  summarize(n_years = n_distinct(year))

## Pare data to 25 y
## Fill in NA rows for all missing daily time steps
# First remove all NA to remove trailing and leading NAs
testdat.na <- test.dat %>% filter(is.na(discharge) == FALSE)

# Fill in all missing daily timesteps
testdat.na <- testdat.na %>% filter(is.na(date) == FALSE) %>%
                             group_by(site) %>% 
                             complete(date = seq(min(date), max(date), by = "day"))

t.dat <- testdat.na %>% group_by(site) %>%
  filter(date >= max(date, na.rm = TRUE) %m-% years(25))

# ungroup
t.dat <- t.dat %>% arrange(date) %>%
                       ungroup()

# to list
tdat.lst <- split(t.dat, t.dat$site)

# convert all Q to numeric
tdat.lst <- lapply(tdat.lst, function(x) {
  x$discharge <- as.numeric(x$discharge)
  return(x)
})

## Remove site column, required for DFFT
tdat.lst <- lapply(tdat.lst, select, -"site")

## convert from tibble to dataframe
tdat.lst <- lapply(tdat.lst, data.frame)

flow.list <- lapply(tdat.lst, asStreamflow, max.na = 5000)
seas.list <- lapply(flow.list, fourierAnalysis)

## DFFT fit plots
lapply(names(seas.list), function(x) {
  pdf(file = paste0("plots/", "DFFT-", x, ".pdf"))
  print(plot(seas.list[[x]]))
  title(paste0("\n", x))
  dev.off()
})

# testing on direct download: WORKS!

####
## Now try extracting these two sites from duckdb and run
duck.dat <- dat %>% mutate(time = as.Date(time, format = "%Y-%m-%d")) %>%
  setNames(c("site", "date", "cfs")) %>%
  mutate(discharge = cfs*0.028316846592) %>% # convert to m^3 s-1
  select(-cfs)

## filter USGS-06279500 & USGS-06259500
duck.dat <- duck.dat %>% filter(site %in% c("USGS-06259000", "USGS-06279500"))

##############################
### Prep data for analysis ###
##############################
## summary stats on length of record for each site
summ.dat <- duck.dat %>% mutate(year = year(date)) %>%
  group_by(site) %>%
  summarize(n_years = n_distinct(year))

## Pare data to 25 y
## Fill in NA rows for all missing daily time steps
# First remove all NA to remove trailing and leading NAs
duckdat.na <- duck.dat %>% filter(is.na(discharge) == FALSE)

# Fill in all missing daily timesteps
duckdat.na <- duckdat.na %>% filter(is.na(date) == FALSE) %>%
  group_by(site) %>% 
  complete(date = seq(min(date), max(date), by = "day"))

tduck.dat <- duckdat.na %>% group_by(site) %>%
  filter(date >= max(date, na.rm = TRUE) %m-% years(25))

# ungroup
tduck.dat <- tduck.dat %>% arrange(date) %>%
  ungroup()

# to list
duckdat.lst <- split(tduck.dat, tduck.dat$site)

# convert all Q to numeric
duckdat.lst <- lapply(duckdat.lst, function(x) {
  x$discharge <- as.numeric(x$discharge)
  return(x)
})

## Remove site column, required for DFFT
duckdat.lst <- lapply(duckdat.lst, select, -"site")

## convert from tibble to dataframe
duckdat.lst <- lapply(duckdat.lst, data.frame)

duckflow.list <- lapply(duckdat.lst, asStreamflow, max.na = 5000)
duckseas.list <- lapply(flow.list, fourierAnalysis)

## DFFT fit plots
lapply(names(duckseas.list), function(x) {
  pdf(file = paste0("plots/", "DFFT-", x, ".pdf"))
  print(plot(seas.list[[x]]))
  title(paste0("\n", x))
  dev.off()
})


##############
### trying another way
flow.func <- function(site){
  dat <- tdat.suff %>% filter(site == site) %>% select(date, cfs) %>% arrange(date)
  dat.flow <- asStreamflow(dat, river.name = site, max.na = 5000) # Remove cutoff for maximum NA values allowed
  dat.seas<-fourierAnalysis(dat.flow)
  
  data <- dat.seas$signal
  data$site <- paste(site)
  data$seasonal <- dat.seas$seasonal 
  data <- data %>% mutate(across(seasonal, ~ifelse(seasonal == 1, "yes", "no")))
  return(data)
}

# Run DFFT for all sites
sites <- unique(tdat.suff$site)
df.list <- lapply(sites, flow.func)
flow.dat <- rbindlist(df.list)
head(flow.dat)
######

#######################
### Summary metrics ###
#######################
## Metrics: FPExt, HSAF, HSAM, LSAF, LSAM, NAA, rms.noise, rms.signal, snr, Zdays, Seasonal, HFsigma, LFsigma
# All metrics that are not originally composite (such as HFsigma, SNR, etc.) are averaged over the full Q record
### ***Currently calculated on calendar years- should convert to water year basis?*** Or running values?
DFFTavg <- function(X.flow, X.seas, SiteName){
  X.bl <- prepareBaseline(X.flow)
  X.FPExt <- getFPExt(X.bl$resid.sig, X.flow$data$year)
  X.HSAF <- getHSAF(X.bl$resid.sig, X.flow$data$year)
  X.HSAM <- getHSAM(X.bl$resid.sig, X.flow$data$year)%>%
    select(year, HSAM)
  X.LSAF <- getLSAF(X.bl$resid.sig, X.flow$data$year)
  X.LSAM <- getLSAM(X.bl$resid.sig, X.flow$data$year)%>%
    select(year, LSAM)
  X.NAA <- getNAA(X.bl$resid.sig, X.flow$data$year)
  X.HFsigma <- sigmaHighFlows(X.flow) #Use X.HFsigma$sigma.hfb
  X.LFsigma <- sigmaLowFlows(X.flow) #Use X.LFsigma$sigma.hfb
  X.Zdays <- X.seas$signal%>% #Average number of zero flow days per year
    group_by(year)%>%
    summarize(Zdays=sum(discharge <0.1))
  #Find average of each DFFT metric
  X.fftavg <- bind_cols(Site=SiteName, HSAM=mean(X.HSAM$HSAM, na.rm=T),LSAM=mean(X.LSAM$LSAM, na.rm=T),
                        HSAF=mean(X.HSAF$HSAF, na.rm=T), LSAF=mean(X.LSAF$LSAF, na.rm=T), FPExt=mean(X.FPExt$FPExt),
                        NAA=mean(X.NAA$NAA), HFsigma=mean(X.HFsigma$sigma.hfb), LFsigma=mean(X.LFsigma$sigma.lfb))
  X.fftavg <- bind_cols(X.fftavg, rms.signal=X.seas$rms$rms.signal,
                        rms.noise=X.seas$rms$rms.noise, snr=X.seas$rms$snr,
                        Zdays=mean(X.Zdays$Zdays, na.rm=T), Seasonal=X.seas$seasonal)
}

met.out <- mapply(x = flow.list, y = seas.list, z = names(seas.list), FUN = function(x, y, z) DFFTavg(x, y, z))
met.out.df <- met.out[-1,] %>%
  as_tibble(., rownames = "metric") %>%
  unnest(-metric) %>%
  pivot_longer(!metric, names_to = "site", values_to = "value")

## Metrics (up to 25 y pre-fire) to use as covariates:
# snr: signal-to-noise ratio, metric of repeatability of seasonal signal through time, rms.signal/rms.noise
# rms.signal: measures seasonal variation. rm.signal = 0 for no seasonal variation
write.csv(met.out, here("hydro", "DFFT_avg_25years.csv"), row.names = FALSE)

## Plot
metrics.pl <- met.out.df %>% ggplot(aes(x = site, y = value)) +
  geom_point() +
  facet_wrap(~metric, scales = "free_y") +
  theme_bw() +
  theme(axis.text.x = element_text(angle = 90, hjust = 1),
        axis.title.x = element_blank(),
        axis.title.y = element_blank())

ggsave(metrics.pl, path = here("hydro", "plots"), file = "DFFT_metrics_25y.pdf", width = 10, height = 8, units = "in")

#################################################################
### Summarize metrics over 3 years pre- and 3 years post-fire ###
#################################################################
### ****Goals here- ave metrics pre- and post-fire, then sum of + anomalies post-fire-- filter flow and seas to 3 y pre & 3 y post fire first?
## !!!Probably not using this!!! ##
# Metrics by year
####****Summarizing by calendar year... Switch to years in fire timeline or water years?***####
#DFFTyr <- function(X.flow, X.seas, SiteName){
#  X.bl <- prepareBaseline(X.flow)
#  X.FPExt <- getFPExt(X.bl$resid.sig, X.flow$data$year)
#  X.HSAF <- getHSAF(X.bl$resid.sig, X.flow$data$year)
#  X.HSAM <- getHSAM(X.bl$resid.sig, X.flow$data$year)%>%
#    select(year, HSAM)
#  X.LSAF <- getLSAF(X.bl$resid.sig, X.flow$data$year)
#  X.LSAM <- getLSAM(X.bl$resid.sig, X.flow$data$year)%>%
#    select(year, LSAM)
#  X.NAA <- getNAA(X.bl$resid.sig, X.flow$data$year)
#  X.HFsigma <- sigmaHighFlows(X.flow) #Use X.HFsigma$sigma.hfb
#  X.LFsigma <- sigmaLowFlows(X.flow) #Use X.LFsigma$sigma.hfb
#  X.Zdays <- X.seas$signal%>% #Average number of zero flow days per year
#    group_by(year)%>%
#    summarize(Zdays = sum(discharge <0.1))
#  X.fftavg <- bind_cols(Site = SiteName, HSAM = X.HSAM$HSAM, LSAM = X.LSAM$LSAM,
#                        HSAF = X.HSAF$HSAF, LSAF = X.LSAF$LSAF, FPExt = X.FPExt$FPExt,
#                        NAA = X.NAA$NAA, HFsigma = X.HFsigma$sigma.hfb, LFsigma = X.LFsigma$sigma.lfb,
#                        X.Zdays)
#}

#met.out.y <- mapply(x = flow.list, y = seas.list, z = names(seas.list), FUN = function(x, y, z) DFFTyr(x, y, z))

#met.out.y.df <- met.out.y %>%
#  as_tibble(., rownames = "metric") %>%
#  unnest(-metric) 
#%>%
#  pivot_longer(!metric, names_to = "site", values_to = "value")

## Try single site...need to get year as a column
## This works
#ss.flow <- flow.list[["07103700"]]
#ss.seas <- seas.list[["07103700"]]

#ss.metrics <- DFFTyr(ss.flow, ss.seas, "07103700")

## filter to pre-fire & filter to post-fire
# Note change years(x) if revising pre-/post-fire window
t.sites.pp <- t.sites %>% mutate(before.fire = igDate - years(3)) %>%
  mutate(after.fire = igDate + years(3)) %>%
  mutate(ybeffire = format(as.Date(before.fire, format = "%Y-%m-%d"), "%Y")) %>%
  mutate(yaftfire = format(as.Date(after.fire, format = "%Y-%m-%d"), "%Y"))

#names(ss.metrics)[names(ss.metrics) == 'Site'] <- 'usgs_site'
#ss.metrics.f <- left_join(ss.metrics, t.sites.pp, by = "usgs_site") 

###########################
### High-flow anomalies ###
###########################
# sum of pre-fire and post-fire high-flow anomalies

## Extract positive anomalies
panom <- lapply(seas.list, function(x) subset(x$signal, x$signal$resid.sig >= 0) )

# Join site IDs to anomalies
panom <- Map(cbind, panom, usgs_site = names(panom))
panom <- lapply(panom, function(x) left_join(x, t.sites.pp, by = "usgs_site"))

# sum pre- and post-fire positive anomalies
pre.sum <- panom %>% map(~ .x %>%
                           filter(date >= before.fire & date <= igDate) %>%
                           summarize(bpos = sum(resid.sig)))

post.sum <- panom %>% map(~ .x %>%
                            filter(date >= igDate & date <= after.fire) %>%
                            summarize(apos = sum(resid.sig)))

dates <- panom %>% map(~ .x %>%
                         select(c("usgs_site", "event_id", "igDate", "before.fire", "after.fire")) %>%
                         unique())

# combine
pre.sum.df <- bind_rows(pre.sum, .id = "usgs_site")
post.sum.df <- bind_rows(post.sum, .id = "usgs_site")
dates.df <- bind_rows(dates, .id = "usgs_site")

pos.anom <- left_join(dates.df, pre.sum.df)
pos.anom <- left_join(pos.anom, post.sum.df)
pos.anom <- pos.anom %>% mutate(panomdiff = bpos - apos)

### Plots ###
hiflow.pl <- pos.anom %>% pivot_longer(cols = c("bpos", "apos"), names_to = "fire", values_to = "HSAM") %>%
  ggplot(aes(x = fire, y = HSAM, group = usgs_site, color = usgs_site)) +
  geom_point() +
  geom_line(aes(group = usgs_site)) +
  scale_x_discrete(limits = c("bpos", "apos"),
                   labels = c("3 y pre-fire", "3 y post-fire")) +
  ylab("high-flow anomalies") +
  theme_bw() +
  theme(axis.title.x = element_blank(),
        axis.text = element_text(size = 14),
        axis.title.y = element_text(size = 14),
        legend.title = element_blank(),
        legend.text = element_text(size = 14),
        plot.title = element_text(hjust = 0.5, size = 14),
        panel.grid = element_blank())

ggsave(hiflow.pl, path = here("hydro", "plots"), file = "demoHSAM.pdf", width = 6, height = 4, units = "in")

###***other possibilities:
###*days since fire of largest positive anomaly OR some fraction of cdf of net anomalies?
###*number days with positive anomalies post-fire
###*Average time between positive anomalies above some threshold

