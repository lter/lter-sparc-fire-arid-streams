library(tidyverse)
library(ggplot)


nitrate <- read.csv("/Users/ash/Documents/projects/CRASS/lter-sparc-fire-arid-streams/brms model and analysis/data/processed/nitrate_largest_pre_post_covariates.csv")
str(nitrate)


#add level 2 eco
eco <- read.csv("/Users/ash/Documents/projects/CRASS/study_sites_ecoregions.csv")
str(eco)

nitrate <- merge(nitrate, eco[,c(4,6)], by="na_l3name")
str(nitrate)


nitrate$uniqID <- paste0(nitrate$event,"-",nitrate$usgs_site)

nitrate$uniqID[1]

nitrate2 <- merge(nitrate, fs_new, by="uniqID", all.x=TRUE)
str(nitrate2)

write.csv(nitrate, "working-nitrate.csv")

fs <- read.csv("/Users/ash/Documents/projects/CRASS/cbi-mass-export/crass_cbi_pct_by_basin-3-5.csv")

##check for distinct gage-site-year
fs$uniqID <- paste0(fs$fireid,"-",fs$GAGE_ID)
uq <- length(unique(fs$uniqID))

##have some overlap from processing, remove those 
fs_new <- fs[!duplicated(fs$uniqID),]
colnames(fs_new)

fs_new$cbi_weighted <- (1 * fs_new$pct_class_1) + (2 * fs_new$pct_class_2) + (3 * fs_new$pct_class_3)

write.csv(fs_new, "all-crass-cbi.csv")
fire <- read.csv("all-crass-cbi.csv")
str(fire)
###facetting by ecoregion, plot value_std by covariates 


###Fire size by ecoregion
allchem <- read.csv("lter-sparc-fire-arid-streams/brms model and analysis/data/processed/nitrate_largest_pre_post_covariates.csv")
str(allchem)

allchem$eventID <- gsub("[{}]","",allchem$event)
allchem$eventID[1]
tr <- strsplit(allchem$eventID[2971],",")[[1]]
allchem$cum_cbi <- 0

newchem <- c()
uniqcats <- unique(allchem$usgs_site)
k<- uniqcats[1]

for (k in uniqcats) {
  print(k)
  onerw <- allchem[allchem$usgs_site == k,]
  print(length(onerw$eventID))
  #print(onerw)
  ev <- strsplit(onerw$eventID,",")[[1]]
  print(ev)
  frs <- 0
  for (d in ev){
        ev1 <- d
        fr <- fire[fire$fireid == ev1 & fire$GAGE_ID == k,]
        frs <- sum(frs + fr$cbi_weighted)
  }
  print(frs)
  onerw$cum_cbi <- as.numeric(frs)
  newchem <- rbind(newchem, onerw)
}
newchem$event 
hist(unique(newchem$cum_cbi))

##slope 
p1 <- ggplot(data=nitrate, aes(x=slope_avg_deg))+
        geom_histogram()+
        #geom_vline(aes(xintercept=mean(slope_avg_deg))) +
        
        facet_wrap(~na_l2name)
p1

str(newchem)

newchem_sum <- newchem %>%
  group_by(usgs_site) %>%
  summarise(mean_cbi = mean(cum_cbi, na.rm=TRUE),mean_per_cent_burned = mean(cum_per_cent_burned, na.rm=TRUE),mean_slope = mean(slope_avg_deg, na.rm=TRUE),na_l3name = first(na_l3name))

###percent burned by ecoregion
p1 <- ggplot(data=newchem_sum, aes(x=mean_per_cent_burned))+
        geom_histogram()+
        xlab("Cumulative percent of catchment burned")+
        #geom_vline(aes(xintercept=mean(pct_burned))) +
        facet_wrap(~na_l3name)
p1
ggsave("lter-sparc-fire-arid-streams/brms model and analysis/covariate-plots/percent-burn_by_ecoregion.png",p1)

p2 <- ggplot(data=newchem_sum, aes(x=mean_cbi))+
        geom_histogram()+
        xlab("Cumulative Burn Severity Index")+
        #geom_vline(aes(xintercept=mean(cbi_weighted))) +
        facet_wrap(~na_l3name)
p2

ggsave("lter-sparc-fire-arid-streams/brms model and analysis/covariate-plots/cbi_by_ecoregion.png",p2)

p3 <- ggplot(data=newchem_sum, aes(x=mean_slope))+
        geom_histogram()+
        xlab("Mean slope of catchment (degrees)")+
        #geom_vline(aes(xintercept=mean(slope_avg_deg))) +
        facet_wrap(~na_l3name)
p3

ggsave("lter-sparc-fire-arid-streams/brms model and analysis/covariate-plots/slope_by_ecoregion.png",p3)

