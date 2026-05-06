install.packages("VedicDateTime")

library(tidyverse)
library(quantreg)


dat <- read.csv("combined_time_series.csv",header=T)
prcp <- read.csv("fire-arid_precipitation.csv",header = T)

head(dat)
head(prcp)
print(prcp)

dat$year <- year(dat$date)
dat$day <- format(as.Date(dat$date), format = "%j")
head(dat)
nrow(dat)
wantusgs <- unique(dat$usgs_site)
length(wantusgs)
prcp_sub <- prcp %>% filter(usgs_site %in% wantusgs)

###merge dat and prcp by usgs_site and 
withpr <- merge(dat,prcp_sub,by=c("usgs_site","year","day"))
nrow(withpr)

write.csv(withpr,"combined_with_precipitation.csv",row.names=F)


