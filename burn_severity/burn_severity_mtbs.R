##burn severity extractions by watershed 
##built to use with yearly mtbs tifs
#fire data clipped by watershed boundry 
library(sf)
library(raster)
library(tidyverse)
library(rgdal)
library(ggplot2)
library(exactextractr)
library(gtools)

setwd("~/Documents/projects/lter-arid-streams")
basin <- sf::read_sf("catchment_07227100.geojson") ##catchment 
allfires <- sf::read_sf("mtbs_perimeter_data/mtbs_perims_DD.shp") ###all mtbs fire perimeters (1984-2022)

############align shapefiles################
#st_crs(basin) ##make sure crs matches mtbs
#st_crs(allfires)
basinreproj <- st_transform(basin, crs(allfires))
st_crs(basinreproj)
 ##make sure crs matches mtbs

#########select only fires within catchment############
sf_use_s2(FALSE)
basinfires <- st_intersection(basinreproj, allfires) ###I should come back and check on this, not sure if it clips or keeps full fire despite being outside watershed
##check <- sf::read_sf("catchment_fires_07227100 (1).geojson") ###check with steven's given data


##############main function#############
##years is list of unique years where a fire did occur within the basin/catchment (this is for processing time)
##basinfires is a sf objject of fire perimeters within the basin/catchment 
##filebase is the path and file namebase to mtbs severity mosaics divided by year

mosaicloop <- function(years, basinfires, filebase) { ##years can also be a sequence
  outdf <- c()
  for (i in years){
    print(i)
    yrrast <- raster(paste0(filebase,i,".tif"))                                 ##select and load single year raster
    firesyr <- subset(basinfires, year(basinfires$Ig_Date) ==i)                           ## select fires for that year
    for (k in unique(firesyr$Event_ID)){                                        ##this is for when multiple fires happen in 1 year 
      onefire <- subset(firesyr, firesyr$Event_ID == k)                         ##select one fire event
      zs <- exact_extract(yrrast,onefire,coverage_area=T,default_value=-9999)   ##extract data
      zsdf <- makedf(zs)                                                        ##this makes a db from that data (as.data.frame doesn't work here)
      zsdf <- subset(zsdf, zsdf$value != -9999)                                 ##remove NAs
      percentfire <- zsdf %>% group_by(value) %>% summarise(percentfire = (sum(coverage_area)/(onefire$BurnBndAc*4047))*100)##burnbndac = burned acres 
      percentbasin <- zsdf %>% group_by(value) %>% summarise(percentbasin = (sum(coverage_area)/onefire$area_m2)*100) ## basin area
      out <- merge(percentfire,percentbasin,merge_by="value") 
      out$year <- i
      out$event_id <- k
      outdf <- rbind(outdf,out)
    }
  }
  return(outdf)
}

##this can work with the CONUS tifs but using NM tifs here for SPEED
years <- unique(year(basinfires$Ig_Date))

test <- mosaicloop(years,basinfires=basinfires,filebase="NM_mtbs/mtbs_NM_")

#write.csv(test,file="burn_severity_subset.csv")

############processing time##############
## 1 catchment with 7 fires and 6 years took 1.28seconds to run.
##regional estimates with all 30924 fires in MTBS db should take ~109 minutes
##not accounting for st_intersection time OR raster load time
##could bloat processing time above 5 hours 


########subfunction needed to convert extracted data to df ###############
makedf <- function(extracted) { ##takes exact_extract output and makes data.frame
  output <- c()
  for (i in extracted){
    output <- rbind(output,i)
  }
  return(output)
}








