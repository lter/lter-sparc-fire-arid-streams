## Stream distance between points
##Yvette D. Hastings
##Written: 2-8-2024

##mapping
##load libraries
library(dplyr)
library(sf)
library(ggplot2)
library(ggspatial)
library(raster)
library(riverdist)

##load geojson files
catchment <- sf::read_sf("temp_usgs_data_yvette/catchment_07227100.geojson") ##catchment
catchment_fires <- sf::read_sf("temp_usgs_data_yvette/catchment_fires_07227100.geojson") ##fires
catchment_flowlines <- sf::read_sf("temp_usgs_data_yvette/flowline_07227100.geojson") ##flowlines
catchment_sampling <- sf::read_sf("temp_usgs_data_yvette/sampling_point_07227100.geojson") ##sampling point

##find where streams intersect fire polygons and extract lat/long
intersections_lp <- st_intersection(catchment_flowlines, catchment_fires)

nhd_20018713 <- as.data.frame(intersections_lp[[6]][[1]][[2]])
nhd_20018747 <- as.data.frame(intersections_lp[[6]][[2]][[2]])
nhd_20017833 <- as.data.frame(intersections_lp[[6]][[4]][[2]])
nhd_20017773 <- as.data.frame(intersections_lp[[6]][[7]][[2]]) ##This is the one of most interest
nhd_20018821 <- as.data.frame(intersections_lp[[6]][[10]][[2]])

polygon_intersect <- data.frame(nhd_id = c('Sampling Point', '20018713', '20018747', '20017833', '20017773', '20018821'),
                                event_id = c('NA', 'NM3495910381020160221', 'NM3495910381020160221', 'NM3502110365219900621',
                                             'NM3503310396519980714', 'NM3474310406619930321'),
                                longitude = c(catchment_sampling$X, nhd_20018713[1,1], nhd_20018747[1,1], nhd_20017833[1,1], nhd_20017773[1,1], nhd_20018821[1,1]),
                                latitude = c(catchment_sampling$Y, nhd_20018713[1,2], nhd_20018747[1,2], nhd_20017833[1,2], nhd_20017773[1,2], nhd_20018821[1,2]))

polygon_intersect_sf <- st_as_sf(polygon_intersect, coords = c('longitude', 'latitude'), crs = 4326) #set as sf object

##calculate distances - Euclidean distance
Euclidean_distance <- st_geometry(obj = catchment_fires) %>%
  st_distance(y = catchment_sampling)%>%
  as.data.frame() %>%
  cbind(catchment_fires$event_id, catchment_fires$ig_date)

##calculate distance along stream lines - https://cran.r-project.org/web/packages/riverdist/vignettes/riverdist_vignette.html
wgs84_UTM <- CRS("+proj=utm +zone=13 +datum=WGS84") ##convert WGS84 to UTM projection

flow_network <- line2network(sf = catchment_flowlines, reproject=wgs84_UTM)
plot(flow_network)

topologydots(rivers=flow_network) #check connectedness; everything looks good

cord.dec = SpatialPoints(cbind(polygon_intersect$longitude,polygon_intersect$latitude),proj4string=CRS("+proj=longlat"))
points_xy <- as.data.frame(spTransform(cord.dec, CRS("+proj=utm +zone=13 +datum=WGS84"))) #Convert wgs84 to UTM

#converts XY data to stream locations and plots
points_riv <- xy2segvert(x=points_xy$coords.x1, y = points_xy$coords.x2, rivers = flow_network)
head(points_riv)
hist(points_riv$snapdist, main = 'Snapping Distance (m)', xlab = 'Distance (m)')

zoomtoseg(seg=c(6,313), rivers = flow_network) #zoom map
points(polygon_intersect$longitude, polygon_intersect$longitude, pch = 16, col = 'red')
riverpoints(seg = points_riv$seg, vert = points_riv$vert, rivers = flow_network, pch = 16, col = 'blue')

##calculating distance - need seg and vert from points_riv output
detectroute(start = 6, end = 139, rivers = flow_network) ##this shows which segments are traversed from start to finish

sample_to_20018713 <- riverdistance(startseg = 6, startvert = 54, endseg = 268, endvert = 52, 
                                    rivers = flow_network, map = TRUE) #returns distance in meters
sample_to_20018747 <- riverdistance(startseg = 6, startvert = 54, endseg = 285, endvert = 126, 
                                    rivers = flow_network, map = TRUE) #returns distance in meters
sample_to_20017833 <- riverdistance(startseg = 6, startvert = 54, endseg = 167, endvert = 17, 
                                    rivers = flow_network, map = TRUE) #returns distance in meters
sample_to_20017773 <- riverdistance(startseg = 6, startvert = 54, endseg = 139, endvert = 3, 
                                    rivers = flow_network, map = TRUE) #returns distance in meters
sample_to_20018821 <- riverdistance(startseg = 6, startvert = 54, endseg = 320, endvert = 29, 
                                    rivers = flow_network, map = TRUE) #returns distance in meters

distances <- data.frame(nhdplus_comid = c('20018713', '20018747', '20017833', '20017773', '20018821'),
                        event_id = c('NM3495910381020160221_2016', 'NM3495910381020160221_2016_2', 'NM3502110365219900621_1990',
                                     'NM3503310396519980714_1998', 'NM3474310406619930321_1993'),
                        stream_distance_m = c(sample_to_20018713[1], sample_to_20018747[1], sample_to_20017833[1],
                                     sample_to_20017773[1], sample_to_20018821[1]),
                        stream_distance_km = c(sample_to_20018713[1]/1000, sample_to_20018747[1]/1000, sample_to_20017833[1]/1000,
                                       sample_to_20017773[1]/1000, sample_to_20018821[1]/1000),
                        Euclidean_distance_m = c(Euclidean_distance[1,1], Euclidean_distance[1,1], Euclidean_distance[2,1],
                                               Euclidean_distance[3,1], Euclidean_distance[7,1]))

##map
##https://www.r-bloggers.com/2020/12/visualizing-geospatial-data-in-r-part-2-making-maps-with-ggplot2/
ggplot() +
  geom_sf(data = catchment, fill = 'antiquewhite1', color = 'black') +
  geom_sf(data = catchment_flowlines, aes(color = 'Streams'), show.legend = 'line') +
  geom_sf(data = catchment_fires, color = 'black', aes(fill = 'Fire Boundaries')) +
  geom_sf(data = polygon_intersect_sf, size = 3, aes(color = 'Intersection')) +
  geom_sf(data = catchment_sampling, aes(color = 'Sampling Point'), size = 3, show.legend = 'point') +
  ggtitle("USGS Catchment 07227100") +
  xlab("Longitude") + ylab("Latitude") +
  theme_classic() +
  scale_color_manual(name = ' ', values = c('Streams' = '#0073e6', 'Sampling Point' = '#5ba300', 'Intersection' = 'black'),
                     guide = guide_legend(override.aes = list(linetype = c("blank", "blank", "solid"), 
                                                              shape = c(16, 16, NA)))) +
  scale_fill_manual(name = 'Legend', values = c('Fire Boundaries' = '#b51963'),
                    guide = guide_legend(override.aes = list(linetype = "blank", shape = NA))) +
  theme(plot.title = element_text(hjust = 0.5),
        legend.position = 'right',
        legend.margin = margin(-0.85, 0, 0,0, unit = 'cm')) +
  annotation_scale(location = 'bl',
                   width_hint = 0.5,
                   pad_x = unit(0.3, "cm"),
                   pad_y = unit(0.1, 'cm')) +
  annotation_north_arrow(
    location = "tl",
    width = unit(1, 'cm'),
    height = unit(1, 'cm'),
    pad_x = unit(1, 'cm'),
    pad_y = unit(0.3, "in"),
    style = north_arrow_fancy_orienteering,
    which_north = 'true') +
  scale_x_continuous(limits = c(-103.7, -103.6))+
  scale_y_continuous(limits = c(34.97, 35.08))












