#! /bin/bash

ogr2ogr -f GeoJSON -s_srs Bell4_watershed.prj -t_srs EPSG:4326 hanan_bell_4_ws.geojson Bell4_watershed.shp
ogr2ogr -f GeoJSON -s_srs Bell4_PourPoint.prj -t_srs EPSG:4326 hanan_bell_4_pp.geojson Bell4_PourPoint.shp
