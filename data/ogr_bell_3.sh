#! /bin/bash

ogr2ogr -f GeoJSON -s_srs Bell3_watershed.prj -t_srs EPSG:4326 hanan_bell_3_ws.geojson Bell3_watershed.shp
ogr2ogr -f GeoJSON -s_srs Bell3_PourPoint.prj -t_srs EPSG:4326 hanan_bell_3_pp.geojson Bell3_PourPoint.shp
