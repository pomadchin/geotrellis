#!/bin/bash

./sbt -Dsbt.supershell=false "++$SCALA_VERSION" \
  "project accumulo" publishLocal \
  "project accumulo-spark" publishLocal \
  "project cassandra" publishLocal \
  "project cassandra-spark" publishLocal \
  "project gdal" publishLocal \
  "project gdal-spark" compile \
  "project geotools" publishLocal \
  "project hbase" publishLocal \
  "project hbase-spark" publishLocal \
  "project layer" publishLocal \
  "project proj4" publishLocal \
  "project raster" publishLocal \
  "project raster-publishLocalkit" publishLocal \
  "project s3" publishLocal \
  "project s3-spark" publishLocal \
  "project shapefile" publishLocal && \
./sbt -Dsbt.supershell=false "++$SCALA_VERSION" \
  "project spark" publishLocal && \
./sbt -Dsbt.supershell=false "++$SCALA_VERSION" \
  "project spark-pipeline" publishLocal \
  "project spark-publishLocalkit" publishLocal \
  "project store" publishLocal \
  "project util" publishLocal \
  "project vector" publishLocal \
  "project vector-publishLocalkit" publishLocal \
  "project vectortile" publishLocal || { exit 1; }
