#!/bin/bash



# add Spark Conf variables
# master
master="yarn-client"

# add env vars
# for example location for mts streaming app jar
MTS_STREAMING_ETL=

# need an mts users who is in the same group a bfgjava


exec sudo -u bfgjava  spark-submit --class com.bigfishgames.spark.install.Transform --master $master /home/data/mts-streaming-etl/mobile-telemetry-assembly-0.0.1.jar

