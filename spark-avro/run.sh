#!/bin/bash
#----------------------------------------------------------#
rm -fR /tmp/spark/avro/
sbt clean
sbt package

#----------------------------------------------------------#
spark-submit            \
  --packages com.databricks:spark-avro_2.10:2.0.1  \
  --class "SparkAvro"   \
  --master local[4]      \
  --driver-memory 4G     \
  --executor-memory 4G   \
  target/scala-2.10/sparkavro_2.10-1.0.jar  \
  src/test/resources/episodes.avro

#----------------------------------------------------------#
ls -l /tmp/spark/avro/
