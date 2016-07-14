#!/bin/bash

rm -fR temp/
sbt clean
sbt package

spark-submit \
  --class "SparkWordCount" \
  --master local[4]        \
  --driver-memory 4G       \
  --executor-memory 4G     \
  target/scala-2.10/word-count_2.10-1.0.jar \
  words.txt \
  temp

