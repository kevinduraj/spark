#!/bin/bash

SCALA='scala-2.10'

# git pull

if [ "$1" == "top" ] 2>/dev/null; then
  
  sbt clean
  sbt assembly
  echo "dse spark-submit --class TopDomains target/$SCALA/spark-engine-assembly-1.0.jar $2 $3"
  dse spark-submit --class TopDomains target/$SCALA/spark-engine-assembly-1.0.jar $2 $3


elif [ "$1" == "total" ] 2>/dev/null; then
  
  sbt clean
  sbt assembly
  echo "dse spark-submit --class Total target/$SCALA/spark-engine-assembly-1.0.jar"
  dse spark-submit --class Total target/$SCALA/spark-engine-assembly-1.0.jar

elif [ "$1" == "visit" ] 2>/dev/null; then
  
  sbt clean
  sbt assembly
  echo "dse spark-submit --class Visit target/$SCALA/spark-engine-assembly-1.0.jar"
  dse spark-submit --class Visit target/$SCALA/spark-engine-assembly-1.0.jar

elif [ "$1" == "links" ] 2>/dev/null; then

  sbt clean
  sbt assembly
  echo "dse spark-submit --class Links target/$SCALA/spark-engine-assembly-1.0.jar $2"
  dse spark-submit --class Links target/$SCALA/spark-engine-assembly-1.0.jar $2

elif [ "$1" == "create_table" ] 2>/dev/null; then

  sbt clean
  sbt assembly
  dse spark-submit --class CreateTable target/$SCALA/spark-engine-assembly-1.0.jar $2

elif [ "$1" == "clear" ] 2>/dev/null; then

  echo "cqlsh -e truncate cloud4.vdomain;" 
  cqlsh -e "truncate cloud4.vdomain;" 
  sleep 1
  echo "cqlsh -e truncate cloud4.ldomain;" 
  cqlsh -e "truncate cloud4.ldomain;" 


else

  echo "--------------------------------"
  echo "./run.sh top vdomain 100000"
  echo "./run.sh top ldomain 1000000"
  echo "--------------------------------"
  echo "./run.sh total"
  echo "./run.sh links"
  echo "./run.sh create_table result2"
  echo "--------------------------------"
  echo "./run.sh clear"

fi


