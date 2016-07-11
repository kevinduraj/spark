Spark Submit Scala Jobs 
=======================

###Submit Spark Job
* dse spark -i:spark-job.scala


###Write apostrophe  into FlatFile
```
cat selected | awk  '{ print ",\x22"$1"\x22" }' | tee -a ../save-filter-table.scala 
```

