CQL
====

### Delete Special Characters
cat delete.sorted | awk '{print "DELETE FROM links WHERE domain=\x27"$1"\x27;"}'
sort -n -k2,2 temp.csv 
