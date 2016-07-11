#!/bin/bash

echo "USE engine;" > delete.cql
cat /home/spider/data/3_bad_domains.txt | awk '{print "DELETE FROM links WHERE domain=\x27"$1"\x27;"}' | tee -a delete.cql

cqlsh < delete.cql
