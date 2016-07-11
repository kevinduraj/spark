#!/bin/bash

clear;
cqlsh 192.168.1.159 < export.cql
sort -n -k2,2 temp.csv | tail -50 | tee temp
rm -f temp.csv
cat temp

