#!/bin/bash

clear;
cqlsh 192.168.1.159 -e "COPY engine.links_health TO '/home/temp/engine.links_health' WITH DELIMITER = ' ';"

