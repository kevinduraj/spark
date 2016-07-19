#!/bin/bash

cqlsh 192.168.1.159 -e "COPY engine.links_health2 TO '/home/temp/links_health2' WITH DELIMITER = ' ';"
