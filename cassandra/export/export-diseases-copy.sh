#!/bin/bash

cqlsh -e "COPY cloud1.diseases (url) TO '/home/temp/diseases-0714.dat';"

