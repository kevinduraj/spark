#!/bin/bash

clear;
cqlsh 192.168.1.159 -e "TRUNCATE cloud1.diseases"

