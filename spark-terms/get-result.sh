#!/bin/bash
DIR='/root/health2.dat'
#----------------------------------------------------#
dse hadoop fs -ls $DIR 
dse hadoop fs -getmerge $DIR health2.dat
dse hadoop fs -rmr $DIR 

