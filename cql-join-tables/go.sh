#!/bin/bash

cqlsh -f create-schema.cql

cqlsh -f insert-data.cql

cqlsh -f select-tables.cql

dse spark -i:joining-tables.scala 

cqlsh -f select-tables.cql

