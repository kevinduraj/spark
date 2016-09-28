#!/bin/bash
#------------------------------------------------------------------------------------#
USERNAME='root'
DATABASES='engine40'
DESTINATION='engine37'
#------------------------------------------------------------------------------------#

for DATABASE in $DATABASES
do  
  TABLE_LIST=`mysql -u $USERNAME -p$PASSWORD -NB -e "SHOW TABLES FROM $DATABASE"`

  for E in $TABLE_LIST
  do
      SQL="INSERT LOW_PRIORITY INTO $DESTINATION.$E SELECT * FROM $DATABASE.$E "
      echo "$SQL"
      res1=`mysql -u $USERNAME -p$PASSWORD -NB -e  "$SQL"`
      echo "$res1"
      sleep 3
  done
done

#------------------------------------------------------------------------------------#


