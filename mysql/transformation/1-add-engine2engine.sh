#!/bin/bash
#------------------------------------------------------------------------------------#
# mysql_config_editor set --login-path=local --host=localhost --user=root --password
#------------------------------------------------------------------------------------#
USERNAME='root'
DATABASES='engine78'
DESTINATION='engine77'
#------------------------------------------------------------------------------------#

for DATABASE in $DATABASES
do  
  TABLE_LIST=`mysql --login-path=local -NB -e "SHOW TABLES FROM $DATABASE"`

  for E in $TABLE_LIST
  do
      SQL="INSERT LOW_PRIORITY INTO $DESTINATION.$E SELECT * FROM $DATABASE.$E "
      echo "$SQL"
      res1=`mysql --login-path=local -NB -e  "$SQL"`
      echo "$res1"
  done
done

#------------------------------------------------------------------------------------#


