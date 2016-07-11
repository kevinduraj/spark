#!/bin/bash
#------------------------------------------------------------------------------------#
repair_tables()
{
  USERNAME="root"
  DATABASE=$1

  TABLE_LIST=`mysql -u $USERNAME -p$PASSWORD -NB -e "show tables from $DATABASE"`

  for E in $TABLE_LIST
  do
      #SQL="LOCK TABLES $DATABASE.$E WRITE;
      #     REPAIR TABLE $DATABASE.$E;
      #     OPTIMIZE TABLE $DATABASE.$E;
      #     UNLOCK TABLES; "

      SQL="UPDATE $DATABASE.$E SET hit3=0"
      echo $SQL
      RES=`mysql -u $USERNAME -p$PASSWORD -NB -e  "$SQL"`
      echo $RES
      sleep 3;  
  done
}
#------------------------------------------------------------------------------------#
#           Repair and optimize all tables in the following databases
#------------------------------------------------------------------------------------#
mydb="engine39"

for db in $mydb
do  
    echo "Database: is [$db]"
    repair_tables $db
done

#------------------------------------------------------------------------------------#
