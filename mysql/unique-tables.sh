#!/bin/bash
#------------------------------------------------------------------------------------#
# mysql_config_editor set --login-path=local --host=localhost --user=root --password
#------------------------------------------------------------------------------------#

repair_tables()
{
    SOURCE="engine50"
    FINAL="engine51"
    TABLE_LIST=`mysql --login-path=local -NB -e "SHOW TABLES FROM $SOURCE"`

     for TABLE in $TABLE_LIST
     do
         SQL="SET sql_mode = '';
              TRUNCATE TABLE $FINAL.$TABLE;
              INSERT INTO $FINAL.$TABLE
                SELECT DISTINCT (sha256url)
                ,md5root
                ,url
                ,root
                ,tags
                ,title
                ,body
                ,MIN(alexa)
                ,MAX(rank)
                ,SUM(hit1)
                ,SUM(hit2)
                ,SUM(hit3)
                ,category
                ,period
                ,AVG(gunning)
                ,AVG(flesch)
                ,AVG(kincaid)
                ,AVG(sentence)
                ,AVG(words)
                ,AVG(syllables)
                ,AVG(complex)
              FROM $SOURCE.$TABLE
              GROUP BY sha256url
              ORDER BY period DESC"
 
 
 
       echo $SQL
       RES=`mysql --login-path=local -NB -e  "$SQL"`
       echo $RES
   done

}
#------------------------------------------------------------------------------------#
#           Repair and optimize all tables in the following databases
#------------------------------------------------------------------------------------#

repair_tables

#------------------------------------------------------------------------------------#
