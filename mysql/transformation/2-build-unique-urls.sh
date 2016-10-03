#!/bin/bash
#---------------------------------------------------------------------------------------#
# mysql_config_editor set --login-path=local --host=localhost --user=root --password
#---------------------------------------------------------------------------------------#
SOURCE='engine77'
DESTIN='engine81'
FINAL1='engine82'
START='000'

#---------------------------------------------------------------------------------------#
drop_table()
{
  SQL="DROP TABLE IF EXISTS $DESTIN.part_$START"; echo $SQL
  res1=$(mysql --login-path=local -e  "$SQL"); echo $res1
}
#---------------------------------------------------------------------------------------#
truncate_table()
{
  SQL="TRUNCATE TABLE $FINAL1.part_$START"; echo $SQL
  res1=$(mysql --login-path=local -e  "$SQL"); echo $res1
}
#---------------------------------------------------------------------------------------#
create_schema()
{
  SQL="CREATE DATABASE $DESTIN DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci;"
  echo $SQL
  RES=`mysql --login-path=local -NB -e "$SQL"`
  echo $RES
}
#---------------------------------------------------------------------------------------#
move_sort_tables()
{
  TABLE=$1
  SQL="CREATE TABLE $DESTIN.$TABLE SELECT * FROM $SOURCE.$TABLE ORDER BY period DESC;"; echo "$SQL"
  RES=`mysql --login-path=local -NB -e  "$SQL"`; echo "$RES"
}
#---------------------------------------------------------------------------------------#
select_distinct_row()
{
  TABLE=$1
  SQL="INSERT INTO $FINAL1.$TABLE
          SELECT DISTINCT (sha256url)
		      ,md5root
		      ,url
		      ,root
		      ,tags
		      ,title
		      ,body
		      ,alexa
		      ,rank
		      ,hit1
		      ,hit2
		      ,hit3
		      ,category
		      ,period
		      ,gunning
		      ,flesch
		      ,kincaid
		      ,sentence
		      ,words
		      ,syllables
		      ,complex
 	        FROM $DESTIN.$TABLE"

  echo "$SQL"; RES=`mysql --login-path=local -NB -e  "$SQL"`; echo "$RES"
}
#---------------------------------------------------------------------------------------#
#                                Build Shard 
#---------------------------------------------------------------------------------------#
create_schema

  list=`echo {{0..9},{a..f}}`
  for one in $list; do
    for two in $list; do
      for three in $list; do

        shard="$one$two$three"
        let "hex = 0x$shard"
        let "start = 0x$START"
        if [ $hex -ge $start ]; then                                                                                                                                                                     
          echo $hex " " $shard
          echo $shard > last.log
          
          # drop_table
          move_sort_tables "part_$shard"

          # truncate_table
          select_distinct_row "part_$shard"
        fi  

      done
    done
  done

#---------------------------------------------------------------------------------------#

