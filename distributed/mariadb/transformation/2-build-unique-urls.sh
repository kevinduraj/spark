#!/bin/bash
#---------------------------------------------------------------------------------------#
SOURCE='engine37'
DESTIN='engine38'
FINAL1='engine39'
START='000'
SLP=10
#---------------------------------------------------------------------------------------#
drop_table()
{
  SQL="DROP TABLE IF EXISTS $DESTIN.part_$START"; echo $SQL
  res1=$(mysql -uroot -p$PASSWORD -e  "$SQL"); echo $res1
  sleep $SLP
}
#---------------------------------------------------------------------------------------#
create_schema()
{
  SQL="CREATE DATABASE $DESTIN DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci;"
  echo $SQL
  RES=`mysql -uroot -p$PASSWORD -NB -e "$SQL"`
  echo $RES
}
#---------------------------------------------------------------------------------------#
move_tables()
{
  TABLE=$1
  SQL="CREATE TABLE $DESTIN.$TABLE SELECT * FROM $SOURCE.$TABLE ORDER BY period DESC;"; echo "$SQL"
  RES=`mysql -uroot -p$PASSWORD -NB -e  "$SQL"`; echo "$RES"
  sleep $SLP
}
#---------------------------------------------------------------------------------------#
compute_tables()
{
  TABLE=$1
  SQL="INSERT LOW_PRIORITY INTO $FINAL1.$TABLE
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
 	        FROM $DESTIN.$TABLE
 	        GROUP BY sha256url"

  echo "$SQL"; RES=`mysql -u root -p$PASSWORD -NB -e  "$SQL"`; echo "$RES"
}
#---------------------------------------------------------------------------------------#
#                                Build Shard 
#---------------------------------------------------------------------------------------#
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
          
          #drop_table
          #move_tables "part_$shard"
          compute_tables "part_$shard"
        fi  

      done
    done
  done

#---------------------------------------------------------------------------------------#

