#!/bin/bash                                                                                                                                                                            
#----------------------------------------------------------#
clear;

if [ "$#" -eq 3 ]; then

  SQL="SELECT DISTINCT $3, COUNT($3) AS total FROM $1.part_$2 GROUP BY $3 ORDER BY $3;"; 
  echo "$SQL"
  RES=`nice mysql -uroot -p$PASSWORD -e  "$SQL"`
  #RES=`mysql -u root -p$PASSWORD -NB -e  "$SQL"`
  echo "$RES"

elif [ "$#" -eq 2 ]; then 

  res1=$(openssl rand -hex 16)
  part=$(echo ${res1:7:3})
  SQL="SELECT DISTINCT $2, COUNT($2) AS total FROM $1.part_$part GROUP BY $2 ORDER BY $2 limit 30;"; 
  echo "$SQL"
  RES=`nice mysql -uroot -p$PASSWORD -e  "$SQL"`
  echo "$RES"

else 
  echo "---------------------------------------------"
  echo "     1=engine, 2=table, 3=field              "
  echo "---------------------------------------------"
  echo "./distribution.sh engine77 rank              "
  echo "---------------------------------------------"
  echo "./distribution.sh engine77 195 hit1          "
  echo "./distribution.sh engine77 2a6 hit3          "
  echo "./distribution.sh engine77 3b7 rank          "
  echo "./distribution.sh engine77 5f9 root          "
  echo "./distribution.sh engine77 4c8 sentence      "
  echo "./distribution.sh engine77 5f9 complex       "
  echo "---------------------------------------------"

fi

# +-----------+----------------------+------+-----+----------+-------+
# | Field     | Type                 | Null | Key | Default  | Extra |
# +-----------+----------------------+------+-----+----------+-------+
# | sha256url | char(64)             | NO   |     | NULL     |       |
# | md5root   | char(32)             | NO   |     | NULL     |       |
# | url       | varchar(255)         | NO   |     | NULL     |       |
# | root      | varchar(64)          | NO   |     | NULL     |       |
# | tags      | varchar(128)         | YES  |     | NULL     |       |
# | title     | varchar(128)         | YES  |     | NULL     |       |
# | body      | varchar(4096)        | YES  |     | NULL     |       |
# | alexa     | mediumint(6)         | YES  |     | NULL     |       |
# | rank      | mediumint(6)         | YES  |     | NULL     |       |
# | hit1      | mediumint(6)         | YES  |     | NULL     |       |
# | hit2      | mediumint(6)         | YES  |     | NULL     |       |
# | hit3      | mediumint(6)         | YES  |     | NULL     |       |
# | category  | char(3)              | YES  |     | NULL     |       |
# | period    | date                 | YES  |     | NULL     |       |
# | gunning   | float(5,2)           | NO   |     | 0.00     |       |
# | flesch    | float(5,2)           | NO   |     | 0.00     |       |
# | kincaid   | float(5,2)           | NO   |     | 0.00     |       |
# | sentence  | smallint(5) unsigned | NO   |     | 0        |       |
# | words     | float(5,2) unsigned  | NO   |     | 0.00     |       |
# | syllables | float(7,6) unsigned  | NO   |     | 0.000000 |       |
# | complex   | float(4,2) unsigned  | NO   |     | 0.00     |       |
# +-----------+----------------------+------+-----+----------+-------+

