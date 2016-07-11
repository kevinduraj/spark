#!/bin/sh
#-----------------------------------------------------------------------------------------#
USERNAME='root'
DB='engine37'
file_source='health.dat';
total=""
#-----------------------------------------------------------------------------------------#
exec_sql()
{
  SQL=$1
  RES=`mysql -u $USERNAME -p$PASSWORD -NB -e "$SQL"`
}
#-----------------------------------------------------------------------------------------#
# https://dev.mysql.com/doc/refman/5.5/en/fulltext-natural-language.html
# IN NATURAL LANGUAGE MODE
# IN BOOLEAN MODE
#-----------------------------------------------------------------------------------------#
execute_build_sql()
{
  TABLE=$DB'.part_'$1
 
  #BOOST=$(( ( RANDOM % 5 ) + 3 ))
  SQL="SELECT count(*) FROM $TABLE WHERE MATCH (title,body) AGAINST('"$line"' IN BOOLEAN MODE);"
  total=$(echo "$SQL" | mysql -u $USERNAME -p$PASSWORD -N) 
 
  # Compute hit1 rank only if result is not 0 
  if [[ "$total" != 0 ]] ; then 
    printf '%5s' $total' '
    total2=$(echo "10-l($total)" | bc -l)
    int=${total2%.*}

    if [ "$int" ];then 
      SQL="UPDATE $TABLE SET hit1=hit1+$int WHERE MATCH (title,body) AGAINST('"$line"' IN BOOLEAN MODE);"
      echo "$SQL"; exec_sql "$SQL"
    else 
      SQL="IGNORE $TABLE SET hit1=hit1+$int WHERE MATCH (title,body) AGAINST('"$line"' IN BOOLEAN MODE);"
      echo "$SQL"; 
    fi 
  fi

}
#----------------------------------------------------------------------------------------#
ordered_partitions()
{

    list=`echo {{0..9},{a..f}}`
    for one in $list; do
      for two in $list; do
        for three in $list; do


          while IFS='' read -r line || [[ -n "$line" ]]; do
          execute_build_sql $one$two$three
          done < $file_source 


        done
      done
    done
  
}
#----------------------------------------------------------------------------------------#
random_partitions()
{
    while IFS='' read -r part1 || [[ -n "$part1" ]]; do
    
    
        while IFS='' read -r line || [[ -n "$line" ]]; do
        execute_build_sql $part1
        done < $file_source 
    
    
    done < "parts.dat"
}
#----------------------------------------------------------------------------------------#

echo; echo; echo; 
random_partitions

#----------------------------------------------------------------------------------------#

