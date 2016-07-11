# Remove short lines
sed -i '/^.\{,16\}$/d' /var/log/httpd/access_log

spark-shell --driver-memory 8G --executor-memory 2G --executor-cores 4 -i bot.scala
