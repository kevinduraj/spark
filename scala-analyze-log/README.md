Analyze Custom HTTP Log
=======================


###access_log
```
200|19812|173.192.92.174|ae.5c.c0ad.ip4.static.sl-reverse.com|GET / HTTP/1.0|myhealthcare.com|http://myhealthcare.com/
200|19812|66.115.186.120|jmedia31.nationalnet.com|GET / HTTP/1.0|myhealthcare.com|http://myhealthcare.com/
200|18859|75.126.50.198|dweb01.dal.fe.vscopehost.com|GET / HTTP/1.0|find1friend.com|http://find1friend.com/
200|18859|67.228.206.52|34.ce.e443.ip4.static.sl-reverse.com|GET / HTTP/1.0|find1friend.com|http://find1friend.com/
200|19812|85.17.175.239|dc001.ams01.traiddns.net|GET / HTTP/1.0|myhealthcare.com|http://myhealthcare.com/
200|19812|75.126.50.206|ce.32.7e4b.ip4.static.sl-reverse.com|GET / HTTP/1.0|myhealthcare.com|http://myhealthcare.com/
200|19812|85.13.145.154|dd26632.kasserver.com|GET / HTTP/1.0|myhealthcare.com|http://myhealthcare.com/
200|19812|75.126.50.200|dweb06.dal.fe.vscopehost.com|GET / HTTP/1.0|myhealthcare.com|http://myhealthcare.com/
200|19812|108.168.211.92|5c.d3.a86c.ip4.static.sl-reverse.com|GET / HTTP/1.0|myhealthcare.com|http://myhealthcare.com/
200|19812|64.237.33.131|www.aoaforums.com|GET / HTTP/1.0|myhealthcare.com|http://myhealthcare.com/
200|19812|75.126.50.200|dweb06.dal.fe.vscopehost.com|GET / HTTP/1.0|myhealthcare.com|http://myhealthcare.com/
200|19812|75.126.50.203|cb.32.7e4b.ip4.static.sl-reverse.com|GET / HTTP/1.0|myhealthcare.com|http://myhealthcare.com/
200|18859|75.126.50.197|c5.32.7e4b.ip4.static.sl-reverse.com|GET / HTTP/1.0|find1friend.com|http://find1friend.com/
200|18859|69.65.43.214|server2.extremedns.com|GET / HTTP/1.0|find1friend.com|http://find1friend.com/
```

###Output
```
logDF: org.apache.spark.sql.DataFrame = [http: string, size: int, ip: string, host: string, url: string, ref: string]
query1: String = SELECT ip, COUNT(*) AS total FROM log_table GROUP BY ip ORDER BY total DESC LIMIT 7
+--------------+-----+                                                          
|            ip|total|
+--------------+-----+
|66.115.186.120|  116|
| 75.126.50.200|   91|
|  202.191.98.9|   66|
|108.168.211.92|   52|
|  52.3.127.144|   48|
|  88.250.46.38|   48|
|173.193.180.35|   44|
+--------------+-----+

query2: String = SELECT ip, SUM(size) AS bigsize FROM log_table GROUP BY ip ORDER BY bigsize DESC LIMIT 7
+--------------+-------+                                                        
|            ip|bigsize|
+--------------+-------+
|66.115.186.120|2228969|
| 75.126.50.200|1755485|
|  202.191.98.9|1279314|
|108.168.211.92|1003766|
|173.193.180.35| 847656|
|   67.43.1.132| 815421|
|173.193.180.43| 791537|
+--------------+-------+

query2: String = SELECT ref, COUNT(*) AS total FROM log_table GROUP BY ref ORDER BY total DESC LIMIT 12
+--------------------+-----+
|                 ref|total|
+--------------------+-----+
|    myhealthcare.com| 1239|
|     find1friend.com|  981|
|      pacificair.com|  196|
|        play-mp3.com|  188|
|www.myhealthcare.com|   25|
|           nitra.net|   21|
|    pacificstore.com|   20|
|www.losangelespeo...|   18|
|      protonchat.com|   15|
|www.pacificjustic...|   14|
|                   -|   13|
|  www.pacificair.com|   11|
+--------------------+-----+

botDF: org.apache.spark.sql.DataFrame = [http: string, size: int, ip: string, host: string, url: string, ref: string]
query3: String = SELECT host, COUNT(*) AS total FROM bot_table GROUP BY host ORDER BY total DESC LIMIT 7
+--------------------+-----+                                                    
|                host|total|
+--------------------+-----+
|msnbot-157-55-39-...|    5|
|msnbot-40-77-167-...|    4|
|msnbot-157-55-39-...|    4|
|msnbot-207-46-13-...|    3|
|msnbot-157-55-39-...|    3|
|msnbot-207-46-13-...|    3|
|msnbot-157-55-39-...|    2|
+--------------------+-----+
```


