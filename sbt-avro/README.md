Spark Avro
==========

###Output
```
root
 |-- title: string (nullable = false)
 |-- air_date: string (nullable = false)
 |-- doctor: integer (nullable = false)

+--------------------+----------------+------+
|               title|        air_date|doctor|
+--------------------+----------------+------+
|   The Eleventh Hour|    3 April 2010|    11|
|   The Doctor's Wife|     14 May 2011|    11|
| Horror of Fang Rock|3 September 1977|     4|
|  An Unearthly Child|23 November 1963|     1|
|The Mysterious Pl...|6 September 1986|     6|
|                Rose|   26 March 2005|     9|
|The Power of the ...| 5 November 1966|     2|
|          Castrolava|  4 January 1982|     5|
+--------------------+----------------+------+

------- Filter By Doctor > 5 -----
+--------------------+----------------+------+
|               title|        air_date|doctor|
+--------------------+----------------+------+
|   The Eleventh Hour|    3 April 2010|    11|
|   The Doctor's Wife|     14 May 2011|    11|
|The Mysterious Pl...|6 September 1986|     6|
|                Rose|   26 March 2005|     9|
+--------------------+----------------+------+

--------- GroupBy Doctor ---------
+------+-----+
|doctor|count|
+------+-----+
|     1|    1|
|     2|    1|
|     4|    1|
|     5|    1|
|     6|    1|
|     9|    1|
|    11|    2|
+------+-----+


-------------- doctor > 10 --------------------
+-----------------+------+
|            title|doctor|
+-----------------+------+
|The Eleventh Hour|    11|
|The Doctor's Wife|    11|
+-----------------+------+

[The Eleventh Hour,3 April 2010,11]
[The Doctor's Wife,14 May 2011,11]
```


###Avro Dependancies
```
spark-submit --packages com.databricks:spark-avro_2.10:2.0.1
```
###References:
* [https://github.com/databricks/spark-avro](https://github.com/databricks/spark-avro)
