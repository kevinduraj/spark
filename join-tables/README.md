Spark Submit Join Tables
========================

1. cqlsh -f create-schema.cql

2. cqlsh -f insert-data.cql

3. cqlsh -f select-tables.cql

4. dse spark -i:joining-tables.scala 

5. cqlsh -f select-tables.cql

###Spark Context RDD Result
* dse spark -i:joining-tables.scala

```
 part | author
------+-------------------
 1002 | Bjarne Stroustrup
 1005 |     James Gosling
 1004 |        Larry Wall
 1003 |    Martin Odersky
 1006 |      Doug Cutting
 1000 |     Matei Zaharia
 1001 |    Linus Torvalds

(7 rows)

 part | clust | book
------+-------+---------------------------------
 1002 |    30 |                   A Tour of C++
 1002 |    31 |               Principles of C++
 1002 |    32 |               The Design of C++
 1005 |    60 |               The Java Language
 1005 |    61 |              Java Specification
 1004 |    50 |                Programming Perl
 1003 |    40 |             Programmin in Scala
 1006 |    70 |                Lucene in Action
 1006 |    71 |                     Hadoop Book
 1000 |    10 |                  Learning Spark
 1000 |    11 |                   Data Analysis
 1000 |    12 | Parallel Programming With Spark
 1001 |    20 |                    Linux Kernel
 1001 |    21 |               Linux Open Source

(14 rows)

 part | clust | author            | book
------+-------+-------------------+---------------------------------
 1002 |    30 | Bjarne Stroustrup |                   A Tour of C++
 1002 |    31 | Bjarne Stroustrup |               Principles of C++
 1002 |    32 | Bjarne Stroustrup |               The Design of C++
 1005 |    60 |     James Gosling |               The Java Language
 1005 |    61 |     James Gosling |              Java Specification
 1004 |    50 |        Larry Wall |                Programming Perl
 1003 |    40 |    Martin Odersky |             Programmin in Scala
 1006 |    70 |      Doug Cutting |                Lucene in Action
 1006 |    71 |      Doug Cutting |                     Hadoop Book
 1000 |    10 |     Matei Zaharia |                  Learning Spark
 1000 |    11 |     Matei Zaharia |                   Data Analysis
 1000 |    12 |     Matei Zaharia | Parallel Programming With Spark
 1001 |    20 |    Linus Torvalds |                    Linux Kernel
 1001 |    21 |    Linus Torvalds |               Linux Open Source

(14 rows)
```

###Cassandra SQL Context Result
* dse spark -i:joining-csc.scala

```
[1000,Matei Zaharia,Learning Spark]
[1000,Matei Zaharia,Data Analysis]
[1000,Matei Zaharia,Parallel Programming With Spark]
[1001,Linus Torvalds,Linux Kernel]
[1001,Linus Torvalds,Linux Open Source]
[1002,Bjarne Stroustrup,A Tour of C++]
[1002,Bjarne Stroustrup,Principles of C++]
[1002,Bjarne Stroustrup,The Design of C++]
[1003,Martin Odersky,Programmin in Scala]
[1004,Larry Wall,Programming Perl]
[1005,James Gosling,The Java Language]
[1005,James Gosling,Java Specification]
[1006,Doug Cutting,Lucene in Action]
[1006,Doug Cutting,Hadoop Book]
```

