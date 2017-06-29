-- DROP KEYSPACE IF EXISTS test; 

CREATE KEYSPACE test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

CREATE table test.left_table(
  part text PRIMARY KEY,
  author text,
);

CREATE table test.right_table( 
  part text,
  clust text,
  book text,
  PRIMARY KEY (part, clust)
);

CREATE TABLE test.big_table (
  part text,
  clust text,
  author text,
  book text,
  PRIMARY KEY (part, clust)
);

