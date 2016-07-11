DROP KEYSPACE IF EXISTS test_join; 

CREATE KEYSPACE test_join WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

CREATE table test_join.left_table(
  part text PRIMARY KEY,
  author text,
);

CREATE table test_join.right_table( 
  part text,
  clust text,
  book text,
  PRIMARY KEY (part, clust)
);

CREATE TABLE test_join.big_table (
  part text,
  clust text,
  author text,
  book text,
  PRIMARY KEY (part, clust)
);

