-- DROP KEYSPACE IF EXISTS test; 

CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

CREATE TABLE IF NOT EXISTS test.left_table(
  part text PRIMARY KEY,
  author text,
);

CREATE TABLE IF NOT EXISTS test.right_table( 
  part text,
  clust text,
  book text,
  PRIMARY KEY (part, clust)
);

CREATE TABLE IF NOT EXISTS test.big_table (
  part text,
  clust text,
  author text,
  book text,
  PRIMARY KEY (part, clust)
);

