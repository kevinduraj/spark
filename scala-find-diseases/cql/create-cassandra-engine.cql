DROP KEYSPACE IF EXISTS engine;
CREATE KEYSPACE engine WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };
DESCRIBE KEYSPACES;

-- ----------------------------------- --
CREATE TABLE IF NOT EXISTS
engine.domain (
  root text,
  count counter,
  PRIMARY KEY (root)
);

-- ----------------------------------- --
CREATE TABLE IF NOT EXISTS
engine.links (
    domain text,
    url text,
    PRIMARY KEY( domain, url )
) WITH CLUSTERING ORDER BY (url ASC);

-- ----------------------------------- --
CREATE TABLE IF NOT EXISTS 
engine.visited (
    url text PRIMARY KEY
);
-- ----------------------------------- --
