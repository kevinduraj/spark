-- DROP KEYSPACE IF EXISTS disease;
CREATE KEYSPACE IF NOT EXISTS disease 
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
DESCRIBE KEYSPACES;

USE disease;

-- ---------------------------------------------------------- --
CREATE TABLE IF NOT EXISTS general  ( 
          condition text,
          url text,
          PRIMARY KEY (condition, url)
);
-- ---------------------------------------------------------- --
CREATE TABLE IF NOT EXISTS temp ( 
          condition text,
          url text,
          PRIMARY KEY (condition, url)
);
-- ---------------------------------------------------------- --

DESCRIBE KEYSPACE disease;
