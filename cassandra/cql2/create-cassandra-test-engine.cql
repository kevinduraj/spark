DROP KEYSPACE IF EXISTS test;
CREATE KEYSPACE test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
DESCRIBE KEYSPACES;
-- ----------------------------------------------------------- --
CREATE TABLE IF NOT EXISTS
test.vdomain (
  domain text,
  total counter,
  PRIMARY KEY (domain)
)
WITH comment     = 'visited total domain'
 AND compression = { 'sstable_compression' : '' } 
 AND compaction  = { 'class' :  'LeveledCompactionStrategy' }; 
-- ----------------------------------------------------------- --
CREATE TABLE IF NOT EXISTS
test.ldomain (
  domain text,
  total counter,
  PRIMARY KEY (domain)
)
WITH comment     = 'links total domain'
 AND compression = { 'sstable_compression' : '' } 
 AND compaction  = { 'class' :  'LeveledCompactionStrategy' }; 
-- ----------------------------------------------------------- --
CREATE TABLE IF NOT EXISTS
test.visit (
    url text,
    PRIMARY KEY( url )
)
WITH comment='visited urls'
 AND compression = { 'sstable_compression' : '' } 
 AND compaction  = { 'class' :  'LeveledCompactionStrategy' }; 
-- ----------------------------------------------------------- --
CREATE TABLE IF NOT EXISTS
test.link (
    url text,
    PRIMARY KEY( url )
)
WITH comment='web page links'
 AND compression = { 'sstable_compression' : '' } 
 AND compaction  = { 'class' :  'LeveledCompactionStrategy' }; 
-- ----------------------------------------------------------- --
DESCRIBE KEYSPACE test;

