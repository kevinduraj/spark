DROP KEYSPACE IF EXISTS cloud1;
CREATE KEYSPACE cloud1 WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
DESCRIBE KEYSPACES;
-- ----------------------------------------------------------- --
CREATE TABLE IF NOT EXISTS
cloud1.temp (
    url text,
    PRIMARY KEY( url )
)
WITH comment='temp table'
 AND compression = { 'sstable_compression' : '' } 
 AND compaction  = { 'class' :  'LeveledCompactionStrategy' }; 
-- ----------------------------------------------------------- --
CREATE TABLE IF NOT EXISTS
cloud1.diseases (
    url text,
    condition text,
    PRIMARY KEY( url )
)
WITH comment='web page links'
 AND compression = { 'sstable_compression' : '' } 
 AND compaction  = { 'class' :  'LeveledCompactionStrategy' }; 
-- ----------------------------------------------------------- --
DESCRIBE KEYSPACE cloud1;
