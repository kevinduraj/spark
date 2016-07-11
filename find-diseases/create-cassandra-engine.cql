DROP KEYSPACE IF EXISTS cloud4;
CREATE KEYSPACE cloud4 WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
DESCRIBE KEYSPACES;
-- ----------------------------------------------------------- --
CREATE TABLE IF NOT EXISTS
cloud4.vdomain (
  domain text,
  total counter,
  PRIMARY KEY (domain)
)
WITH comment     = 'visited total domain'
 AND compression = { 'sstable_compression' : '' } 
 AND compaction  = { 'class' :  'LeveledCompactionStrategy' }; 
-- ----------------------------------------------------------- --
CREATE TABLE IF NOT EXISTS
cloud4.ldomain (
  domain text,
  total counter,
  PRIMARY KEY (domain)
)
WITH comment     = 'links total domain'
 AND compression = { 'sstable_compression' : '' } 
 AND compaction  = { 'class' :  'LeveledCompactionStrategy' }; 
-- ----------------------------------------------------------- --
CREATE TABLE IF NOT EXISTS
cloud4.visit (
    url text,
    PRIMARY KEY( url )
)
WITH comment='visited urls'
 AND compression = { 'sstable_compression' : '' } 
 AND compaction  = { 'class' :  'LeveledCompactionStrategy' }; 
-- ----------------------------------------------------------- --
CREATE TABLE IF NOT EXISTS
cloud4.link1 (
    url text,
    PRIMARY KEY( url )
)
WITH comment='web page links'
 AND compression = { 'sstable_compression' : '' } 
 AND compaction  = { 'class' :  'LeveledCompactionStrategy' }; 
-- ----------------------------------------------------------- --
--                      SOCIAL TABLES
-- ----------------------------------------------------------- --
CREATE TABLE IF NOT EXISTS
cloud4.lfacebook (
    url text,
    PRIMARY KEY( url )
)
WITH comment='Social Links'
 AND compression = { 'sstable_compression' : '' } 
 AND compaction  = { 'class' :  'LeveledCompactionStrategy' }; 
-- ----------------------------------------------------------- --
CREATE TABLE IF NOT EXISTS
cloud4.ltwitter (
    url text,
    PRIMARY KEY( url )
)
WITH comment='Social Links'
 AND compression = { 'sstable_compression' : '' } 
 AND compaction  = { 'class' :  'LeveledCompactionStrategy' }; 
-- ----------------------------------------------------------- --
CREATE TABLE IF NOT EXISTS
cloud4.lyoutube (
    url text,
    PRIMARY KEY( url )
)
WITH comment='Social Links'
 AND compression = { 'sstable_compression' : '' } 
 AND compaction  = { 'class' :  'LeveledCompactionStrategy' }; 
-- ----------------------------------------------------------- --
CREATE TABLE IF NOT EXISTS
cloud4.linstagram (
    url text,
    PRIMARY KEY( url )
)
WITH comment='Social Links'
 AND compression = { 'sstable_compression' : '' } 
 AND compaction  = { 'class' :  'LeveledCompactionStrategy' }; 
-- ----------------------------------------------------------- --
CREATE TABLE IF NOT EXISTS
cloud4.lwordpress (
    url text,
    PRIMARY KEY( url )
)
WITH comment='Social Links'
 AND compression = { 'sstable_compression' : '' } 
 AND compaction  = { 'class' :  'LeveledCompactionStrategy' }; 
-- ----------------------------------------------------------- --
CREATE TABLE IF NOT EXISTS
cloud4.llinkedin (
    url text,
    PRIMARY KEY( url )
)
WITH comment='Social Links'
 AND compression = { 'sstable_compression' : '' } 
 AND compaction  = { 'class' :  'LeveledCompactionStrategy' }; 
-- ----------------------------------------------------------- --
DESCRIBE KEYSPACE cloud4;
