-- ----------------------------------------------------------- --
DROP TABLE engine.vdomain;
CREATE TABLE IF NOT EXISTS
engine.vdomain (
  domain text,
  total counter,
  PRIMARY KEY (domain)
)
WITH comment     = 'visited total domain'
 AND compression = { 'sstable_compression' : '' } 
 AND compaction  = { 'class' :  'LeveledCompactionStrategy' }; 
-- ----------------------------------------------------------- --
DROP TABLE engine.ldomain;
CREATE TABLE IF NOT EXISTS
engine.ldomain (
  domain text,
  total counter,
  PRIMARY KEY (domain)
)
WITH comment     = 'links total domain'
 AND compression = { 'sstable_compression' : '' } 
 AND compaction  = { 'class' :  'LeveledCompactionStrategy' }; 
-- ----------------------------------------------------------- --
DESCRIBE KEYSPACE engine;
