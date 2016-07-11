-- ----------------------------------------------------------- --
CREATE TABLE IF NOT EXISTS
engine2.vdomain (
  domain text,
  total counter,
  PRIMARY KEY (domain)
)
WITH comment     = 'visited total domain'
 AND compression = { 'sstable_compression' : '' } 
 AND compaction  = { 'class' :  'LeveledCompactionStrategy' }; 
-- ----------------------------------------------------------- --
CREATE TABLE IF NOT EXISTS
engine2.ldomain (
  domain text,
  total counter,
  PRIMARY KEY (domain)
)
WITH comment     = 'links total domain'
 AND compression = { 'sstable_compression' : '' } 
 AND compaction  = { 'class' :  'LeveledCompactionStrategy' }; 
-- ----------------------------------------------------------- --
