-- ----------------------------------------------------------- --
CREATE TABLE IF NOT EXISTS
cloud4.link5 (
    url text,
    PRIMARY KEY( url )
)
WITH comment='links'
 AND compression = { 'sstable_compression' : '' } 
 AND compaction  = { 'class' :  'LeveledCompactionStrategy' }; 
-- ----------------------------------------------------------- --
DESCRIBE KEYSPACE cloud4;
