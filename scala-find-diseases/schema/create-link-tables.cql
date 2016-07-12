-- ----------------------------------------------------------- --
CREATE TABLE IF NOT EXISTS
cloud4.diseases (
    url text,
    PRIMARY KEY( url )
)
WITH comment='diseases links'
 AND compression = { 'sstable_compression' : '' } 
 AND compaction  = { 'class' :  'LeveledCompactionStrategy' }; 
-- ----------------------------------------------------------- --
DESCRIBE KEYSPACE cloud4;
