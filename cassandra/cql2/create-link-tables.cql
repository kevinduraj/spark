-- ----------------------------------------------------------- --
CREATE TABLE IF NOT EXISTS
cloud4.link_clean (
    url text,
    PRIMARY KEY( url )
)
WITH comment='best crawl links'
 AND compression = { 'sstable_compression' : '' } 
 AND compaction  = { 'class' :  'LeveledCompactionStrategy' }; 
-- ----------------------------------------------------------- --
DESCRIBE KEYSPACE cloud4;
