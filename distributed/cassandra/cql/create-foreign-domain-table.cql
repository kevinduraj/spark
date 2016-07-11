-- ----------------------------------------------------------- --
CREATE TABLE IF NOT EXISTS
cloud4.foreign_domain (
    domain text,
    language text,
    PRIMARY KEY( domain )
)
WITH comment='foreign domain language'
 AND compression = { 'sstable_compression' : '' } 
 AND compaction  = { 'class' :  'LeveledCompactionStrategy' }; 
-- ----------------------------------------------------------- --
DESCRIBE KEYSPACE cloud4;
