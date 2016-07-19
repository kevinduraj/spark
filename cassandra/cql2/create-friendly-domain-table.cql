-- ----------------------------------------------------------- --
CREATE TABLE IF NOT EXISTS
cloud4.friendly_domain (
    domain text,
    language text,
    PRIMARY KEY( domain )
)
WITH comment='friendly domain'
 AND compression = { 'sstable_compression' : '' } 
 AND compaction  = { 'class' :  'LeveledCompactionStrategy' }; 
-- ----------------------------------------------------------- --
DESCRIBE KEYSPACE cloud4;
