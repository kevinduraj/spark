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
CREATE TABLE IF NOT EXISTS
cloud4.lflickr (
    url text,
    PRIMARY KEY( url )
)
WITH comment='Social Links'
 AND compression = { 'sstable_compression' : '' } 
 AND compaction  = { 'class' :  'LeveledCompactionStrategy' }; 
-- ----------------------------------------------------------- --
DESCRIBE KEYSPACE cloud4;
