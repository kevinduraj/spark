CREATE KEYSPACE musiccsv
WITH replication = {
  'class': 'SimpleStrategy', 
  'replication_factor' : 1
};

USE musiccsv;

CREATE TABLE tracks_by_album (
  album_title TEXT,
  album_year INT,
  performer TEXT STATIC,
  album_genre TEXT STATIC,
  track_number INT,
  track_title TEXT,
  PRIMARY KEY ((album_title, album_year), track_number)
);
