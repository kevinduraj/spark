DROP KEYSPACE IF EXISTS kevin;
CREATE KEYSPACE IF NOT EXISTS kevin WITH replication = { 'class': 'SimpleStrategy', 'replication_factor' : 1 };
USE kevin;

DROP TABLE IF EXISTS search_key;
 
CREATE TABLE search_key (
            id text PRIMARY KEY,
            created_at timestamp,
            previous_event_id varchar,
            properties map<text,text>
         );

CREATE INDEX key_index ON search_key (KEYS(properties));

INSERT INTO search_key (id, properties) VALUES ('1', { 'key234' : 'val857', 'key879' : 'val685' });
INSERT INTO search_key (id, properties) VALUES ('2', { 'key766' : 'val435', 'key255' : 'val246' });
INSERT INTO search_key (id, properties) VALUES ('3', { 'key001' : 'val001', 'key222' : 'val468' });

SELECT * FROM search_key WHERE properties CONTAINS KEY 'key255';

