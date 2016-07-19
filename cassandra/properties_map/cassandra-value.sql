CREATE KEYSPACE kevin WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
USE kevin;
CREATE TABLE data_points (
            id text PRIMARY KEY,
            created_at timestamp,
            previous_event_id varchar,
            properties map<text,text>
         );

CREATE index on data_points (properties);
INSERT INTO data_points (id, properties) VALUES ('1', { 'fruit' : 'apple',  'band' : 'Beatles' });
INSERT INTO data_points (id, properties) VALUES ('2', { 'fruit' : 'cherry', 'band' : 'Beatles' });
INSERT INTO data_points (id, properties) VALUES ('3', { 'key1'  : 'value1', 'key2' : 'values2' });
SELECT * FROM data_points WHERE properties CONTAINS 'Beatles';


