-- having this we will have two RowKeys, compare to the DDL:
-- CREATE TABLE sensor_data (
-- 	sensor_id text, 
-- 	datatype text, 
-- 	time timestamp, 
-- 	time_range timestamp, 
-- 	time_scale text, 
-- 	value text, 
-- 	PRIMARY KEY((sensor_id, time_range), time)
-- );
-- You can check this by using cassandra-cli tool
INSERT INTO sensor_data(SENSOR_ID, DATATYPE, TIME, TIME_RANGE, TIME_SCALE, VALUE) VALUES('ABC-123', 'FLOAT', '2013-04-03 07:01:00', '2013-04-03 07:00:00', 'HOUR','100') USING TTL 200;
INSERT INTO sensor_data(SENSOR_ID, DATATYPE, TIME, TIME_RANGE, TIME_SCALE, VALUE) VALUES('ABC-123', 'FLOAT', '2013-04-03 07:02:00', '2013-04-03 07:00:00', 'HOUR', '100') USING TTL 200;
INSERT INTO sensor_data(SENSOR_ID, DATATYPE, TIME, TIME_RANGE, TIME_SCALE, VALUE) VALUES('ABC-123', 'FLOAT', '2013-04-03 07:03:00', '2013-04-03 07:00:00', 'HOUR', '100') USING TTL 200;
INSERT INTO sensor_data(SENSOR_ID, DATATYPE, TIME, TIME_RANGE, TIME_SCALE, VALUE) VALUES('ABC-123', 'FLOAT', '2013-04-03 07:04:00', '2013-04-03 07:00:00', 'HOUR', '100') USING TTL 200;
INSERT INTO sensor_data(SENSOR_ID, DATATYPE, TIME, TIME_RANGE, TIME_SCALE, VALUE) VALUES('ABC-123', 'FLOAT', '2013-04-03 07:04:00', '2013-04-03 07:00:00', 'HOUR', '101') USING TTL 200;
INSERT INTO sensor_data(SENSOR_ID, DATATYPE, TIME, TIME_RANGE, TIME_SCALE, VALUE) VALUES('ABC-123', 'FLOAT', '2013-04-03 08:01:00', '2013-04-03 08:00:00', 'HOUR', '100') USING TTL 200;