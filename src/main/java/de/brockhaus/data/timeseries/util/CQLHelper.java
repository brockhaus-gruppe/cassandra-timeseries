package de.brockhaus.data.timeseries.util;

/**
 * Some handy CQL statements
 * 
 * Project: cassandra.timeseries
 *
 * Copyright (c) by Brockhaus Group
 * www.brockhaus-gruppe.de
 * @author mbohnen, Aug 16, 2015
 *
 */
public class CQLHelper {
	
	public static final String INSERT= "INSERT INTO sensor_data(SENSOR_ID, DATATYPE, TIME, TIME_RANGE, TIME_SCALE, VALUE) VALUES(?, ?, ?, ?, ?, ?);";
	public static final String INSERT_WITH_TTL = "INSERT INTO sensor_data(SENSOR_ID, DATATYPE, TIME, TIME_RANGE, TIME_SCALE, VALUE) VALUES(?, ?, ?, ?, ?, ?) USING TTL %d;";

	public static final String DELETE_ALL = "TRUNCATE sensor_data;";
	
	public static final String GET_NO_OF_RECORDS = "SELECT COUNT(*) FROM sensor_data";
	
	public static final String FIND_BY_SENSORID = "SELECT * FROM sensor_data WHERE SENSOR_ID = ? ";
	
	public static final String FIND_BY_SENSORID_AND_TIMEINTERVALL = "SELECT * FROM sensor_data WHERE SENSOR_ID = ? AND TIME >= ? AND TIME <= ? ALLOW FILTERING";
}
