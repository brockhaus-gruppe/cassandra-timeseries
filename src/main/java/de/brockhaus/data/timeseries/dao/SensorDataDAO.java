package de.brockhaus.data.timeseries.dao;

import java.util.Date;
import java.util.List;

import de.brockhaus.data.timeseries.util.SensorDataTO;
import de.brockhaus.data.timeseries.util.TimeRange;

/**
 * Just the interface ...
 * Maybe we have other data stores one day, maybe we start thinking of using a GenericDAO one day ...
 * 
 * Project: cassandra.timeseries
 *
 * Copyright (c) by Brockhaus Group
 * www.brockhaus-gruppe.de
 * @author mbohnen, Aug 6, 2015
 *
 */
public interface SensorDataDAO {
	
	/**
	 * the one size fits all method
	 * @param cql
	 */
	void executeStatement(String cql);

	/**
	 * Inserting sensor data presuming we want to have the default ttl and default row granularity 
	 * for this sensor
	 * 
	 * @param data the data to insert
	 */
	void insertSensorData(SensorDataTO data);
	
	/**
	 * Inserting data presuming a certain time range which determines the 'sizing'
	 * rows in cassandra
	 * 
	 * @param data the data to insert
	 * @param timeRange the 'row granularity'
	 * @param ttl the time to live of this record
	 */
	void insertSensorData(SensorDataTO data, TimeRange timeRange, int ttl);
	
	/**
	 * 
	 * @param list
	 */
	void bulkInsertOfSensorData(List<SensorDataTO> list);
	
	/**
	 * 
	 */
	long getNumberOfRecords();
	
	/**
	 * 
	 * @param id
	 * @return
	 */
	List<SensorDataTO> findBySensorId(String id);
	
	/**
	 * 
	 * @param sensorId
	 * @param from
	 * @param to
	 * @return
	 */
	List<SensorDataTO> findBySensorIDAndTimeInterval(String sensorId, Date from, Date to);
	
	List<SensorDataTO> findByTimeInterval(Date from, Date to);
}
