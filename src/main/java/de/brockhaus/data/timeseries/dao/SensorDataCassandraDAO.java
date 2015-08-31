package de.brockhaus.data.timeseries.dao;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import de.brockhaus.data.timeseries.util.CQLHelper;
import de.brockhaus.data.timeseries.util.SensorDataTO;
import de.brockhaus.data.timeseries.util.SensorDataType;
import de.brockhaus.data.timeseries.util.TimeRange;

/**
 * The concrete DAO for Cassandra
 * 
 * The DDL for the table is:
 * 
 * CREATE TABLE sensor_data (
 * 		sensor_id text, 
 * 		datatype text, 
 * 		time timestamp, 		// the time the value was sent
 * 		time_range timestamp, 	// the granularity of the individual row within cassandra
 * 		time_scale text			// the granularity scale (DAY, HOUR, ...)
 * 		value text,	
 * 		PRIMARY KEY((sensor_id, time_range), time)); //partition key: (sensor_id, time_range) ; clustering key: time
 * 
 * Cassandra stores only one row for each partition key. All the data associated to that partition key is stored as 
 * columns in the datastore. All the data which is inserted against same clustering key is grouped together.
 * A pretty good explanation can be found here: 
 * http://intellidzine.blogspot.de/2014/01/cassandra-data-modelling-primary-keys.html
 * 
 * Project: cassandra.sensordata
 *
 * Copyright (c) by Brockhaus Group
 * www.brockhaus-gruppe.de
 * @author mbohnen, Mar 27, 2015
 *
 */
public class SensorDataCassandraDAO implements SensorDataDAO {
	
	private static final Logger LOG = Logger.getLogger(SensorDataCassandraDAO.class);

	/** default time bucket */
	private TimeRange range = TimeRange.DAY;
	
	/** 
	 * TTL(time to live):
	 * a negative value indicates no ttl 
	 * one year: 31556952000
	 * one month: 2629746000
	 * one week:   604800000
	 * one day:     86400000
	 * one hour:     3600000
	 * one minute:     60000
	 */
	private int ttl = -1;
	
	@Autowired
	private CassandraDataSource ds;
	
	@Override
	public void executeStatement(String cql) {
		Session session = ds.connect();
		session.execute(cql);	
	}
	
	@Override
	public void insertSensorData(SensorDataTO data) {
		this.insertSensorData(data, this.range, this.ttl);
	}

	@Override
	public void insertSensorData(SensorDataTO data, TimeRange timeRange, int ttl) {
		Session session = ds.connect();

		String cql = null;
		if(ttl > 0) {
			cql = String.format(CQLHelper.INSERT_WITH_TTL, ttl);	
		} else {
			cql = CQLHelper.INSERT;
		}
		
		LOG.debug("Inserting: " + cql);

		PreparedStatement pStat = session.prepare(cql);
		BoundStatement bStat = new BoundStatement(pStat);
		bStat.bind(
				data.getSensorid(), 
				data.getDatatype().toString(), 
				data.getTime(),
				this.calculateRange(data.getTime()),
				this.range.toString(),
				data.getValue().toString());
		session.execute(bStat);
	}

	@Override
	public void bulkInsertOfSensorData(List<SensorDataTO> list) {
		for (SensorDataTO sensorDataTO : list) {
			this.insertSensorData(sensorDataTO);
		}
	}
	
	
	@Override
	public long getNumberOfRecords() {
		Session session = ds.connect();
		
		long count = 0;
		// asynchronously
		ResultSetFuture res = session.executeAsync(CQLHelper.GET_NO_OF_RECORDS);
		for (Row row: res.getUninterruptibly()) {
			count = row.getLong("count");
		}
		
		session.close();
		return count;
	}

	@Override
	public List<SensorDataTO> findBySensorId(String id) {
		Session session = ds.connect();
		
		List<SensorDataTO> tos = new ArrayList<SensorDataTO>();
		String cql = CQLHelper.FIND_BY_SENSORID;
		
		PreparedStatement pStat = session.prepare(cql);
		BoundStatement bStat = new BoundStatement(pStat);
		bStat.bind(id);
		
		// asynchronous
		ResultSetFuture res = session.executeAsync(bStat);
		for (Row row: res.getUninterruptibly()) {
			SensorDataTO sensorDataTO = new SensorDataTO();
			sensorDataTO.setDatatype(SensorDataType.valueOf(row.getString("DATATYPE").toUpperCase()));
			sensorDataTO.setSensorid(row.getString("SENSOR_ID"));
			sensorDataTO.setTime(row.getDate("TIME"));
			sensorDataTO.setValue(row.getString("VALUE"));
			
			tos.add(sensorDataTO);
		}
		
		session.close();
		return tos;
	}

	@Override
	public List<SensorDataTO> findBySensorIDAndTimeInterval(String sensorId, Date from, Date to) {
		
		Session session = ds.connect();
		List<SensorDataTO> tos = new ArrayList<SensorDataTO>();
		
		// TODO: checkout whether necessary
		Timestamp fromTs = new Timestamp(from.getTime());
		Timestamp toTs = new Timestamp(to.getTime());
		
		String cql = CQLHelper.FIND_BY_SENSORID_AND_TIMEINTERVALL;

		PreparedStatement pStat = session.prepare(cql);
		BoundStatement bStat = new BoundStatement(pStat);
		bStat.bind(sensorId, fromTs, toTs);
		
		ResultSet res = session.execute(bStat);

		for (Row row : res) {
			SensorDataTO sensorDataTO = new SensorDataTO();
			sensorDataTO.setDatatype(SensorDataType.valueOf(row.getString("DATATYPE").toUpperCase()));
			sensorDataTO.setSensorid(row.getString("SENSOR_ID"));
			sensorDataTO.setTime(row.getDate("TIME"));
			sensorDataTO.setValue(row.getString("VALUE"));
			
			tos.add(sensorDataTO);		 
		}
		
		return tos;
	}
	
	public List<SensorDataTO> findByTimeInterval(Date from, Date to) {
		
		Session session = ds.connect();
		List<SensorDataTO> tos = new ArrayList<SensorDataTO>();
		
		// TODO: checkout if necessary
		Timestamp fromTs = new Timestamp(from.getTime());
		Timestamp toTs = new Timestamp(to.getTime());
		
		String cql = "SELECT * FROM sensor_data WHERE TIME > ? AND TIME < ?";

		PreparedStatement pStat = session.prepare(cql);
		BoundStatement bStat = new BoundStatement(pStat);
		bStat.bind(fromTs, toTs);
		
		ResultSet res = session.execute(bStat);

		for (Row row : res) {
			SensorDataTO sensorDataTO = new SensorDataTO();
			sensorDataTO.setDatatype(SensorDataType.valueOf(row.getString("DATATYPE").toUpperCase()));
			sensorDataTO.setSensorid(row.getString("SENSOR_ID"));
			sensorDataTO.setTime(row.getDate("TIME"));
			sensorDataTO.setValue(row.getString("VALUE"));
			
			tos.add(sensorDataTO);		 
		}
		
		return tos;
	}
	
	// helper method to truncate the date provided to the appropriate time bucket
	private Date calculateRange(Date date) {
		
		Instant instant = date.toInstant();
		switch(this.range) {
			case DAY: instant = instant.truncatedTo(ChronoUnit.DAYS); break;
			case HOUR: instant = instant.truncatedTo(ChronoUnit.HOURS); break;
			case MINUTE: instant = instant.truncatedTo(ChronoUnit.MINUTES); break;
			case SECOND: instant = instant.truncatedTo(ChronoUnit.SECONDS); break;
			case MILLISECOND: instant = instant.truncatedTo(ChronoUnit.MILLIS); break;
		}
		
		date = Date.from(instant);
		
		return date;
	}

	// getters and setters to keep Spring happy	
	public TimeRange getRange() {
		return range;
	}

	public void setRange(TimeRange range) {
		this.range = range;
	}

	public int getTtl() {
		return ttl;
	}

	public void setTtl(int ttl) {
		this.ttl = ttl;
	}
}
