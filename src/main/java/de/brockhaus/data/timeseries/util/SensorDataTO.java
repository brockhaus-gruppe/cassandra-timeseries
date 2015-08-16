package de.brockhaus.data.timeseries.util;

import java.io.Serializable;
import java.util.Date;

/**
 * A transfer object related to sensor data
 * 
 * Project: cassandra.timeseries
 *
 * Copyright (c) by Brockhaus Group
 * www.brockhaus-gruppe.de
 * @author mbohnen, Aug 6, 2015
 *
 */
public class SensorDataTO implements Serializable {

	private String sensorid;
	private Date time;
	private SensorDataType datatype;
	private String value;
	
	public SensorDataTO() {
		
	}

	public SensorDataTO(String sensorid, Date time, SensorDataType datatype, String value) {
		super();
		this.sensorid = sensorid;
		this.time = time;
		this.datatype = datatype;
		this.value = value;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getSensorid() {
		return sensorid;
	}

	public void setSensorid(String sensorid) {
		this.sensorid = sensorid;
	}

	public Date getTime() {
		return time;
	}

	public void setTime(Date time) {
		this.time = time;
	}

	public SensorDataType getDatatype() {
		return datatype;
	}

	public void setDatatype(SensorDataType datatype) {
		this.datatype = datatype;
	}
	
	//TODO implement hasCode, equals and toString
}
