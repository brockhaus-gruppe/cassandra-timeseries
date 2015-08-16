package de.brockhaus.data.timeseries.dao;

import java.util.Date;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import de.brockhaus.data.timeseries.util.CQLHelper;
import de.brockhaus.data.timeseries.util.SensorDataTO;
import de.brockhaus.data.timeseries.util.SensorDataType;



public class SensorDataCassandraDAOTest {
	
	private static SensorDataCassandraDAO dao;
	
	public static void main(String[] args) {
		SensorDataCassandraDAOTest test = new SensorDataCassandraDAOTest();
		test.init();
		
		test.testInsertSensorData();
		
		test.tearDown();
	}
	
	@BeforeClass
	public static void init() {
		ApplicationContext context= new ClassPathXmlApplicationContext("spring-beans.xml");
		dao = (SensorDataCassandraDAO) context.getBean("cassandra_dao");
		
		SensorDataCassandraDAOTest.createDataSet();
	}
	
	private static void createDataSet() {
		dao.executeStatement("INSERT INTO sensor_data(SENSOR_ID, DATATYPE, TIME, TIME_RANGE, TIME_SCALE, VALUE) VALUES('ABC-123', 'FLOAT', '2013-04-03 07:01:00', '2013-04-03 07:00:00', 'HOUR','100');");
		dao.executeStatement("INSERT INTO sensor_data(SENSOR_ID, DATATYPE, TIME, TIME_RANGE, TIME_SCALE, VALUE) VALUES('ABC-123', 'FLOAT', '2013-04-03 07:02:00', '2013-04-03 07:00:00', 'HOUR','100');");
		dao.executeStatement("INSERT INTO sensor_data(SENSOR_ID, DATATYPE, TIME, TIME_RANGE, TIME_SCALE, VALUE) VALUES('ABC-123', 'FLOAT', '2013-04-03 07:03:00', '2013-04-03 07:00:00', 'HOUR','100');");
		dao.executeStatement("INSERT INTO sensor_data(SENSOR_ID, DATATYPE, TIME, TIME_RANGE, TIME_SCALE, VALUE) VALUES('ABC-123', 'FLOAT', '2013-04-03 07:04:00', '2013-04-03 07:00:00', 'HOUR','100');");
		dao.executeStatement("INSERT INTO sensor_data(SENSOR_ID, DATATYPE, TIME, TIME_RANGE, TIME_SCALE, VALUE) VALUES('ABC-123', 'FLOAT', '2013-04-03 07:05:00', '2013-04-03 07:00:00', 'HOUR','100');");
		dao.executeStatement("INSERT INTO sensor_data(SENSOR_ID, DATATYPE, TIME, TIME_RANGE, TIME_SCALE, VALUE) VALUES('ABC-123', 'FLOAT', '2013-04-03 08:01:00', '2013-04-03 08:00:00', 'HOUR','100');");
		
		Assert.assertTrue(dao.getNumberOfRecords() == 6);
	}

	@AfterClass
	public static void tearDown() {
		dao.executeStatement(CQLHelper.DELETE_ALL);
		Assert.assertTrue(dao.getNumberOfRecords() == 0);
	}
	
	@Test
	public void testDaoExists() {
		Assert.assertNotNull(dao);
	}
	
	@Test
	public void testInsertSensorData() {
		SensorDataTO to = new SensorDataTO("XYZ-123", new Date(System.currentTimeMillis()), SensorDataType.FLOAT, "1234.56");	
		dao.insertSensorData(to);
		
		Assert.assertTrue(dao.findBySensorId("XYZ-123").size() == 1);
	}

}
