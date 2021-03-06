package de.brockhaus.data.timeseries.dao;

import org.apache.log4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

/**
 * Inspired by the DAO Pattern ... 
 * Later on we might have a MongoDBDataSource.
 * 
 * @see spring-beans.xml
 * 
 * Project: cassandra.sensordata
 *
 * Copyright (c) by Brockhaus Group
 * www.brockhaus-gruppe.de
 * @author mbohnen, Mar 27, 2015
 *
 */
public class CassandraDataSource {
	
	// just a logger
	private static final Logger LOG = Logger.getLogger(CassandraDataSource.class);
	// are we connected?
	private boolean connected;
	
	// where to connect to?
	private String hostIP = "127.0.0.1";
	private Cluster cluster;
	private Session session;
	
	//similar to database
	private String keyspace = "test";
	
	
	public CassandraDataSource() {
		//lazy
	}
	

	/**
	 * Connects to a given IP
	 * @param node
	 */
	public Session connect(String node, String keyspace) {
		this.keyspace = keyspace;
		cluster = Cluster.builder().addContactPoint(node).build();
		session = cluster.connect(keyspace);
		
		Metadata metadata = cluster.getMetadata();
		LOG.debug(System.out.printf("Connected to cluster: %s\n ", metadata.getClusterName()));
		
		for (Host host : metadata.getAllHosts()) {
			LOG.debug(System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
					host.getDatacenter(), host.getAddress(), host.getRack()));			
		}
		
		this.connected = true;
		
		return this.session;
	}
	
	public Session connect() {
		return this.connect(this.hostIP, this.keyspace);
	}
	
	public void close() {
		cluster.close();
	}

	
	
	// all getters and setters as Spring wants this
	public String getHostIP() {
		return hostIP;
	}

	public void setHostIP(String hostIP) {
		this.hostIP = hostIP;
	}

	public String getKeyspace() {
		return keyspace;
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}
}