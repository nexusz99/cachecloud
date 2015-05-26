package com.ahems.test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;

import me.prettyprint.cassandra.serializers.ObjectSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHost;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;

import com.ahems.api.CacheCloudClient;
import com.ahems.api.SockIOPool;
import com.danga.MemCached.MemCachedClient;

public class demo {

	public static void main(String[] args) throws InterruptedException {
		
		init();
		PropertyConfigurator.configure("log4j.properties");

		String keyspace = "asdf";
		String columnfamily = Setting.columnfamily;
		String key = "testkey";
		int run = Setting.run;
		String[] names = new String[run];
		int[] value = new int[Setting.value_size*1024];
		
		Cluster cluster = HFactory.getCluster("Test Cluster");
		try{
		cluster.addColumnFamily(HFactory.createColumnFamilyDefinition(keyspace, columnfamily), true);
		}catch(Exception e){
			System.out.println("Already Defined Column Family : "+columnfamily);
			
		}
		
		System.out.println("+++++++++++++++++++++++++++++++");
		System.out.println("+      Cache Cloud DEMO       +");
		System.out.println("+++++++++++++++++++++++++++++++\n");
		System.out.println(">> 1. Setting "+run+"EA Data (Cassandra , CacheCloud)");
		System.out.println(">> Value Size : "+Setting.value_size+"KB");
		
		
		//Generate Test Name
		for(int i = 0; i < run; i++){
			names[i] = "testname_"+i;
		}
		
		//Generate Test Value
		for(int i = 0; i<Setting.value_size*1024;i++){
			value[i] = 1;
		}
		
		//Set Data
		CacheCloudClient ccc = new CacheCloudClient();
		for(int i = 0; i< run; i++){
			ccc.cache_insert(keyspace, columnfamily, key+i, names[i],value);
			Thread.sleep(100);
		}
		
		System.out.println(">> Finish Setting Data !");
		System.out.println("------------------------");
		System.out.println(">> 2. Single Get Cache From CacheCloud");
		
		//Cache Get
		double sum = 0;
		for(int i =0;i<run;i++){
			long start = System.currentTimeMillis();
			try{
				QueryResult<HColumn<String,Object>> result = ccc.cache_get(keyspace, columnfamily, key+i, names[i]);
				if(((int[])result.get().getValue()).length!=value.length){
					System.out.println("Cache Unmatched "+names[i]);
				}
				
			}catch(Exception e){System.out.println(names[i]);}
			long elapse = System.currentTimeMillis() - start;
			sum+=elapse;
		}
		System.out.println(">> Average Time : "+sum/run+"ms");
		System.out.println("------------------------");
		System.out.println(">> 3. Single Get Cache From Cassandra");
		sum = 0;
		
		Keyspace ks = HFactory.createKeyspace(keyspace, HFactory.getCluster("Test Cluster"));
		StringSerializer ss = new StringSerializer();
		ObjectSerializer os = new ObjectSerializer();
		
		for(int i = 0; i<run;i++){
			long start = System.currentTimeMillis();
			ColumnQuery<String, String, Object> columnquery = HFactory.createColumnQuery(ks, ss, ss, os);
			columnquery.setColumnFamily(columnfamily).setKey(key+i).setName(names[i]);
			QueryResult<HColumn<String, Object>> result = columnquery.execute();
			if(((int[])result.get().getValue()).length!=value.length){
				System.out.println("Value Unmatched");
			}
			long elapse = System.currentTimeMillis() - start;
			sum+=elapse;
		}
		System.out.println(">> Average Time : "+sum/run+"ms");
		System.out.println("------------------------");
	}
	
	public static void init(){
		SockIOPool pool = SockIOPool.getInstance();
		pool.setListServer("http://nexusz99.i.ahems.co.kr");
		pool.setInitConn(3);
		pool.setMaintSleep(30);
		pool.setMinConn(1);
		pool.setMaxConn(3);
		pool.setNagle(true);
		pool.setHashingAlg(SockIOPool.CONSISTENT_HASH);
		pool.initialize();
		
		Cluster cluster = HFactory.getOrCreateCluster("Test Cluster","cats1.kaist.ac.kr");
		cluster.addHost(new CassandraHost("cats1.kaist.ac.kr:9160"), false);
		cluster.addHost(new CassandraHost("cats2.kaist.ac.kr:9160"), false);
		
		
		com.danga.MemCached.SockIOPool mem = com.danga.MemCached.SockIOPool.getInstance();
		mem.setServers(new String[]{"cats3.kaist.ac.kr:11211","cats4.kaist.ac.kr:11211"});
		mem.initialize();
		
		MemCachedClient mmc = new MemCachedClient();
		mmc.flushAll();
		mmc.flushAll();
		
		Properties config = new Properties();
		String cfgFilename = "cc.config";
		InputStream file;
		try {
			file = new FileInputStream(cfgFilename);
			config.load(file);
			Setting.run = Integer.valueOf(config.getProperty("run"));
			Setting.value_size = Integer.valueOf(config.getProperty("size"));
			Setting.columnfamily = config.getProperty("columnfamily");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	private static class Setting{
		private static int run ;
		private static int value_size ;
		private static int wait;
		private static String columnfamily = "demo";
	}

}
