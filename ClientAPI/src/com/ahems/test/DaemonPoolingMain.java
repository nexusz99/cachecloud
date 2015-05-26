package com.ahems.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;

import me.prettyprint.cassandra.service.CassandraHost;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.exceptions.HCassandraInternalException;
import me.prettyprint.hector.api.exceptions.HInvalidRequestException;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.ahems.api.CacheCloudClient;
import com.ahems.api.SockIOPool;
import com.danga.MemCached.MemCachedClient;

public class DaemonPoolingMain {
	
	public static Object o = new Object();
	public static StringBuilder result = new StringBuilder();
	
	public static void main(String[] args) throws InterruptedException{
		
		BufferedWriter write = null;
		Logger log = Logger.getLogger(DaemonPoolingMain.class);
		PropertyConfigurator.configure("log4j.properties");
		
		try {
			initialization();
		} catch (Exception e) {
			log.error("Initialization Failed",e);
		}
		
		Cluster cluster = HFactory.getOrCreateCluster("Test Cluster","cats1.kaist.ac.kr");
		MemCachedClient mmc = new MemCachedClient();
		
		for(int cache = 0; cache < Setting.keys_cached_size.length; cache++){
			String columnfamily = Setting.columnfamily+cache;
			ColumnFamilyDefinition cfdef = HFactory.createColumnFamilyDefinition(Setting.keyspace, columnfamily);
			cfdef.setKeyCacheSize(Setting.keys_cached_size[cache]);
			cfdef.setRowCacheSize(0);
			cfdef.setColumnType(ColumnType.STANDARD);
			cfdef.setComparatorType(ComparatorType.BYTESTYPE);
			
			try{
				cluster.addColumnFamily(cfdef, true);
				
			} catch(HInvalidRequestException e){
				log.error(columnfamily+" already Define");
				
			} catch(HCassandraInternalException e){
				log.error("Hector API InternalException",e);
			}
			
			for(int value = 0; value < Setting.value_size.length; value++){
				
				int[] testValue = createTestValue(Setting.value_size[value]*1024);
				
				mmc.flushAll();
				mmc.flushAll();
				mmc.flushAll();
				
				//Set Test Value
				Info info = createTestInfo(columnfamily , 0, Setting.run, testValue);
				ThreadBench ben = new ThreadBench(0, info);
				ben.start();
				synchronized (o) {
					o.wait();
				}
				
				info.op = 10;
				ben = new ThreadBench(10, info);
				ben.start();
				synchronized (o) {
					o.wait();
				}
				
				Thread.sleep(60*1000);
				
				//Test
				for(int op = 0; op < Setting.op.length; op++){
					String filename = "cc100/CacheCloud-Machine:"+Setting.ServerNum+"-Value_Size:"+Setting.value_size[value]+"-Operation:"+Setting.op[op]+".txt";
					log.debug("Start >> "+filename);
					BufferedWriter writer = null;
					File f = new File("cc100");
					if(!f.isDirectory()){
						f.mkdir();
					}
					try {
						writer = new BufferedWriter(new FileWriter(filename));
					
						info.op = Setting.op[op];
						
						ben = new ThreadBench(op, info);
						ben.start();
						synchronized (o) {
							o.wait();
						}
						writer.write(result.toString());
						writer.close();
						
						
					} catch (Exception e) {
						log.error(e);
						System.exit(0);
					}
				}
				
			}
			
			
		}
	
		
	}
	
	private static void initialization() throws IOException{
		Properties config = new Properties();
		String cfgFilename = "cc.config";
		InputStream file = new FileInputStream(cfgFilename);
		
		config.load(file);
		
		Setting.keyspace = config.getProperty("KeySpace");
		Setting.columnfamily = config.getProperty("ColumnFamily");
		Setting.run = Integer.valueOf(config.getProperty("Run"));
		Setting.value_size = parseIntArray(config.getProperty("Value-Size"));
		Setting.keys_cached_size = parseIntArray(config.getProperty("KeyCached"));
		Setting.op = parseIntArray(config.getProperty("Operation"));
		
		String serverlist = config.getProperty("Server");
		Setting.ServerNum = serverlist.split("/").length;
		
		SockIOPool pool = SockIOPool.getInstance();
		pool.setServers(toServerArray(serverlist, 0));
		
		pool.setInitConn(3);
		pool.setMaintSleep(30);
		pool.setMinConn(1);
		pool.setMaxConn(3);
		pool.setNagle(true);
		pool.setHashingAlg(SockIOPool.CONSISTENT_HASH);
		pool.initialize();
		
		com.danga.MemCached.SockIOPool mempool = com.danga.MemCached.SockIOPool.getInstance();
		mempool.setServers(toServerArray(serverlist, 1));
		mempool.initialize();
		
		Cluster cluster = HFactory.getOrCreateCluster("Test Cluster","cats1.kaist.ac.kr");
		cluster.addHost(new CassandraHost("cats1.kaist.ac.kr:9160"), false);
		cluster.addHost(new CassandraHost("cats2.kaist.ac.kr:9160"), false);
		
		
		
	}
	
	private static String[] toServerArray(String serverlist, int type){
		String[] tmp = serverlist.split("/");
		for(int i = 0; i < tmp.length; i++){
			switch(type){
				case 0: //cachecloud
					tmp[i]+=":10405";
					break;
				case 1: //memcached
					tmp[i]+=":11211";
					break;
			}
		}
		return tmp;
	}
	
	private static int[] parseIntArray(String src){
		String[] tmp = src.split(",");
		int len = tmp.length;
		int[] ret = new int[len];
		for(int i = 0; i < len ; i++){
			ret[i] = Integer.parseInt(tmp[i]);
		}
		return ret;
	}
	
	private static int[] createTestValue(int size){
		int[] ret = new int[size];
		for(int i =0; i<size; i++){
			ret[i] = 1;
		}
		return ret;
	}
	
	private static Info createTestInfo(String cf, int op, int run, int[] value){
		Info ret = new Info();
		ret.keyspace = Setting.keyspace;
		ret.columnfamily = cf;
		ret.op = op;
		ret.run = run;
		ret.value = value;
		ret.key = "TestKey";
		ret.name = "TestName";
		return ret;
	}
	
	private static class Setting{
		private static int run ;
		private static int[] value_size ;
		private static int[] keys_cached_size;
		private static int[] op ;
		private static String keyspace = "netbook";
		private static String columnfamily = "sung";
		private static int ServerNum;
	}
}
