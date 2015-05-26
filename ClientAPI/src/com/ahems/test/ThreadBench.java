package com.ahems.test;

import java.net.SocketTimeoutException;

import me.prettyprint.cassandra.serializers.ObjectSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;

import org.apache.log4j.Logger;

import com.ahems.api.CacheCloudClient;

public class ThreadBench extends Thread{
	private int threadNo;
	private int jobCount;
	private int op;
	private int[] value;
	private String key;
	private String name;
	private String keyspace = "cho";
	private String columnfamily = "sung";
	
	Logger log = Logger.getLogger(ThreadBench.class);
	
	private StringSerializer ss = new StringSerializer();
	private ObjectSerializer os = new ObjectSerializer();
	private Keyspace ks ;
	private StringBuilder result = new StringBuilder();
	private CacheCloudClient api = new CacheCloudClient();
	
	public ThreadBench(int no, Info info){
		this.keyspace = info.keyspace;
		this.columnfamily = info.columnfamily;
		this.op = info.op;
		this.key = info.key;
		this.name = info.name;
		this.jobCount = info.run;
		this.value = info.value;
		this.threadNo = no;
		
		ks = HFactory.createKeyspace(this.keyspace, HFactory.getCluster("Test Cluster"));
	}
	
	public void run(){
		switch(op){
		case 0:
			set();
			break;
		case 1:
			SingleGet();
			break;
		case 2:
			multigetslice();
			break;
		case 10:
			single_get_update();
			break;
		case 11:
			multigetslice_update();
			break;
		}
		
		synchronized (DaemonPoolingMain.o) {
			DaemonPoolingMain.result = result;
			DaemonPoolingMain.o.notifyAll();
		}
	}
	
	private void single_get_update(){
		System.out.println("Update");
		for(int i = 0; i< jobCount; i++){
			api.cache_single_get_update(keyspace, columnfamily, key+i, name+i);
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
			}
		}
	}
	
	private void multigetslice_update(){
		System.out.println("multigetslice Update");
		for(int i = 0; i< jobCount; i+=5){
			String[] keys_array = new String[]{key+(i+0),key+(i+1),key+(i+2),key+(i+3),key+(i+4)};
			api.cache_get_multigetslice(keyspace, columnfamily, keys_array);
		}
		
	}
	
	private void set(){
		System.out.println("Set");
		for(int i = 0; i< jobCount; i++){
			api.cache_insert(keyspace, columnfamily, key+i, name+i, value);
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
			}
		}
		
		
	}
	
	private void SingleGet(){
		long start = System.currentTimeMillis();
		long cs, elapse,now;
		int cachemiss = 0 , valuemiss = 0;
		String tmpName = name;
		//cache hit
		for(int i = 0;i<jobCount; i++){
			cs = System.currentTimeMillis();
			try{
				QueryResult<HColumn<String,Object>> resultset = api.cache_get(keyspace, columnfamily, key+i, name+i);
				if(resultset == null || resultset.get()==null){
					cachemiss++;
					/*ColumnQuery<String, String, Object> columnquery = HFactory.createColumnQuery(ks, ss, ss, os);
					columnquery.setColumnFamily(columnfamily).setKey(key+i).setName(tmpName+i);
					QueryResult<HColumn<String, Object>> qresult = columnquery.execute();
					if(!isEqual(qresult.get(), value)){
						valuemiss++;
						log.debug("Cassandra Value Unmatched!");
					}*/
				} else{
					if(!isEqual(resultset.get(), value)){
						valuemiss++;
						log.debug("Cache Value Unmatched!");
					}
				}
			}
			catch(Exception e){
				log.debug("Cache Get Exception",e);
			} finally{
				elapse = System.currentTimeMillis() - cs;
				now = System.currentTimeMillis();
				result.append(now-start+"\t"+elapse+"\n");
			}
		}
		
		log.debug("Cache Miss Count : "+cachemiss);
		log.debug("Value Unmatched Count : "+valuemiss);
		
	}
	
	private void multigetslice(){
		long start = System.currentTimeMillis();
		long cs, elapse,now;
		int misscnt = 0;
		for(int i = 0;i<jobCount; i+=5){
			String[] keys_array = new String[]{key+(i+0),key+(i+1),key+(i+2),key+(i+3),key+(i+4)};
			cs = System.currentTimeMillis();
			QueryResult<Rows<String,String,Object>> resultset = api.cache_get_multigetslice(keyspace, columnfamily, keys_array);
			if(resultset==null || resultset.get()==null){
				misscnt++;
				MultigetSliceQuery<String,String,Object> query = HFactory.createMultigetSliceQuery(ks, ss, ss, os);
				query.setColumnFamily(columnfamily).setKeys(keys_array).setRange("", "", false, keys_array.length);
				QueryResult<Rows<String,String,Object>> a = query.execute();
				//System.out.println(a.get().getCount());
			} else{
				if(((int[])resultset.get().getByKey(key+(i+0)).getColumnSlice().getColumnByName(name+(i+0)).getValue()).length!=value.length){
					misscnt++;
					log.debug("Cache Unmatched\tkey : "+key+i+"/"+name+i+"\nresult : "
							+((int[])resultset.get().getByKey(key+(i+0)).getColumnSlice().getColumnByName(name+(i+0)).getValue()).length
							+"\nWant : "+value.length);
				}
			}
			elapse = System.currentTimeMillis() - cs;
			now = System.currentTimeMillis();
			result.append(now-start+"\t"+elapse+"\n");
		}
		log.debug("Cache Miss Count : "+misscnt);
	}
	
	private boolean isEqual(HColumn<String, Object> src,int[] dst){
		boolean ret = false;
		try{
			if(((int[])src.getValue()).length == dst.length)
				ret = true;
			else 
				ret = false;
		} catch(NullPointerException e){
			e.printStackTrace();
		}
		return ret;
	}
	
}
