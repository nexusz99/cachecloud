package com.ahems;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.ahems.Queue.OpType;
import com.ahems.Queue.Queue;
import com.ahems.mapper.QueryResultMapper;
import com.danga.MemCached.MemCachedClient;

import me.prettyprint.cassandra.serializers.ObjectSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHost;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;

/**
 * GET이 FAIL난 데이터들에 대해서 Cassandra에서 데이터를 불러와 Memcached에 저장하는 Thread
 * @author nexusz99
 *
 */
public class FailureThread extends Thread{
	@SuppressWarnings("unused")
	private final int SleepTime=1*1000; //30초마다 Thread 실행
	private final int maxSize = 1020*1024; 
	private Cluster cluster ;
	private Keyspace ks;
	private StringSerializer ss;
	private ObjectSerializer os;
	private MemCachedClient mmc;
	private Queue queue ;
	private Logger log = Logger.getLogger(FailureThread.class);
	
	public FailureThread(Queue queue){
		this.queue = queue;
	}
	
	@Override
	public void run(){
		while(true){
			try{
					String query = (String)queue.pop(OpType.fail);
					if(query==null)
						Thread.sleep(SleepTime);
					
					String[] sp = query.split("/");
					int command = Integer.valueOf(sp[0]);
					String keyspace = sp[1];
					String columnfamily = sp[2];
					ks = HFactory.createKeyspace(keyspace, cluster);
					byte[] buf = null;
					switch(command){
					case Command.set:
						query = query.replaceFirst("[0-9]", "0");
					case Command.get: //ColumnQuery
						ColumnQuery<String, String, Object> cq = HFactory.createColumnQuery(ks, ss, ss, os);
						cq.setColumnFamily(columnfamily).setKey(sp[3]).setName(sp[4]);
						buf = toByteArray(cq.execute());
						break;
					case Command.columnslice: //SliceQuery
						SliceQuery<String,String,Object> sq = HFactory.createSliceQuery(ks, ss, ss, os);
						sq.setColumnFamily(columnfamily).setKey(sp[3]).setColumnNames(getArrays(sp[4]));
						buf = toByteArray(sq.execute());
						break;
					case Command.multigetslice: //MultigetSliceQuery
						MultigetSliceQuery<String, String, Object> msq = HFactory.createMultigetSliceQuery(ks, ss, ss, os);
						String[] keys = getArrays(sp[3]);
						msq.setColumnFamily(columnfamily).setKeys(keys).setRange("", "", false, getArraySize(keys));
						buf = toByteArray(msq.execute());
						break;
					case Command.rangeslice: //RangeSlicesQuery
						keys = getArrays(sp[3]);
						RangeSlicesQuery<String, String, Object> rsq = HFactory.createRangeSlicesQuery(ks, ss, ss, os);
						int size = Integer.valueOf(sp[4].trim());
						rsq.setColumnFamily(columnfamily).setKeys(keys[0], keys[1]).setRange("", "", false, size);
						buf = toByteArray(rsq.execute());
						break;
					}
					
					
					//Cassandra에서 가져온 데이터가 있을 경우에만 Memcached 에 Set 한다. 
					boolean ret = false;
					if(buf!=null){
						ArrayList<String> keyset= new ArrayList<String>();
						if(buf.length > this.maxSize){
							int bufPointer=0,i=0;
							String key;
							byte[] tmpbuf ;
							while(bufPointer!=buf.length){
								if(buf.length - bufPointer > this.maxSize)
								{
									key = query.trim()+"/"+i;
									tmpbuf = new byte[this.maxSize];
									System.arraycopy(buf, bufPointer, tmpbuf, 0, this.maxSize);
									ret = mmc.set(key,tmpbuf);
									if(!ret)
									{
										log.error("Failer Data >> Memcached set fail : "+key+ " [Data size : "+(float)this.maxSize/1024+"kb]");
										break;
									}
									else
									{
										i++;
										bufPointer+=this.maxSize;
										keyset.add(key);
									}
								}
								else
								{
									int size = buf.length - bufPointer;
									key = query.trim()+"/"+i;
									tmpbuf = new byte[size];
									System.arraycopy(buf, bufPointer, tmpbuf, 0, size);
									ret = mmc.set(key,tmpbuf);
									if(!ret)
									{
										log.error("Failer Data >> Memcached set fail : "+key+ " [Data size : "+(float)this.maxSize/1024+"kb]");
										queue.put(OpType.fail, query);
										break;
									}
									else
									{
										i++;
										bufPointer+=size;
										keyset.add(key);
									}
								}
							}
						}
						else
						{
							ret = mmc.set(query.trim()+"/0",buf);
							if(!ret){
								log.error("Failer Data >> Memcached set fail : "+query+ " [Data size : "+(float)buf.length/1024+"kb]");
								continue;
							}
							else
								keyset.add(query.trim()+"/0");
						}
						
						ret = mmc.set(query, keyset); //쿼리에 대한 keyset 저장
						if(!ret){
							log.error("Failer keyset >> Memcached set fail : "+query+ " [Data size : "+(float)buf.length/1024+"kb]");
						}
						
					} else{
						log.error("Get Data From Cassandra Fail : "+query);
					}
					Thread.sleep(1);
			}
			catch (Exception e){}
			
		}
	}
	
	public void initialize(){
		cluster = HFactory.getOrCreateCluster("Test Cluster", "cats1.kaist.ac.kr:9160");
		cluster.addHost(new CassandraHost("cats1.kaist.ac.kr:9160"), false);
		cluster.addHost(new CassandraHost("cats2.kaist.ac.kr:9160"), false);
		mmc = new MemCachedClient();
		ss = new StringSerializer();
		os = new ObjectSerializer();
	}
	
	/*
	 * QueryResult 객체를 직렬화 해주는 부분이다. 실제 인자로 넘겨지는 부분은 QueryResultMapper 객체이다.
	 */
	private <T> byte[] object2byte(QueryResult<T> obj) throws IOException{
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(bos);
		oos.writeObject(obj);
		return bos.toByteArray();
	}
	
	private <T> byte[] toByteArray(QueryResult<T> src) throws IOException{
		if(src==null || src.get()==null)
			return null;
		
		QueryResultMapper<T> dst = new QueryResultMapper<T>();
		dst.setExcutionTimeMicro(src.getExecutionTimeMicro());
		dst.setExcutionTimeNano(src.getExecutionTimeNano());
		dst.setType(src.get());
		
		return object2byte(dst);
	}
	
	
	// multigetslice 와 같은 multiget 에서 multi 를 위한 배열을 얻어오기 위해 파싱하는 함수
	private String[] getArrays(String arrays){
		return arrays.split("&");
	}
	
	private int getArraySize(String[] array){
		return array.length;
	}
	
	// 각 명령어들의 고유 번호를 저장하는 클래스
	protected static class Command{
		public static final int get = 0;
		public static final int columnslice = 1;
		public static final int multigetslice = 2;
		public static final int rangeslice = 3;
		public static final int set = 4;
	}
}




