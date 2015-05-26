package com.ahems.api;

import java.io.IOException;
import java.net.SocketTimeoutException;

import org.apache.log4j.Logger;

import com.ahems.mapper.QueryResultMapper;

import me.prettyprint.cassandra.serializers.ObjectSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;

public class CacheCloudClient {
	private SockIOPool pool;
	private Cluster cluster;
	private Logger log = Logger.getLogger(CacheCloudClient.class);
	
	private final String get = "0";
	private final String columnslice = "1";
	private final String multigetslice = "2";
	private final String rangeslice = "3";
	private final String set = "4";
	private final String sp="/";
	private final String array_sp="&";
	
	public CacheCloudClient(){
		this.cluster = HFactory.getCluster("Test Cluster");
		this.pool = SockIOPool.getInstance();
	}
	
	public void cache_single_get_update(String keyspace, String columnfamily, String key, String name){
		String query = set+sp+keyspace+sp+columnfamily+sp+key+sp+name;
		String sockquery = keyspace+sp+columnfamily+sp+key+sp+name;
		SockIOPool.SockIO sock = pool.getSock(sockquery);
		if(sock==null)
			return;
		try {
			sock.write(query);
			sock.close();
		} catch (IOException e) {
			log.error("Cache Insert Fail",e);
		}
	}
	
	
	
	public void cache_insert(String keyspace, String columnfamily, String key, String name, Object value){
		String query = set+sp+keyspace+sp+columnfamily+sp+key+sp+name;
		String sockquery = keyspace+sp+columnfamily+sp+key+sp+name;
		SockIOPool.SockIO sock = pool.getSock(sockquery);
		if(sock==null){
			log.error("Cannot Get Daemon Socket");
			return;
		}
		try {
			
			Keyspace ks = HFactory.createKeyspace(keyspace, cluster);
			Mutator<String> mutator = HFactory.createMutator(ks, StringSerializer.get());
			mutator.insert(key, columnfamily, HFactory.createColumn(name, value, StringSerializer.get(), ObjectSerializer.get()));

			sock.write(query);
			sock.close();
			
		} catch (Exception e) {
			log.error("Cache Insert Fail",e);
		}
	}
	
	public void cache_delete(String keyspace, String columnfamily, String key, String name){
		Keyspace ks = HFactory.createKeyspace(keyspace, this.cluster);
		Mutator<String> mutator = HFactory.createMutator(ks, StringSerializer.get());
		mutator.delete(key, columnfamily, name, StringSerializer.get());
	}
	
	
	public QueryResultMapper<HColumn<String,Object>> cache_get(String keyspace, String columnfamily, String key, String name){
		QueryResultMapper<HColumn<String,Object>> result = null;
		int len = 0;
		byte[] databuf = null;
		String query = get+sp+keyspace+sp+columnfamily+sp+key+sp+name;
		String sockquery = keyspace+sp+columnfamily+sp+key+sp+name;
		SockIOPool.SockIO sock = pool.getSock(sockquery);
		try {
			sock.write(query);
			byte[] lenbuf = sock.read(4);
			len = sock.byteArrayToInt(lenbuf);
			if(len!=4){
				databuf = sock.read(len);
				result = (QueryResultMapper<HColumn<String,Object>>)sock.readObject(databuf);
			}
			sock.close();
			sock = null;
		} catch (IOException e) {
			log.error("IOException",e);
			if(sock!=null)
				try {
					sock.trueClose();
				} catch (IOException e1) {
					log.error("Socket Error",e1);
				}
			sock = null;
		} 
		catch (ClassNotFoundException e) {}
		
		return result;
	}
	
	public QueryResultMapper<ColumnSlice<String,Object>> cache_get_columnslice(String keyspace, String columnfamily,String key, String[] names_array){
		String query = columnslice+sp+keyspace+sp+columnfamily+sp+key+sp;
		for(int i = 0; i< names_array.length; i++){
			query+=names_array[i]+array_sp;
		}
		QueryResultMapper<ColumnSlice<String,Object>> result = null;
		
		SockIOPool.SockIO sock = pool.getSock(query);
		try {
			sock.write(query);
			byte[] lenbuf = sock.read(4);
			int len = sock.byteArrayToInt(lenbuf);
			if(len!=4){
				byte[] databuf = sock.read(len);
				result = (QueryResultMapper<ColumnSlice<String,Object>>)sock.readObject(databuf);
			}
			sock.close();
			sock = null;
		} catch (IOException e) {
			log.error("cache_get_columnslice > IOException",e);
			if(sock!=null)
				try {
					sock.trueClose();
				} catch (IOException e1) {
					log.error("Socket Error",e1);
				}
			sock = null;
		} catch (ClassNotFoundException e) {}
		return result;
	}
	
	public QueryResultMapper<Rows<String,String,Object>> cache_get_multigetslice(String keyspace, String columnfamily, String[] keys_array){
		QueryResultMapper<Rows<String,String,Object>> result = null;
		String query = multigetslice+sp+keyspace+sp+columnfamily+sp;
		for(int i = 0; i< keys_array.length; i++){
			query+=keys_array[i]+array_sp;
		}
		
		SockIOPool.SockIO sock = pool.getSock(query);
		try {
			sock.write(query);
			byte[] lenbuf = sock.read(4);
			int len = sock.byteArrayToInt(lenbuf);
			if(len!=4){
				byte[] databuf = sock.read(len);
				result = (QueryResultMapper<Rows<String,String,Object>>)sock.readObject(databuf);
			}
			sock.close();
			sock = null;
		} catch (IOException e) {
			log.error("cache_get_multigetslice > IOException",e);
			if(sock!=null)
				try {
					sock.trueClose();
				} catch (IOException e1) {
					log.error("Socket Error",e1);
				}
			sock = null;
		} catch (ClassNotFoundException e) {}
		
		return result;
	}
	
	public QueryResultMapper<OrderedRows<String,String,Object>> cache_get_rangeslice(String keyspace, String columnfamily, String[] keys_array){
		QueryResultMapper<OrderedRows<String,String,Object>> result = null;
		
		String query = rangeslice+sp+keyspace+sp+columnfamily+sp;
		for(int i = 0; i< keys_array.length; i++){
			query+=keys_array[i]+array_sp;
		}
		query+=sp+keys_array.length;
		SockIOPool.SockIO sock = pool.getSock(query);
		try {
			sock.write(query);
			byte[] lenbuf = sock.read(4);
			int len = sock.byteArrayToInt(lenbuf);
			if(len!=4){
				byte[] databuf = sock.read(len);
				result = (QueryResultMapper<OrderedRows<String,String,Object>>)sock.readObject(databuf);
			}
			sock.close();
			sock = null;
		} catch (IOException e) {
			log.error("cache_get_rangeslice > IOException",e);
			if(sock!=null)
				try {
					sock.trueClose();
				} catch (IOException e1) {
					log.error("Socket Error",e1);
				}
			sock = null;
		} catch (ClassNotFoundException e) {}
		return result;
	}
}
