package com.ahems.mapper;

import java.io.Serializable;
import me.prettyprint.cassandra.service.CassandraHost;
import me.prettyprint.hector.api.query.Query;
import me.prettyprint.hector.api.query.QueryResult;

public class QueryResultMapper<T> implements QueryResult<T>, Serializable {
	
	private T value;
	
	private long ExecutionTimeMicro;
	private long ExcutionTimeNano;
	
	public void setExcutionTimeMicro(long ExecutionTimeMicro){
		this.ExecutionTimeMicro = ExecutionTimeMicro;
	}
	
	public void setExcutionTimeNano(long ExcutionTimeNano){
		this.ExcutionTimeNano = ExcutionTimeNano;
	}
	
	@Override
	public long getExecutionTimeMicro() {
		return ExecutionTimeMicro;
	}

	@Override
	public long getExecutionTimeNano() {
		return ExcutionTimeNano;
	}

	@Override
	public CassandraHost getHostUsed() {
		return null;
	}
	
	
	public void setType(T type){
		this.value = type;
	}
	
	@Override
	public Query<T> getQuery() {
		return null;
	}

	@Override
	public T get() {
		return value;
	}
	
}