package com.ahems.test;

/*
 * 벤치마킹하기 위한 쓰레드가 쓰기 위한 테스트 정보
 */
public class Info {
	public String keyspace;
	public String columnfamily;
	public String key;
	public String name;
	public int[] value;
	public int op;
	public int run;
}
