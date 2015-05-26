package com.ahems.Queue;

import java.util.NoSuchElementException;

public interface Queue {
	public void clear(int type);
	public void put(int type, Object o);
	public Object pop(int type) throws InterruptedException, NoSuchElementException;
}
