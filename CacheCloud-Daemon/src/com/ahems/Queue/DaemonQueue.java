package com.ahems.Queue;

import java.util.LinkedList;
import java.util.NoSuchElementException;

public class DaemonQueue implements Queue {

	private static DaemonQueue instance = new DaemonQueue();
	
	private LinkedList requestJobs = new LinkedList();
	private LinkedList failJobs = new LinkedList();
	
	private static final Object requestMonitor = new Object();
	private static final Object failMonitor = new Object();
	
	public static DaemonQueue getInstance(){
		if(instance == null){
			instance = new DaemonQueue();
		}
		return instance;
	}
	
	@Override
	public void clear(int type) {
		
	}

	@Override
	public void put(int type, Object o) {
		switch(type){
		case OpType.request:
			requestPut(o);
			break;
		case OpType.fail:
			failPut(o);
			break;
		}
	}

	@Override
	public Object pop(int type) throws InterruptedException, NoSuchElementException {
		Object o = null;
		switch(type){
		case OpType.request:
			o = requestPop();
			break;
		case OpType.fail:
			o = failPop();
			break;
		}
		return o;
	}
	
	private void requestPut(Object o){
		synchronized (requestMonitor) {
			requestJobs.addLast(o);
			requestMonitor.notify();
		}
	}
	
	private Object requestPop() throws InterruptedException, NoSuchElementException{
		Object o = null;
		synchronized (requestMonitor) {
			if(requestJobs.isEmpty()){
				requestMonitor.wait();
			}
			o = requestJobs.removeFirst();
		}
		
		if(o==null)
			throw new NoSuchElementException();
		return o;
	}
	
	private void failPut(Object o){
		synchronized (failMonitor) {
			failJobs.addLast(o);
		}
	}
	
	private Object failPop() throws InterruptedException{
		Object o = null;
		synchronized (failMonitor) {
			if(failJobs.isEmpty()){
				return null;
			}
			o = failJobs.removeFirst();
		}
		if(o==null)
			throw new NoSuchElementException();
		return o;
	}

}
