package com.ahems;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.PropertyConfigurator;

import com.ahems.Queue.DaemonQueue;
import com.ahems.Queue.Queue;

public class main {
	
	/**
	 * args[0] : 실행할 Worker Thread 와 Failure Thread 의 갯수
	 * @param args
	 * @throws ParseException 
	 */
	public static void main(String[] args) throws ParseException {
		
		
		
		Queue queue = DaemonQueue.getInstance(); //모든 Thread 에서 공용으로 사용할 큐
		PropertyConfigurator.configure("log4j.properties");
		
		WorkerThread mc = new WorkerThread(queue);
		mc.init();
		mc.setDaemon(true);
		mc.start();
		
		FailureThread ft = new FailureThread(queue);
		ft.initialize();
		ft.setDaemon(true);
		ft.start();
		
		
		for(int i = 0; i<4; i++){
			WorkerThread mc2 = new WorkerThread(queue);
			mc2.setDaemon(true);
			mc2.start();
			
			FailureThread ft2 = new FailureThread(queue);
			ft2.initialize();
			ft2.setDaemon(true);
			ft2.start();
		}
		
		NIODaemon dm = new NIODaemon();
		dm.init_Daemon(queue);
		dm.startServer();
		
		
	}
	
	
}
