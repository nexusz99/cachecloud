package com.ahems.api;

import java.io.IOException;

public interface LineStream {
	/**
	 * Send Query to Daemon
	 * @param query
	 * @throws IOException
	 */
	public void write(String query) throws IOException;
	
	
	/**
	 * Read bytes from socket
	 * @param len Read Length
	 * @return
	 * @throws IOException
	 */
	public byte[] read(int len) throws IOException; 
	
	
	/**
	 * Read Object From byte buffer
	 * @param buf Byte Array to read object
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public Object readObject(byte[] buf) throws IOException, ClassNotFoundException;
}
