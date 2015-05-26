package com.ahems;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;

import com.ahems.FailureThread.Command;
import com.ahems.Queue.OpType;
import com.ahems.Queue.Queue;
import com.danga.MemCached.MemCachedClient;
import com.danga.MemCached.SockIOPool;


/**
 * Client 에서 온 요청을 처리하는 WorkerThread
 * @author nexusz99
 *
 */
public class WorkerThread extends Thread{
	
	private Queue queue = null;
	/**
	 * 
	 * @param no 쓰레드 번호
	 * @param queue 큐
	 */
	public WorkerThread(Queue queue){
		this.queue = queue;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void run() {
		
		MemCachedClient mmc = new MemCachedClient(); //Memcached Server Connectioin Instance
		Logger log = Logger.getLogger(WorkerThread.class);
		while(true){
			SocketChannel channel = null;
			ByteBuffer response_buf = null;
			byte[] result = null;
			String key = null;
			try{
				/*
				 * 큐에서 데이터를 가져온다. 만약 데이터가 없다면, 쓰레드는 wait() 상태가 되고 데이터가 큐에 삽입되면 notify()에 의해 깨어난다.
				 */
				DataStructure workdata  = (DataStructure) queue.pop(OpType.request);
				key = workdata.data;
				channel = workdata.channel;
				ArrayList<String> keyset = null;
				Object[] value = null;
				ByteArrayOutputStream valueBuffer = new ByteArrayOutputStream();
				
				/* 
				 * Client 에서 온 쿼리가 'SET' 이 아닐 경우에만 캐시에서 데이터를 가져온다.
				 * 왜냐하면, 모든 명령어에 대해 일단 캐시에 접근하는데, 이 때 SET은 캐시화를 시키는 것이기 때문에 캐시에 접근할 필요가 없다.
				 * SET에 대해서는 무조건 FAIL 처리 하고, FailureThread 가 이를 Single Get과 같은 방식으로 캐시화를 한다. 
				 */
				if(key.charAt(0) == Command.set+48)
				{
					queue.put(OpType.fail, key);
					continue;
				}
				
				keyset = (ArrayList<String>)mmc.get(key); //해당 키에 대한 분할되어 저장되어있을 키셋을 얻어온다.
				
				if(!keyset.isEmpty() || keyset!=null) //분할되어 있는 키셋의 value를 하나로 합친다.
				{
					String[] keylist = (String[])keyset.toArray(new String[keyset.size()]);
					value = (Object[])mmc.getMultiArray(keylist);
					for(int i=0;i<value.length;i++){
						valueBuffer.write((byte[])value[i]);
					}
					result = valueBuffer.toByteArray();
				}
				
			}
			catch (IOException e) {
			}
			catch(NullPointerException e){
			}
			catch (InterruptedException e) {}
			catch (NoSuchElementException e){}
			finally{
				if(key.charAt(0) == Command.set+48)
				{
					continue;
				}
				if(result==null)
				{
					result = "fail".getBytes(); // 캐시 miss가 발생할 경우 fail이라는 단어의 길이를 전송한다. 따라서 클라이언트에서 처음 4바이트를 읽어 그 값이 4 이면 캐시 miss가 발생한 것이다.
					queue.put(OpType.fail, key); //Cache Miss 가 난 경우이므로 나중에 Cassandra 에서 읽어들이기 위해 Queue 에 저장해 놓는다.
				}
				int len = result.length;
				response_buf = ByteBuffer.allocate(len+4);
				
				response_buf.putInt(len);
				if(len!=4)
					response_buf.put(result);
				response_buf.flip();
				
				/*
				 * 캐시 데이터를 클라이언트에게 보내주는 부분이다. Loop를 도는 이유는 보내는 데이터 사이즈가 매우 크게 되면 한번에 전송이 불가능하기 때문에, 데이터를 전부다 전송할 때 까지 루프를 돈다.
				 * 
				 */
				
				try{
					int re = channel.write(response_buf); //결과를 클라이언트에 전송
					while(re < len){
						re+=channel.write(response_buf);
					}
					
					Thread.sleep(1); //Thread 가 CPU 자원을 다 사용하지 못하게 하기 위해 1ms 의 쉬는 시간을 준다.
				} catch(Exception e){
					
				}
			}
		}
	}
	
	/**
	 * Memecached Server Connection Init
	 */
	public void init(){
		String[] servers = {"localhost:11211"};
		
		SockIOPool pool = SockIOPool.getInstance();
		pool.setServers(servers);
		pool.setInitConn( 5 ); 
		pool.setMinConn( 5 );
		pool.setMaxConn( 250 );
		pool.setMaintSleep( 30 );
		pool.setNagle( false );
		pool.setSocketTO( 3000 );
		pool.setAliveCheck( true );
		pool.initialize();
	}
	
	
}
