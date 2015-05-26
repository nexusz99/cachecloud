package com.ahems;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;

import org.apache.log4j.Logger;

import com.ahems.Queue.OpType;
import com.ahems.Queue.Queue;


public class NIODaemon {
	
	private static final String HOST = "localhost"; //Socket bind host address
	private static final int PORT = 10405; //Socket bing host port
	private Selector selector = null;
    private ServerSocketChannel serverSocketChannel = null;
    private ServerSocket serverSocket = null;
    private Queue queue;
    private Logger log = Logger.getLogger(NIODaemon.class);
    
    /**
	 * Daemon 초기화 함수.
	 * Selector, Channel 생성
	 */
	public void init_Daemon(Queue queue){
		try
        {
           selector = Selector.open();
           //서버소켓채널 생성
           serverSocketChannel = ServerSocketChannel.open();
           //비블록킹 모드 설정
           serverSocketChannel.configureBlocking(false);
           //서버소켓채널과 연결된 서버소켓 가져오기
           serverSocket = serverSocketChannel.socket();

           //소켓 바인드
           InetSocketAddress isa = new InetSocketAddress(PORT);
           serverSocket.bind(isa);
           serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

           this.queue = queue;
        }
        catch(IOException e)
        {
        	log.error(e.getMessage());
           return ;
        }
	}
	
	public void startServer(){
		while(true){
		try {
			selector.select();
		} catch (IOException e) {
			log.error(e.getMessage());
		}
		
		//셀렉터에 준비된 이벤트들을 하나씩 처리한다.
		Iterator it = selector.selectedKeys().iterator();
			while(it.hasNext()){
				SelectionKey key = (SelectionKey)it.next();
				//클라이언트가 연결을 요청할 경우
				if(key.isAcceptable()){
					accept(key);
				}
				//클라이언트로부터 데이터를 수신할 때
				else if(key.isReadable()){
					read(key);  //클라이언트에서 보내는 데이터 총 사이즈가 1K 미만일때만 테스트하고 싶을 때 사용하면됨.
				}	
				it.remove(); // 이미 처리한 이벤트이므로 제거.
			}	
		}
	}
	
	/**
	 * 클라이언트의 접속 요청 이벤트를 처리하여, 소켓체널에 등록하는 함수
	 * @param srcKey 연결 요청이 온 클라이언트의 SelectionKey
	 */
	private void accept(SelectionKey srcKey){
		ServerSocketChannel server = (ServerSocketChannel)srcKey.channel();
        SocketChannel sc;
        try
        {
            sc = server.accept();
            registerChannel(selector, sc, SelectionKey.OP_READ); //Selectionkey 로 read 와 write 를 등록
        }
        catch(ClosedChannelException e)
        {
            log.error(e.getMessage());
        }
        catch(IOException e)
        {
        	log.error(e.getMessage());
        }
	}
	
	/**
	 * 소켓체널에 클라이언트 등록
	 * @param selector
	 * @param sc
	 * @param ops
	 * @throws ClosedChannelException
	 * @throws IOException
	 */
	private void registerChannel(Selector selector,SocketChannel sc,int ops) throws ClosedChannelException,IOException
    {
        if(sc==null)
        {   
            return;
        }
        sc.configureBlocking(false); //non-blocking socket 으로 설정
        sc.register(selector, ops);
    }
	
	
	private void read(SelectionKey key)
	{
		SocketChannel channel = (SocketChannel)key.channel();
		ByteBuffer buf = ByteBuffer.allocate(1024);
		int size = 0;
		long start = System.currentTimeMillis();
		try {
			size = channel.read(buf);
			if(size==-1)
			{
				channel.close();
				return ;
			}
			buf.flip();
			byte[] bt = new byte[buf.limit()];
			int limit = buf.limit();
			for(int i = 0;i<limit;i++){
				bt[i] = buf.get();
			}
			DataStructure QueueStruct = new DataStructure();
			String requestKey = new String(bt);
			QueueStruct.data = requestKey;
			QueueStruct.channel = channel;
			
			long elapse = System.currentTimeMillis() - start;
			
			
			queue.put(OpType.request, QueueStruct);
			
		} catch (IOException e) {
			log.error(e.getMessage());
		} 
		
	}
}
