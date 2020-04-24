package io.kafka.network;

import static java.lang.String.format;
import io.kafka.common.exception.InvalidRequestException;
import io.kafka.config.ServerConfig;
import io.kafka.core.Controller;
import io.kafka.core.DispatcherTask;
import io.kafka.network.receive.Receive;
import io.kafka.network.request.Request;
import io.kafka.network.request.RequestHandler;
import io.kafka.network.request.RequestHandlerFactory;
import io.kafka.network.send.Send;
import io.kafka.network.session.NioSession;
import io.kafka.network.session.SessionEvent;
import io.kafka.utils.Closer;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tf
 * @version 创建时间：2019年1月17日 上午9:58:42
 * @ClassName *处理来自单个连接的所有请求的线程。
 * 			  *其中每个都有自己的选择器
 */
public class Processor extends AbstractServerThread implements SessionEvent,Controller {

	private BlockingQueue<NioSession> newConnections;
	
	private static final Logger requestLogger = LoggerFactory.getLogger("kafka.request.logger");

	private RequestHandlerFactory requesthandlerFactory;

	private int maxRequestSize;

	private DispatcherTask dispatcher;

    private volatile int selectTries = 0;

    private long nextTimeout = 0;
	
	/**
	 * 
	 * @param requesthandlerFactory
	 * @param maxRequestSize   请求最大包size
	 * @param maxCacheConnections 最大连接数
	 */
	public Processor(ServerConfig serverConfig, RequestHandlerFactory requesthandlerFactory, //
                     DispatcherTask dispatcher,
                     int maxRequestSize,//
                     int maxCacheConnections) {
	    super(serverConfig);
		this.requesthandlerFactory = requesthandlerFactory;
		this.dispatcher = dispatcher;
		this.maxRequestSize = maxRequestSize;
		this.newConnections = new ArrayBlockingQueue<>(maxCacheConnections);
	}
	/**
	 * 连接处理
	 * @param session
	 */
	public void accept(NioSession session) {
		newConnections.add(session);
		//唤醒阻塞在selector.select上的线程
        getSelector().wakeup();
	}

	@Override
	public void run() {
	    //等待所有处理器准备就绪
        this.serverSync.notifyReady();
		startupComplete();
		while (isRunning()) {
			try {
				// 注册OP_Read
                configureNewConnections();
                final Selector selector = getSelector();
                int ready = selector.select(500);
                /*
                if (ready <= 0) {
                    this.selectTries++;
                    // 检测连接是否过期或者idle，计算下次timeout时间
                    this.nextTimeout = this.checkSessionTimeout();
                    continue;
                }else {
                    this.selectTries = 0;
                }*/
                if (ready <= 0) continue;
                Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                while (iter.hasNext() && isRunning()) {
                    SelectionKey key = null;
                    try {
                        key = iter.next();
                        iter.remove();
                        if (key.isReadable()) {
                            onRead(key);
                        } else if (key.isWritable()) {
                            OnWrite(key);
                        } else if (!key.isValid()) {
                            onClosed(key);
                        } else {
                            throw new IllegalStateException("SelectionKey-处理器线程的状态无法识别。");
                        }
                    }catch (EOFException eofe) {
                    	//通道读取不到数据
                    	Socket socket = channelFor(key).socket();
                    	requestLogger.debug(format("connection closed by %s:%d.", socket.getInetAddress(), socket.getPort()));
                        onClosed(key);
                    }
                    catch (InvalidRequestException ire) {
                        Socket socket = channelFor(key).socket();
                        requestLogger.info(format("关闭链接 ( %s:%d ) 无效的请求: %s", socket.getInetAddress(), socket.getPort(),
                                ire.getMessage()));
                        onClosed(key);
                    } catch (Throwable t) {
                        Socket socket = channelFor(key).socket();
                        final String msg = "关闭链接( %s:%d ) error :%s";
                        if (requestLogger.isDebugEnabled()) {
                        	requestLogger.error(format(msg, socket.getInetAddress(), socket.getPort(), t.getMessage()), t);
                        } else {
                        	requestLogger.info(format(msg, socket.getInetAddress(), socket.getPort(), t.getMessage()));
                        }
                        onClosed(key);
                    }
                }
			} catch (IOException e) {
				requestLogger.error(e.getMessage(), e);
			}
		}
	}

	public void onRead(final SelectionKey key) throws IOException {
        if (dispatcher.getReadEventDispatcher() == null) {
            this.read(key);
        }
        else {
            dispatcher.getReadEventDispatcher().dispatch(()->{
                try {
                    Processor.this.read(key);
                } catch (IOException e) {
                    requestLogger.error(e.getMessage());
                }
            });
        }
    }

    public void OnWrite(SelectionKey key) throws IOException {
        if (dispatcher.getWriteEventDispatcher() == null) {
            this.write(key);
        }
        else {
            dispatcher.getWriteEventDispatcher().dispatch(()->{
                try {
                    Processor.this.write(key);
                } catch (IOException e) {
                    requestLogger.error(e.getMessage());
                }
            });
        }

    }

    public void onMessage(NioSession session, Request request) throws IOException {
        if (dispatcher.getDispatchMessageDispatcher() == null) {
            dispatchReceivedMessage(session,request);
        }
        else {
            this.dispatcher.getDispatchMessageDispatcher().dispatch(()->{
                try {
                    Processor.this.dispatchReceivedMessage(session,request);
                } catch (IOException e) {
                    requestLogger.error(e.getMessage());
                }
            });
        }
    }

    private void read(SelectionKey key) throws IOException {
        SocketChannel socketChannel = channelFor(key);
        NioSession session = getSessionFromAttchment(key);
        if(session == null)
            return;
        Receive receive = session.getReceive();
        receive.readFrom(this,key,socketChannel,requesthandlerFactory);
    }

    /**
     * 发送接收到的消息
     */
    private Send dispatchReceivedMessage(NioSession session, Request request) throws IOException {
        Send maybeResponse = handle(request);
        // 如果有结果，放入session，切换状态
        if (maybeResponse != null) {
            session.resultSend(maybeResponse);
            //key.interestOps(SelectionKey.OP_WRITE);
        }
        return maybeResponse;
    }

	private void write(SelectionKey key) throws IOException {
        NioSession session = getSessionFromAttchment(key);
		Send response = session.getSend();
        session.onMessageSent(response);
        SocketChannel socketChannel = channelFor(key);
        int written = response.writeTo(socketChannel);
        if(requestLogger.isDebugEnabled()) {
            requestLogger.debug("Send[ "+response.getClass().getName()+" ] write[ " + written+" ] ");
        }
	}
	
	/**
	 * 处理生成可选响应的已完成请求
	 * @param request
	 * @return
	 */
	private Send handle(Request request) throws IOException {

		RequestHandler handlerMapping = requesthandlerFactory.mapping(request.getRequestKey());
		if (handlerMapping == null) {
            throw new InvalidRequestException("No handler found request");
        }
		//计时器
		long start = System.nanoTime();
        Send maybeSend = handlerMapping.handler(request.getRequestKey(), request);
        if(requestLogger.isDebugEnabled()){
        	requestLogger.debug(format("requestTypeId=[%d] 处理耗时: %d",request.getRequestKey(),System.nanoTime() - start));
        }
        return maybeSend;
	}
	
	private SocketChannel channelFor(SelectionKey key) {
		 return (SocketChannel) key.channel();
	}

    private final NioSession getSessionFromAttchment(final SelectionKey key) {
        if (key.attachment() instanceof NioSession) {
            return (NioSession) key.attachment();
        }
        return null;
    }

	public void onClosed(SelectionKey key) {
		SocketChannel channel = (SocketChannel) key.channel();
        if (requestLogger.isDebugEnabled()) {
        	requestLogger.debug("关闭链接： " + channel.socket().getRemoteSocketAddress());
        }
        Closer.closeQuietly(channel.socket());
        Closer.closeQuietly(channel);
        NioSession session = getSessionFromAttchment(key);
        session.close();
        key.attach(null);
        key.cancel();
	}
	private void configureNewConnections() {
        while (newConnections.size() > 0) {
            NioSession session = newConnections.poll();
            session.setReceive(maxRequestSize);
            logger.info("OP_ACCEPT请求->来自Client->{}",session.getRemoteSocketAddress());
            //标记读
            session.registerSession(getSelector());
        }
    }

    private final long checkSessionTimeout() {
        long nextTimeout = 0;

        return nextTimeout;
    }
}
