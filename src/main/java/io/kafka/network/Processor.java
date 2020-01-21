package io.kafka.network;

import static java.lang.String.format;
import io.kafka.api.RequestKeys;
import io.kafka.common.exception.InvalidRequestException;
import io.kafka.config.ServerConfig;
import io.kafka.core.Controller;
import io.kafka.network.receive.BoundedByteBufferReceive;
import io.kafka.network.receive.Receive;
import io.kafka.network.request.RequestHandler;
import io.kafka.network.request.RequestHandlerFactory;
import io.kafka.network.send.Send;
import io.kafka.network.session.NioSession;
import io.kafka.utils.Closer;

import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.nio.channels.ClosedChannelException;
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
public class Processor extends AbstractServerThread implements Controller {

	private BlockingQueue<NioSession> newConnections;
	
	private static final Logger requestLogger = LoggerFactory.getLogger("kafka.request.logger");

	private RequestHandlerFactory requesthandlerFactory;

	private int maxRequestSize;

    private volatile int selectTries = 0;

    private long nextTimeout = 0;
	
	/**
	 * 
	 * @param requesthandlerFactory
	 * @param maxRequestSize   请求最大包size
	 * @param maxCacheConnections 最大连接数
	 */
	public Processor(ServerConfig serverConfig, RequestHandlerFactory requesthandlerFactory, //
                     int maxRequestSize,//
                     int maxCacheConnections) {
	    super(serverConfig);
		this.requesthandlerFactory = requesthandlerFactory;
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
                        	read(key);
                        } else if (key.isWritable()) {
                            write(key);
                        } else if (!key.isValid()) {
                            close(key);
                        } else {
                            throw new IllegalStateException("SelectionKey-处理器线程的状态无法识别。");
                        }
                    }catch (EOFException eofe) {
                    	//通道读取不到数据
                    	Socket socket = channelFor(key).socket();
                    	requestLogger.debug(format("connection closed by %s:%d.", socket.getInetAddress(), socket.getPort()));
                        close(key);
                    }
                    catch (InvalidRequestException ire) {
                        Socket socket = channelFor(key).socket();
                        requestLogger.info(format("关闭链接 ( %s:%d ) 无效的请求: %s", socket.getInetAddress(), socket.getPort(),
                                ire.getMessage()));
                        close(key);
                    } catch (Throwable t) {
                        Socket socket = channelFor(key).socket();
                        final String msg = "关闭链接( %s:%d ) error :%s";
                        if (requestLogger.isDebugEnabled()) {
                        	requestLogger.error(format(msg, socket.getInetAddress(), socket.getPort(), t.getMessage()), t);
                        } else {
                        	requestLogger.info(format(msg, socket.getInetAddress(), socket.getPort(), t.getMessage()));
                        }
                        close(key);
                    }
                }
			} catch (IOException e) {
				requestLogger.error(e.getMessage(), e);
			}
		}
	}

    private void read(SelectionKey key) throws IOException {
        SocketChannel socketChannel = channelFor(key);
        NioSession session = getSessionFromAttchment(key);
        if(session == null)
            return;
        Receive request = session.getReceive();
        //sizeBuffer [size -4bytes] + contentBuffer
        int read = request.readFrom(socketChannel);
        if (read < 0) {
            close(key);
        } else if (request.complete()) {
            Send maybeResponse = handle(key, request);
            // 如果有响应，发送它，否则什么都不做。
            if (maybeResponse != null) {
                session.resultSend(maybeResponse);
                key.interestOps(SelectionKey.OP_WRITE);
                request.reset();
            }
        } else {
            // 断包处理
            key.interestOps(SelectionKey.OP_READ);
            getSelector().wakeup();
        }
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
        if (response.complete()) {
            key.interestOps(SelectionKey.OP_READ);
        } else {
            key.interestOps(SelectionKey.OP_WRITE);
            getSelector().wakeup();
        }
	}
	
	/**
	 * 处理生成可选响应的已完成请求
	 * @param key
	 * @param request
	 * @return
	 */
	private Send handle(SelectionKey key, Receive request) throws IOException {
		//[size] + buffer=([type -2bytes] + Len(topic) + topic + partition + messageSize + message)
		final short requestTypeId = request.buffer().getShort();
		//获取请求type类型
		final RequestKeys requestType = RequestKeys.valueOf(requestTypeId);
		if (requestType == null) {
            throw new InvalidRequestException("未知请求类型  requestTypeId：" + requestTypeId);
        }
		RequestHandler handlerMapping = requesthandlerFactory.mapping(requestType, request);
		if (handlerMapping == null) {
            throw new InvalidRequestException("No handler found request");
        }
        NioSession session = getSessionFromAttchment(key);
        session.onMessageReceived(requestType,request);
		//计时器
		long start = System.nanoTime();
        Send maybeSend = handlerMapping.handler(requestType, request);
        if(requestLogger.isDebugEnabled()){
        	requestLogger.debug(format("requestTypeId=[%d] 处理耗时: %d",requestTypeId,System.nanoTime() - start));
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

	private void close(SelectionKey key) {

		SocketChannel channel = (SocketChannel) key.channel();
        if (requestLogger.isDebugEnabled()) {
        	requestLogger.debug("关闭链接： " + channel.socket().getRemoteSocketAddress());
        }
        Closer.closeQuietly(channel.socket());
        Closer.closeQuietly(channel);
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
