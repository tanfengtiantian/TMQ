package io.kafka.network;

import io.kafka.config.ServerConfig;
import io.kafka.core.Controller;
import io.kafka.utils.Closer;
import sun.misc.Contended;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * @author tf
 * @version 创建时间：2019年1月17日 上午9:58:11
 * @ClassName 接受和配置新连接的线程。只需要一个
 */
public class Acceptor extends AbstractServerThread implements Controller {
	
	private int port;
	/**
	 * 处理器
	 */
    private Processor[] processors;
    /**
     * 发送缓冲区大小
     */
    @Contended("tlr")
    private int sendBufferSize;
    /**
     * 接收缓冲区大小
     */
    @Contended("tlr")
    private int receiveBufferSize;

    private final AtomicInteger currentProcessor = new AtomicInteger(0);

    /**
     *
     * @param port
     * @param processors
     * @param sendBufferSize
     * @param receiveBufferSize
     */
    public Acceptor(ServerConfig serverConfig, int port, Processor[] processors, int sendBufferSize, int receiveBufferSize) {
        super(serverConfig);
        this.port = port;
        this.processors = processors;
        this.sendBufferSize = sendBufferSize;
        this.receiveBufferSize = receiveBufferSize;
    }
    
	@Override
	public void run() {
        //初始化配置参数
        setSocketOptions(getSocketOptionsFromConfig(serverConfig));
		final ServerSocketChannel serverChannel;
        try {
            serverChannel = ServerSocketChannel.open();
            //设置非堵塞
            serverChannel.configureBlocking(false);
            //绑定端口
            serverChannel.socket().bind(new InetSocketAddress(port));
            //注册连接事件
            serverChannel.register(selectorManager.getSelector(), SelectionKey.OP_ACCEPT);

        } catch (IOException e) {
            logger.error("监听端口： " + port + " 失败(failed).");
            throw new RuntimeException(e);
        }
        
        logger.info("正在端口上等待连接: "+port);
        //等待处理器全部就绪完毕后接收请求
        selectorManager.awaitReady();
        startupComplete();
        int current = currentProcessor.get();
        while(isRunning()) {
            int ready = -1;
            try {
                ready = selectorManager.getSelector().select(500L);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            //获取失败 进入下次循环
            if(ready <= 0) continue;
            Iterator<SelectionKey> iter = selectorManager.getSelector().selectedKeys().iterator();
            while(iter.hasNext() && isRunning())
                try {
                    SelectionKey key = iter.next();
                    //移除这次key
                    iter.remove();
                    //
                    if(key.isAcceptable()) {
                        accept(key,processors[current]);
                    }else {
                        throw new IllegalStateException("无法识别SelectionKey的请求类型.");
                    }
                    //取%顺序调用处理线程
                    //currentProcessor = (currentProcessor + 1) % processors.length;
                    //位运算 取与 0 & 0 = 0 | 0 & 1 = 0 | 1 & 1 = 1
                    current = currentProcessor.getAndIncrement() & (processors.length - 1);
                } catch (Throwable t) {
                    logger.error("连接错误",t);
                }
        }
        //run over
        logger.info("关闭服务器和选择器.");
        Closer.closeQuietly(serverChannel, logger);
        Closer.closeQuietly(selectorManager.getSelector(), logger);
        shutdownComplete();
	}
	
	private void accept(SelectionKey key, Processor processor) throws IOException{
		//处理新接入的请求消息
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
        //设置接收数据最大Buffer
        serverSocketChannel.socket().setReceiveBufferSize(receiveBufferSize);
        //获取客户端请求通道
        SocketChannel socketChannel = serverSocketChannel.accept();
        //设置socket参数
        this.configureSocketChannel(socketChannel);
        //发送处理器队列
        processor.accept(socketChannel);
    }

}
