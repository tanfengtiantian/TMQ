package io.kafka.network;

import io.kafka.config.ServerConfig;
import io.kafka.core.AbstractController;
import io.kafka.core.DispatcherFactory;
import io.kafka.core.DispatcherTask;
import io.kafka.network.request.RequestHandlerFactory;
import io.kafka.utils.Closer;
import io.kafka.utils.Utils;
import java.io.Closeable;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tf
 * @version 创建时间：2019年1月17日 上午9:56:13
 * @ClassName NIOServer启动类
 */
public class SocketServer extends AbstractController implements Closeable {

	private final Logger logger = LoggerFactory.getLogger(SocketServer.class);

    private final RequestHandlerFactory handlerFactory;

    private final int maxRequestSize;
    //
    private final Processor[] processors;
    
    private final Acceptor acceptor;
	
    private final ServerConfig serverConfig;
    
    public SocketServer(RequestHandlerFactory handlerFactory, //
            ServerConfig serverConfig) {
        super();
        this.serverConfig = serverConfig;
        this.handlerFactory = handlerFactory;
        this.maxRequestSize = serverConfig.getMaxSocketRequestSize();
        this.processors = new Processor[serverConfig.getNumThreads()];
        this.acceptor = new Acceptor(serverConfig,serverConfig.getPort(), //
                processors, //
                serverConfig.getSocketSendBuffer(), //
                serverConfig.getSocketReceiveBuffer());
    }
    
    
    public void startup() throws InterruptedException {
        //stateListeners.add("");
        DispatcherTask dispatcherTask = new DispatcherTask();
        dispatcherTask.setReadEventDispatcher(DispatcherFactory.newDispatcher(serverConfig.getReadThreadCount(),
                "notify-kafka-ReadEvent", new ThreadPoolExecutor.CallerRunsPolicy()));
        dispatcherTask.setWriteEventDispatcher(DispatcherFactory.newDispatcher(serverConfig.getWriteThreadCount(),
                "notify-kafka-WriteEvent", new ThreadPoolExecutor.CallerRunsPolicy()));
        dispatcherTask.setDispatchMessageDispatcher(DispatcherFactory.newDispatcher(serverConfig.getDispatchMessageThreadCount(),
                "notify-kafka-DispatchMessage", new ThreadPoolExecutor.CallerRunsPolicy()));

        ServerSync serverSync = acceptor.initialServerSync(this, processors.length);
        //处理器最多处理的客户端连接数
        final int maxCacheConnectionPerThread = serverConfig.getMaxConnections() / processors.length;
        logger.debug("start {} Processor threads",processors.length);
        for (int i = 0; i < processors.length; i++) {
            processors[i] = new Processor(serverConfig, handlerFactory,dispatcherTask, maxRequestSize, maxCacheConnectionPerThread);
            processors[i].setServerSync(serverSync);
            Utils.newThread("kafka-processor-" + i, processors[i], false).start();
        }
        Utils.newThread("kafka-acceptor", acceptor, false).start();
        acceptor.awaitStartup();
        this.notifyStarted();
    }

    /**
     * Shutdown  socket server
     */
    public void close() {
        this.notifyStopped();
        Closer.closeQuietly(acceptor);
        for (Processor processor : processors) {
            Closer.closeQuietly(processor);
        }
    }

	
}
