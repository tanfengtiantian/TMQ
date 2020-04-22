package io.kafka.server;

import io.kafka.config.ServerConfig;
import io.kafka.network.SocketServer;
import io.kafka.rpc.RpcRequestHandlers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.util.concurrent.CountDownLatch;

/**
 * @author tf
 * @version 创建时间：2019年1月17日 下午5:52:53
 * @ClassName 服务
 */
public class RpcService implements Closeable {

	private SocketServer socketServer;
	private ServerConfig config;
	private static Logger logger = LoggerFactory.getLogger(RpcService.class);
	private final CountDownLatch shutdownLatch = new CountDownLatch(1);
	public RpcService(ServerConfig config) {
        this.config = config;
    }
	public void startup(){
		try {
			//网络监听器启动
			RpcRequestHandlers handlers = new RpcRequestHandlers(config);
	        socketServer = new SocketServer(handlers, config);
	        socketServer.startup();

		} catch (Exception ex) {
			logger.error("========================================");
            logger.error("Fatal error during startup.", ex);
            logger.error("========================================");
            close();
		}
		
	}
	public void close() {
	 logger.info("Shutting down tfkafka server(brokerid={})...", this.config.getBrokerId());
        try {
            if (socketServer != null) {
                socketServer.close();
            }

        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
        }
        shutdownLatch.countDown();
        logger.info("Shutdown tfkafka (brokerid={}) completed", config.getBrokerId());
	}
	public void awaitShutdown(){
		try {
			shutdownLatch.await();
		} catch (InterruptedException e) {
			logger.warn(e.getMessage(), e);
		}
	}
}
