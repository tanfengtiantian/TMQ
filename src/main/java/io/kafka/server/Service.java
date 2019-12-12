package io.kafka.server;

import java.io.Closeable;
import java.io.File;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import io.kafka.plugin.BrokerContext;
import io.kafka.plugin.BrokerPlugins;
import io.kafka.transaction.store.JournalTransactionStore;
import io.kafka.transaction.store.TransactionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kafka.config.ServerConfig;
import io.kafka.log.ILogManager;
import io.kafka.log.imp.FixedSizeRollingStrategy;
import io.kafka.log.imp.LogManager;
import io.kafka.network.SocketServer;
import io.kafka.network.handlers.RequestHandlers;
import io.kafka.utils.Scheduler;

/**
 * @author tf
 * @version 创建时间：2019年1月17日 下午5:52:53
 * @ClassName 服务
 */
public class Service implements Closeable {

	private SocketServer socketServer;
	private ServerConfig config;
	private static Logger logger = LoggerFactory.getLogger(Service.class);
	private File logDir;
	private final CountDownLatch shutdownLatch = new CountDownLatch(1);
	private Scheduler scheduler = new Scheduler(1, "kafka-logcleaner-", false);
	private ILogManager logManager;
	private TransactionStore transactionStore;
	private final Map<String, Properties> pluginsInfo;
	private BrokerPlugins brokerPlugins;
	public Service(ServerConfig config, Map<String, Properties> pluginsInfo) {
        this.config = config;
        this.pluginsInfo = pluginsInfo;
        logDir = new File(config.getLogDir());
        if (!logDir.exists()) {
            logDir.mkdirs();
        }
    }
	public void startup(){
		try {
			//消息加载器
			logManager =new LogManager(config,//
					scheduler,//
	                1000L * 60 * config.getLogCleanupIntervalMinutes(),//
	                1000L * 60 * 60 * config.getLogRetentionHours(),//
	                true);
			logManager.setRollingStategy(new FixedSizeRollingStrategy(config.getLogFileSize()));
			logManager.load();
			logManager.startup();

			//事务加载器
			transactionStore=new JournalTransactionStore(config,logManager);
			transactionStore.load();


			RequestHandlers handlers = new RequestHandlers(config,logManager,transactionStore);
	        socketServer = new SocketServer(handlers, config);
	        socketServer.startup();



	        //插件启动
			this.brokerPlugins = new BrokerPlugins(pluginsInfo, config);
			BrokerContext context = new BrokerContext(logManager, transactionStore, config);
			this.brokerPlugins.init(context,null);
			this.brokerPlugins.start();
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
            scheduler.shutdown();
            if (socketServer != null) {
                socketServer.close();
            }
            if (logManager != null) {
                logManager.close();
            }
            if(this.brokerPlugins!=null){
				this.brokerPlugins.stop();
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
