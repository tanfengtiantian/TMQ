package io.kafka.log.imp;

import static java.lang.String.format;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import io.kafka.transaction.store.JournalStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.kafka.server.TopicTask;
import io.kafka.api.OffsetRequest;
import io.kafka.common.IteratorTemplate;
import io.kafka.config.ServerConfig;
import io.kafka.log.ILog;
import io.kafka.log.ILogManager;
import io.kafka.log.ILogSegment;
import io.kafka.log.LogSegmentFilter;
import io.kafka.log.RollingStrategy;
import io.kafka.server.ServerRegister;
import io.kafka.utils.Closer;
import io.kafka.utils.Scheduler;
import io.kafka.utils.Utils;

/**
 * @author tf
 * @version 创建时间：2018年12月30日 下午9:57:18
 * @ClassName LogManager
 */
public class LogManager implements  ILogManager{
	
	private final Logger                          					logger = LoggerFactory.getLogger(LogManager.class);
	//配置文件地址
	private ServerConfig                         					config;
	//文件滚动策略
	private RollingStrategy              							rollingStategy;
	//恢复最后一个文件为可写
	private boolean                        							needRecovery;
	//定时清理日志任务   一种是超过预设的时间则删除，二是超过预设的大小则删除
	private Scheduler                                               scheduler;
	//定时刷盘任务
	private final Scheduler                                         logFlusherScheduler = new Scheduler(1, "kafka-logflusher-", false);
	//文件块最大条目
	private int                           							flushInterval;
	//文件容量最大值
	private int                          							maxMessageSize;
	//文件日志目录
	private File                                 					logDir;
	//zk启动同步
	private CountDownLatch 											startupLatch;
	//zk注册类
	private ServerRegister                                          serverRegister;
	//Topic注册任务状态
	private volatile boolean 										stopTopicRegisterTasks = false;
	//Topic注册任务队列
	private final LinkedBlockingQueue<TopicTask> 					topicRegisterTasks = new LinkedBlockingQueue<TopicTask>();
	
	//key ：top   v = {0(分区):00(日志段)#01#02 , 1:10#11#12}  
	private final ConcurrentMap<String, ConcurrentMap<Integer,ILog>> logs = new ConcurrentSkipListMap<>();
	
	private final Map<String, Integer> topicPartitionsMap;
	
	private final Object logCreationLock = new Object();
	
	final Random random = new Random();
	
	private int numPartitions;
	
	private long logCleanupIntervalMs;
	
	private long logCleanupDefaultAgeMs;
	
	public LogManager(ServerConfig config, // 定期清理日志时间间隔
            Scheduler scheduler, // 调度器
            long logCleanupIntervalMs, // 定期日志过期任务执行间隔时间
            long logCleanupDefaultAgeMs, // 过期日志保留时间
            boolean needRecovery){
		this.config = config;
		this.needRecovery = needRecovery;
		this.scheduler = scheduler;
		this.logCleanupIntervalMs = logCleanupIntervalMs;
		this.logCleanupDefaultAgeMs = logCleanupDefaultAgeMs;
		this.logDir = Utils.getCanonicalFile(new File(config.getLogDir()));
		this.topicPartitionsMap =new HashMap<>();
		this.numPartitions = config.getNumPartitions();
		this.flushInterval = config.getFlushInterval();
		this.startupLatch = config.getEnableZookeeper() ? new CountDownLatch(1) : null;
		
	}
	
	public void setRollingStategy(RollingStrategy rollingStategy) {
        this.rollingStategy = rollingStategy;
    }

	@Override
	public int deleteLogs(String topic) {
		int value = 0;
		synchronized (logCreationLock) {
			Integer partitions = topicPartitionsMap.remove(topic);
			if(partitions == null)
				return value;
			ConcurrentMap<Integer,ILog> parts = logs.remove(topic);
			if (parts != null) {
				List<ILog> deleteLogs = new ArrayList<>(parts.values());
				for (ILog log : deleteLogs) {
					log.delete();
					value++;
				}
			}
			if (config.getEnableZookeeper()) {
				topicRegisterTasks.add(new TopicTask(TopicTask.TaskType.DELETE, topic));
			}
		}
		return value;
	}

	@Override
	public void load() throws IOException {
		if (this.rollingStategy == null) {
            this.rollingStategy = new FixedSizeRollingStrategy(config.getLogFileSize());
        }
		if (!logDir.exists()) {
            logger.info("创建log目录 '" + logDir.getAbsolutePath() + "'");
            logDir.mkdirs();
        }
		if (!logDir.isDirectory() || !logDir.canRead()) {
            throw new IllegalArgumentException(logDir.getAbsolutePath() + " 不可读取.");
        }
		File[] subDirs = logDir.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(final File dir, final String name) {
				return name.indexOf('-') > 0;
			}
		});
		if (subDirs != null) {
			for (File dir : subDirs) {
				 if (!dir.isDirectory()) {
					 logger.warn("文件不合法（top-0）：'" + dir.getAbsolutePath() + "'");
				 }else {
					  logger.info("Loading log " + dir.getAbsolutePath());
					  final String topicNameAndPartition = dir.getName();
					  if (-1 == topicNameAndPartition.indexOf('-')) {
	                        throw new IllegalArgumentException("不是合法的 topic-0 目录: " + dir.getAbsolutePath());
	                  }
					  int index = topicNameAndPartition.lastIndexOf('-');
					  final String topic = topicNameAndPartition.substring(0, index);
					  final int partition = Integer.valueOf(topicNameAndPartition.substring(index + 1));
					  //load log
					  ILog log = new Log(dir, partition, this.rollingStategy, flushInterval, needRecovery, maxMessageSize);
					  logs.putIfAbsent(topic,new ConcurrentSkipListMap<Integer, ILog>());
					  ConcurrentMap<Integer,ILog> parts = logs.get(topic);
					  parts.put(partition, log);
					  int configPartition = getPartition(topic);
					  //超过配置最大分区，则不进入内存
                      if (configPartition >= partition) {
                         topicPartitionsMap.put(topic, partition + 1);
                      }
				 }
			}
		}
		
		/*计划清除任务以删除旧日志*/
		if (this.scheduler != null) {
            logger.debug("start log cleaner  " + logCleanupIntervalMs + " ms");
            //60秒延迟，
            this.scheduler.scheduleWithRate(() -> {
				try {
					cleanupLogs();
				} catch (IOException e) {
					logger.error("cleanup log failed.", e);
				}
			}, 60 * 1000, logCleanupIntervalMs);
        }
		
		//zk-连接
		if (config.getEnableZookeeper()) {
			this.serverRegister = new ServerRegister(config, this);
            serverRegister.startup();
            //zk-topic任务队列启动
            TopicRegisterTask task = new TopicRegisterTask();
            task.setName("kafka.topic.register");
            task.setDaemon(true);
            task.start();
		}
	}
	
	class TopicRegisterTask extends Thread {
        @Override
        public void run() {
            registeredTaskLooply();
        }
    }
	
	/**
	 * 注册任务循环
	 */
	private void registeredTaskLooply() {
        while (!stopTopicRegisterTasks) {
            try {
                TopicTask task = topicRegisterTasks.take();
                if (task.type == TopicTask.TaskType.SHUTDOWN) break;
                serverRegister.processTask(task);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
        logger.debug("stop topic register task");
    }

	/**
	 * 首次在zk中注册此代理。
	 */
	public void startup() {
		 if (config.getEnableZookeeper()) {
			 //注册Broker
            serverRegister.registerBrokerInZk();
            //注册初始化topic
            for (String topic : logs.keySet()) {
                serverRegister.processTask(new TopicTask(TopicTask.TaskType.CREATE, topic));
            }
            startupLatch.countDown();
        }
		logger.debug("Start log flusher {} ms", config.getFlushSchedulerThreadRate());
        logFlusherScheduler.scheduleWithRate(new Runnable() {
            public void run() {
                flushAllLogs(false);
            }
        }, config.getFlushSchedulerThreadRate(), config.getFlushSchedulerThreadRate());
	}

	@Override
	public ILog getOrCreateLog(String topic, int partition) throws IOException {
		final int configPartitionNumber = getPartition(topic);
		if (partition >= configPartitionNumber) {
            throw new IOException("分区超过了配置的最大数量: " + configPartitionNumber);
        }
		//是否 新主题
		boolean hasNewTopic = false;
		ConcurrentMap<Integer,ILog> parts = getLogPool(topic, partition);
		//有并发的情况
        if (parts == null) {
           ConcurrentMap<Integer,ILog> found 
        	  = logs.putIfAbsent(topic, new ConcurrentSkipListMap<Integer, ILog>());
            if (found == null) {
                hasNewTopic = true;
            }
            parts = logs.get(topic);
        }
        ILog log = parts.get(partition);
        if (log == null) {
        	log = createLog(topic, partition);
        	ILog found = parts.putIfAbsent(partition, log);
        	if (found != null) {
        		log.close();
        		log = found;
        	} else {
        		logger.info(format("Created log  [%s-%d]  目录", topic, partition));
        		final int configPartitions = getPartition(topic);
                for (int i = 0; i < configPartitions; i++) {
                    getOrCreateLog(topic, i);
                }
				topicPartitionsMap.put(topic,configPartitions);
        	}
        }
        /** Zookeeper创建topic*/
        if (hasNewTopic && config.getEnableZookeeper()) {
            topicRegisterTasks.add(new TopicTask(TopicTask.TaskType.CREATE, topic));
        }
		return log;
	}

	@Override
	public ILog createLog(String topic, int partition) throws IOException {
		synchronized (logCreationLock) {
            File d = new File(logDir, topic + "-" + partition);
            d.mkdirs();
            return new Log(d, partition, this.rollingStategy, flushInterval, false, maxMessageSize);
        }
	}
	
	private ConcurrentMap<Integer,ILog> getLogPool(String topic, int partition) throws IOException {
        awaitStartup();
        if (topic.length() <= 0) {
            throw new IllegalArgumentException("没有找到topic:"+topic);
        }
        //
        Integer definePartition = this.topicPartitionsMap.get(topic);
        if (definePartition == null) {
            definePartition = numPartitions;
        }
        if (partition < 0 || partition >= definePartition.intValue()) {
            String msg = "越界错误  partition [%d] topic [%s], max partitions [0,%d)";
            msg = format(msg, partition, topic, definePartition.intValue() - 1);
            logger.warn(msg);
            throw new IOException(msg);
        }
        return logs.get(topic);
    }
	
	private void awaitStartup() {
        if (config.getEnableZookeeper()) {
            try {
                startupLatch.await();
            } catch (InterruptedException e) {
                logger.warn(e.getMessage(), e);
            }
        }
    }
	
	private int getPartition(String topic) {
        return this.numPartitions;
    }
	
	private void flushAllLogs(boolean force) {
		Iterator<ILog> iter = getLogIterator();
		while (iter.hasNext()) {
			ILog log = iter.next();
			try {
                boolean needFlush = force;
              //根据最后刷盘时间和当前时间对比，超过DefaultFlush(默认1秒) 就进行刷盘
                if (!needFlush) {
                    long timeSinceLastFlush = System.currentTimeMillis() - log.getLastFlushedTime();
                    Integer logFlushInterval = config.getDefaultFlushIntervalMs();
                    final String flushLogFormat = "[%s] flush interval %d, last flushed %d, need flush? %s";
                    needFlush = timeSinceLastFlush >= logFlushInterval.intValue();
                    logger.trace(String.format(flushLogFormat, log.getTopicName(), logFlushInterval,
                            log.getLastFlushedTime(), needFlush));
                }
                if (needFlush) {
                    log.flush();
                }
            } catch (IOException ioe) {
                logger.error("Error flushing topic " + log.getTopicName(), ioe);
                logger.error("Halting due to unrecoverable I/O error while flushing logs: " + ioe.getMessage(), ioe);
                Runtime.getRuntime().halt(1);
            } catch (Exception e) {
                logger.error("Error flushing topic " + log.getTopicName(), e);
            }
		}
		
	}
	private void cleanupLogs() throws IOException {
		logger.debug("Begin log cleanup...");
		int total = 0;
		//
        Iterator<ILog> iter = getLogIterator();
        long startMs = System.currentTimeMillis();
        while (iter.hasNext()) {
        	ILog log = iter.next();
        	//一,是超过预设的时间则删除，二,超过预设的大小则删除
            total += cleanupExpiredSegments(log) //清理过期日志      
            		+ cleanupSegmentsToMaintainSize(log);//文件过大清理
        }
        if (total > 0) {
            logger.warn("Log cleanup 完成. 清理[" + total + "] 文件 ，耗时 ：  " + (System.currentTimeMillis() - startMs) / 1000 + " 秒");
        } else {
            logger.trace("Log cleanup 完成. 清理[ " + total + "] 文件 ，耗时  " + (System.currentTimeMillis() - startMs) / 1000 + " 秒");
        }
	}
	
	/**
	 * 清理过期日志段
	 * @param log
	 * @return
	 * @throws IOException 
	 */
	private int cleanupExpiredSegments(ILog log) throws IOException {
		final long startMs = System.currentTimeMillis();
		Long logCleanupThresholdMS = this.logCleanupDefaultAgeMs;
		final long expiredThrshold = logCleanupThresholdMS.longValue();
        List<ILogSegment> toBeDeleted = log.markDeletedWhile(segment -> {
			// check未在expiredthrshold毫秒内修改的文件(当前时间-文件最后修改时间  > 7天)
			return startMs - segment.getFile().lastModified() > expiredThrshold;
		});
        return deleteSegments(log, toBeDeleted);
	}
	
	private int deleteSegments(ILog log,
			List<ILogSegment> segments) {
		int total = 0;
        for (ILogSegment segment : segments) {
            boolean deleted = false;
            try {
                try {
                    segment.getMessageSet().close();
                } catch (IOException e) {
                    logger.warn(e.getMessage(), e);
                }
                if (!segment.getFile().delete()) {
                    deleted = true;
                } else {
                    total += 1;
                }
            } finally {
                logger.warn(String.format("DELETE_LOG[%s] %s => %s", log.getFile().getName(), segment.getFile().getAbsolutePath(),
                        deleted));
            }
        }
        return total;
	}

	/**
	 * 下期优化
	 * @param log
	 * @return
	 */
	private int cleanupSegmentsToMaintainSize(ILog log) {
		return 0;
	}

	private Iterator<ILog> getLogIterator() {
		 return new IteratorTemplate<ILog>() {
			Iterator<ConcurrentMap<Integer, ILog>> iterator = logs.values().iterator();
			Iterator<ILog> logIter;
			@Override
			protected ILog makeNext() {
				while (true) {
                    if (logIter != null && logIter.hasNext()) {
                        return logIter.next();
                    }
                    if (!iterator.hasNext()) {
                        return allDone();
                    }
                    logIter = iterator.next().values().iterator();
	            }
			} 
		 };
	}

	/**
	 * 从给定主题中选取一个随机分区
	 */
	@Override
	public int choosePartition(String topic) {
		return random.nextInt(getPartition(topic));
	}

	@Override
	public ILog getLog(String topic, int partition) {
		ConcurrentMap<Integer,ILog> parts = null;
		try {
			 parts = getLogPool(topic, partition);
		} catch (IOException e) {
			logger.warn(e.getMessage(), e);
		}
		return parts == null ? null : parts.get(partition);
	}
	
	
	@Override
	public void close() throws IOException {
		logFlusherScheduler.shutdown();
        Iterator<ILog> iter = getLogIterator();
        while (iter.hasNext()) {
            Closer.closeQuietly(iter.next(), logger);
        }
        if (config.getEnableZookeeper()) {
            stopTopicRegisterTasks = true;
            topicRegisterTasks.add(new TopicTask(TopicTask.TaskType.SHUTDOWN, null));
            Closer.closeQuietly(serverRegister);
        }
	}

	@Override
	public Map<String, Integer> getTopicPartitionsMap() {
		return topicPartitionsMap;
	}

	@Override
	public List<Long> getOffsets(OffsetRequest offsetRequest) {
		ILog log = getLog(offsetRequest.topic, offsetRequest.partition);
        if (log != null) {
            return log.getOffsetsBefore(offsetRequest);
        }
        return ILog.EMPTY_OFFSETS;
	}

}
