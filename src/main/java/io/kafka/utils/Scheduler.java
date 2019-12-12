package io.kafka.utils;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tf
 * @version 创建时间：2019年1月15日 上午9:46:19
 * @ClassName 定时处理作业程序
 */
public class Scheduler {
	
	final private Logger logger = LoggerFactory.getLogger(getClass());

    final AtomicLong threadId = new AtomicLong(0);

    final ScheduledThreadPoolExecutor executor;

    final String baseThreadName;

    public Scheduler(int numThreads, final String baseThreadName, final boolean isDaemon) {
        this.baseThreadName = baseThreadName;
        executor = new ScheduledThreadPoolExecutor(numThreads, new ThreadFactory() {

            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, baseThreadName + threadId.getAndIncrement());
                t.setDaemon(isDaemon);
                return t;
            }
        });
        executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    }

    /**
     * @param command 执行线程
     * @param delayMs 初始化延时(毫秒)
     * @param periodMs 前一次执行结束到下一次执行开始的间隔时间(毫秒)
     * @return
     */
    public ScheduledFuture<?> scheduleWithRate(Runnable command, long delayMs, long periodMs) {
        return executor.scheduleAtFixedRate(command, delayMs, periodMs, TimeUnit.MILLISECONDS);
    }

    public void shutdownNow() {
        executor.shutdownNow();
        logger.info("ShutdownNow scheduler {} with {} threads.", baseThreadName, threadId.get());
    }

    public void shutdown() {
        executor.shutdown();
        logger.info("Shutdown scheduler {} with {} threads.", baseThreadName, threadId.get());
    }
}
