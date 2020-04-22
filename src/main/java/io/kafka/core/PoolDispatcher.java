package io.kafka.core;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 线程池派发器
 *
 */
public class PoolDispatcher implements Dispatcher {
    public static final int POOL_QUEUE_SIZE_FACTOR = 1000;
    public static final float MAX_POOL_SIZE_FACTOR = 1.25f;
    private final ThreadPoolExecutor threadPool;


    public PoolDispatcher(final int poolSize) {
        this(poolSize, 60, TimeUnit.SECONDS, new ThreadPoolExecutor.AbortPolicy());
    }


    public PoolDispatcher(final int poolSize, final long keepAliveTime, final TimeUnit unit,
                          final RejectedExecutionHandler rejectedExecutionHandler) {
        this.threadPool =
                new ThreadPoolExecutor(poolSize, (int) (MAX_POOL_SIZE_FACTOR * poolSize), keepAliveTime, unit,
                        new ArrayBlockingQueue<Runnable>(poolSize * POOL_QUEUE_SIZE_FACTOR), new WorkerThreadFactory());
        this.threadPool.setRejectedExecutionHandler(rejectedExecutionHandler);
    }


    public PoolDispatcher(final int poolSize, final long keepAliveTime, final TimeUnit unit, final String prefix,
                          final RejectedExecutionHandler rejectedExecutionHandler) {
        this.threadPool =
                new ThreadPoolExecutor(poolSize, (int) (MAX_POOL_SIZE_FACTOR * poolSize), keepAliveTime, unit,
                        new ArrayBlockingQueue<Runnable>(poolSize * POOL_QUEUE_SIZE_FACTOR),
                        new WorkerThreadFactory(prefix));
        this.threadPool.setRejectedExecutionHandler(rejectedExecutionHandler);
    }


    public final void dispatch(final Runnable r) {
        if (!this.threadPool.isShutdown()) {
            this.threadPool.execute(r);
        }
    }


    public void stop() {
        this.threadPool.shutdown();
        try {
            this.threadPool.awaitTermination(1000, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

