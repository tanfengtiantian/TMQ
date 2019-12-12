package io.kafka.network;

import io.kafka.utils.Closer;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.Selector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tf
 * @version 创建时间：2019年1月17日 上午10:02:25
 * @ClassName Selector选择基类
 */
public abstract class AbstractServerThread implements Runnable, Closeable {
    protected SelectorManager selectorManager;
    protected final CountDownLatch startupLatch = new CountDownLatch(1);
    protected final CountDownLatch shutdownLatch = new CountDownLatch(1);
    protected final AtomicBoolean alive = new AtomicBoolean(false);
    final protected Logger logger = LoggerFactory.getLogger(getClass());

    protected void closeSelector() {
        Closer.closeQuietly(selectorManager.getSelector(), logger);
    }

    public void initialSelectorManager(int plength) {
        if (this.selectorManager == null) {
            this.selectorManager = new SelectorManager(this, plength);
        }
    }

    @Override
    public void close() {
        alive.set(false);
        selectorManager.getSelector().wakeup();
        try {
            shutdownLatch.await();
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }

    protected void startupComplete() {
        alive.set(true);
        startupLatch.countDown();
    }

    protected void shutdownComplete() {
        shutdownLatch.countDown();
    }

    protected boolean isRunning() {
        return alive.get();
    }

    public void awaitStartup() throws InterruptedException {
        startupLatch.await();
    }

    public void notifyReady(){


    }
}
