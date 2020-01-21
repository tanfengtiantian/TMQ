package io.kafka.network;

import io.kafka.core.ControllerLifeCycle;
import java.io.IOException;
import java.nio.channels.Selector;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 服务启动线程同步器
 */
public class ServerSync {

    private ControllerLifeCycle controllerLifeCycle;
    private int plength = -1;
    /**
     * Processor准备就绪的个数
     */
    private final AtomicInteger processorReadyCount = new AtomicInteger(0);

    public ServerSync(ControllerLifeCycle controllerLifeCycle, int plength) {
        this.controllerLifeCycle = controllerLifeCycle;
        this.plength = plength;
    }


    public void awaitReady() {
        synchronized (this) {
            while (this.processorReadyCount.get() != this.plength) {
                try {
                    this.wait(1000);
                }
                catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();// reset interrupt status
                }
            }
        }
    }
    public synchronized void notifyReady() {
        this.processorReadyCount.getAndIncrement();
        if (this.processorReadyCount.get() == this.plength) {
            //加入事件模型
            this.controllerLifeCycle.notifyReady();
            this.notifyAll();
        }
    }
}
