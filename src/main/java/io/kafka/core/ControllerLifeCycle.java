package io.kafka.core;

/**
 * Controller生命周期接口
 */
public interface ControllerLifeCycle {
    /**
     * 网络通知就绪
     */
    void notifyReady();

    /**
     * 网络已启动
     */
    void notifyStarted();

    /**
     * 网络停止
     */
    void notifyStopped();
}
