package io.kafka.network;

import java.io.IOException;
import java.nio.channels.Selector;

/**
 * 选择器管理
 */
public class SelectorManager {

    private Selector selector;
    /**
     * 连接器
     */
    private AbstractServerThread serverThread;
    private int plength = -1;
    /**
     * Reactor准备就绪的个数
     */
    private int reactorReadyCount;

    public SelectorManager(AbstractServerThread serverThread, int plength) {
        this.serverThread = serverThread;
        this.plength = plength;
    }
    /**
     * @return the selector
     */
    public Selector getSelector() {
        if (selector == null) {
            try {
                selector = Selector.open();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return selector;
    }

    synchronized void notifyReady() {
        this.reactorReadyCount++;
        if (this.reactorReadyCount == this.plength) {
            //加入事件模型
            this.serverThread.notifyReady();
            this.notifyAll();
        }
    }
}
