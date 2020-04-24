package io.kafka.network.session;

import io.kafka.api.RequestKeys;
import io.kafka.network.receive.Receive;
import io.kafka.network.request.Request;
import io.kafka.network.send.Send;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.Selector;

public interface NioSession {
    /**
     * Start session
     */
    void start();
    /**
     * set session Receive
     */
    void setReceive(int maxRequestSize);
    /**
     *  Receive
     * @return
     */
    Receive getReceive();
    /**
     *  Receive
     * @return
     */
    void resultSend(Send send);

    Send getSend();
    /**
     * 注册事件
     * @param selector
     */
    void registerSession(Selector selector);
    /**
     * Return the remote end's InetSocketAddress
     *
     * @return
     */
    InetSocketAddress getRemoteSocketAddress();
    /**
     *session 创建
     */
    void onCreated();
    /**
     *session 启动
     */
    void onStarted();
    /**
     *session 接收数据
     */
    void onMessageReceived(RequestKeys requestType, Request request);
    /**
     *session 发送数据
     */
    void onMessageSent(Send msg);
    /**
     * 获取本地ip地址
     *
     * @return
     */
    InetAddress getLocalAddress();

    /**
     * Check if session is idle
     *
     * @return
     */
    boolean isIdle();

    /**
     * Close session
     */
    void close();

}
