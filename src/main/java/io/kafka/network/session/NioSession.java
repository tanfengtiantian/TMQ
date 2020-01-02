package io.kafka.network.session;

import java.net.InetAddress;
import java.net.InetSocketAddress;

public interface NioSession {

    /**
     * Start session
     */
    void start();

    /**
     * Return the remote end's InetSocketAddress
     *
     * @return
     */
    InetSocketAddress getRemoteSocketAddress();


    /**
     * 获取本地ip地址
     *
     * @return
     */
    InetAddress getLocalAddress();

    /**
     * Close session
     */
    void close();
}
