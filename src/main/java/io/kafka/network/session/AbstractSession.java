package io.kafka.network.session;

import io.kafka.utils.nettyloc.PooledByteBufAllocator;

import java.net.InetAddress;
import java.net.InetSocketAddress;

public class AbstractSession implements NioSession {

    protected PooledByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;

    protected volatile long sessionIdleTimeout;

    protected volatile long sessionTimeout;

    @Override
    public synchronized void start() {

    }

    @Override
    public InetSocketAddress getRemoteSocketAddress() {
        return null;
    }

    @Override
    public InetAddress getLocalAddress() {
        return null;
    }

    @Override
    public void close() {

    }
}
