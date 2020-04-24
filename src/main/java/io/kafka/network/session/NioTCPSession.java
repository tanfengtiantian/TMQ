package io.kafka.network.session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SocketChannel;
import java.util.List;

public class NioTCPSession extends AbstractSession {

    protected final Logger logger = LoggerFactory.getLogger(NioTCPSession.class);

    private final int initialReadBufferSize;

    public NioTCPSession(final List<SessionHandler> handlers, final SelectableChannel sc, final int readRecvBufferSize) {
        super(handlers);
        selectableChannel = sc;
        if (this.selectableChannel != null && this.getRemoteSocketAddress() != null) {
            this.loopback = this.getRemoteSocketAddress().getAddress().isLoopbackAddress();
        }
        this.setReadBuffer(ByteBuffer.allocate(readRecvBufferSize));
        this.initialReadBufferSize = this.readBuffer.capacity();
        onCreated();
        try {
            //如果字节大小超过initialReadBufferSize 则扩大buffer=recvBufferSize
            this.recvBufferSize = ((SocketChannel) this.selectableChannel).socket().getReceiveBufferSize();
        }
        catch (final Exception e) {
            logger.error("Get socket receive buffer size failed", e);
        }
    }

    public InetSocketAddress getRemoteSocketAddress() {
        if (this.remoteAddress == null) {
            if (this.selectableChannel instanceof SocketChannel) {
                this.remoteAddress =
                        (InetSocketAddress) ((SocketChannel) this.selectableChannel).socket().getRemoteSocketAddress();
            }
        }
        return this.remoteAddress;
    }
}
