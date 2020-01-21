package io.kafka.network.session;

import io.kafka.api.RequestKeys;
import io.kafka.network.receive.BoundedByteBufferReceive;
import io.kafka.network.receive.Receive;
import io.kafka.network.send.Send;
import io.kafka.utils.nettyloc.PooledByteBufAllocator;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class AbstractSession implements NioSession {

    protected PooledByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;//池化内存
    protected List<SessionHandler> handlers;
    /**是否路由器**/
    protected boolean loopback;
    protected SelectableChannel selectableChannel;
    protected InetSocketAddress remoteAddress;
    protected volatile long sessionIdleTimeout;

    protected int recvBufferSize = 16 * 1024;

    protected AtomicLong lastOperationTimeStamp = new AtomicLong(0);

    protected Receive request = null;

    protected Send send = null;

    protected volatile long sessionTimeout;

    public AbstractSession(List<SessionHandler> handlers) {
        this.handlers = handlers;
    }
    @Override
    public synchronized void start() {
        this.onStarted();
    }

    @Override
    public void setReceive(int maxRequestSize) {
        this.recvBufferSize = maxRequestSize;
    }

    @Override
    public Receive getReceive() {
        if(request == null) {
            synchronized (this) {
                request = new BoundedByteBufferReceive(recvBufferSize,allocator);
            }
        }
        return request;
    }

    @Override
    public void resultSend(Send send) {
        this.send = send;
    }

    @Override
    public Send getSend() {
        return send;
    }

    @Override
    public void registerSession(Selector selector) {
        final SelectionKey key = this.selectableChannel.keyFor(selector);
        if (key != null && key.isValid()) {
            this.interestRead(key);
        }else {
            try {
                this.selectableChannel.register(selector, SelectionKey.OP_READ, this);
            }
            catch (final ClosedChannelException e) {
                // ignore
            }
            catch (final CancelledKeyException e) {
                // ignore
            }
        }
    }

    private void interestRead(SelectionKey key) {
        if (key.attachment() == null) {
            key.attach(this);
        }
        key.interestOps(key.interestOps() | SelectionKey.OP_READ);
    }
    @Override
    public void onCreated() {
        try {
            handlers.forEach((handler) -> {
                handler.onSessionCreated(this);
            });
        }
        catch (final Throwable e) {
            this.onException(e);
        }
    }
    @Override
    public void onStarted() {
        try {
            handlers.forEach((handler) -> {
                handler.onSessionStarted(this);
            });
        }
        catch (final Throwable e) {
            this.onException(e);
        }
    }
    @Override
    public void onMessageReceived(RequestKeys requestType, Receive receive) {
        try {
            handlers.forEach((handler) -> {
                handler.onMessageReceived(this,requestType,receive);
            });
        }
        catch (final Throwable e) {
            this.onException(e);
        }
    }
    @Override
    public void onMessageSent(Send msg) {
        try {
            handlers.forEach((handler) -> {
                handler.onMessageSent(this,msg);
            });
        }
        catch (final Throwable e) {
            this.onException(e);
        }
    }

    public void onException(final Throwable e) {
        handlers.forEach((handler) -> {
            handler.onExceptionCaught(this,e);
        });
    }
    protected void onClosed() {
        try {
            handlers.forEach((handler) -> {
                handler.onSessionClosed(this);
            });
        }
        catch (final Throwable e) {
            this.onException(e);
        }
    }
    @Override
    public InetSocketAddress getRemoteSocketAddress() {
        return remoteAddress;
    }

    @Override
    public InetAddress getLocalAddress() {
        return ((SocketChannel) this.selectableChannel).socket().getLocalAddress();
    }

    @Override
    public boolean isIdle() {
        return false;
    }


    @Override
    public void close() {
        onClosed();
    }
}
