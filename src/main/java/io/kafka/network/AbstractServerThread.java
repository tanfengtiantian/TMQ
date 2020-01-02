package io.kafka.network;

import io.kafka.config.ServerConfig;
import io.kafka.core.ControllerLifeCycle;
import io.kafka.utils.Closer;
import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tf
 * @version 创建时间：2019年1月17日 上午10:02:25
 * @ClassName ServerThread基类
 */
public abstract class AbstractServerThread implements Runnable, Closeable {
    protected SelectorManager selectorManager;
    protected final CountDownLatch startupLatch = new CountDownLatch(1);
    protected final CountDownLatch shutdownLatch = new CountDownLatch(1);
    protected final AtomicBoolean alive = new AtomicBoolean(false);
    final protected Logger logger = LoggerFactory.getLogger(getClass());
    protected int soTimeout;
    protected boolean soLingerOn = false;
    protected ServerConfig serverConfig;
    protected void closeSelector() {
        Closer.closeQuietly(selectorManager.getSelector(), logger);
    }

    public AbstractServerThread(ServerConfig serverConfig){
        this.serverConfig = serverConfig;
    }
    public SelectorManager initialSelectorManager(ControllerLifeCycle controllerLifeCycle, int plength) {
        if (this.selectorManager == null) {
            this.selectorManager = new SelectorManager(controllerLifeCycle, plength);
        }
        return selectorManager;
    }

    protected void setSelectorManager(SelectorManager selectorManager) {
        this.selectorManager = selectorManager;
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

    /**
     * Socket options
     */
    protected Map<SocketOption<?>, Object> socketOptions = new HashMap<SocketOption<?>, Object>();

    public void setSocketOptions(final Map<SocketOption<?>, Object> socketOptions) {
        if (socketOptions == null) {
            throw new NullPointerException("Null socketOptions");
        }
        this.socketOptions = socketOptions;
    }

    protected final void configureSocketChannel(final SocketChannel sc) throws IOException {

        sc.socket().setSoTimeout(this.soTimeout);
        //设置非堵塞
        sc.configureBlocking(false);
        // 重复使用local address的，注意，这里说的是local address，即ip加端口组成的本地地址，
        // 也就是说，两个本地地址，如果有任意ip或端口部分不一样，它们本身就是可以共存的，不需要使用这个参数。
        if (this.socketOptions.get(StandardSocketOption.SO_REUSEADDR) != null) {
            sc.socket().setReuseAddress(
                    StandardSocketOption.SO_REUSEADDR.type()
                            .cast(this.socketOptions.get(StandardSocketOption.SO_REUSEADDR)));
        }
        //设置发送数据最大Buffer
        if (this.socketOptions.get(StandardSocketOption.SO_SNDBUF) != null) {
            sc.socket().setSendBufferSize(
                    StandardSocketOption.SO_SNDBUF.type().cast(this.socketOptions.get(StandardSocketOption.SO_SNDBUF)));
        }
        //设置接收数据最大Buffer
        if (this.socketOptions.get(StandardSocketOption.SO_RCVBUF) != null) {
            sc.socket().setReceiveBufferSize(
                    StandardSocketOption.SO_RCVBUF.type().cast(this.socketOptions.get(StandardSocketOption.SO_RCVBUF)));

        }
        //如果这个连接上双方任意方向在2小时之内没有发送过数据，那么tcp会自动发送一个探测探测包给对方，这种探测包对方是必须回应的，回应结果有三种：
        //1.正常ack，继续保持连接；
        //2.对方响应rst信号，双方重新连接。
        //3.对方无响应。
        //这里说的两小时，其实是依赖于系统配置，在linux系统中（windows在注册表中，可以自行查询资料），tcp的keepalive参数
        //net.ipv4.tcp_keepalive_intvl = 75 （发送探测包的周期，前提是当前连接一直没有数据交互，才会以该频率进行发送探测包，如果中途有数据交互，则会重新计时tcp_keepalive_time，到达规定时间没有数据交互，才会重新以该频率发送探测包）
        //net.ipv4.tcp_keepalive_probes = 9  （探测失败的重试次数，发送探测包达次数限制对方依旧没有回应，则关闭自己这端的连接）
        //net.ipv4.tcp_keepalive_time = 7200 （空闲多长时间，则发送探测包）
        if (this.socketOptions.get(StandardSocketOption.SO_KEEPALIVE) != null) {
            sc.socket().setKeepAlive(
                    StandardSocketOption.SO_KEEPALIVE.type()
                            .cast(this.socketOptions.get(StandardSocketOption.SO_KEEPALIVE)));
        }
        //socket.close(),数据发送策略
        if (this.socketOptions.get(StandardSocketOption.SO_LINGER) != null) {
            sc.socket().setSoLinger(this.soLingerOn,
                    StandardSocketOption.SO_LINGER.type().cast(this.socketOptions.get(StandardSocketOption.SO_LINGER)));
        }
        //是否禁止Nagle算法
        if (this.socketOptions.get(StandardSocketOption.TCP_NODELAY) != null) {
            sc.socket().setTcpNoDelay(
                    StandardSocketOption.TCP_NODELAY.type().cast(this.socketOptions.get(StandardSocketOption.TCP_NODELAY)));
        }

        logger.info("OP_ACCEPT请求->来自Client->{}",sc.getRemoteAddress());
    }

    protected Map<SocketOption<?>, Object> getSocketOptionsFromConfig(final ServerConfig config) {
        final Map<SocketOption<?>, Object> result = new HashMap<>();

        result.put(StandardSocketOption.SO_SNDBUF, config.getSocketSendBuffer());
        result.put(StandardSocketOption.SO_RCVBUF, config.getSocketReceiveBuffer());
        result.put(StandardSocketOption.SO_KEEPALIVE, config.isKeepAlive());
        if (config.isSoLinger()) {
            result.put(StandardSocketOption.SO_LINGER, config.getLinger());
        }
        result.put(StandardSocketOption.TCP_NODELAY, config.isTcpNoDelay());
        result.put(StandardSocketOption.SO_REUSEADDR, config.isReuseAddr());

        return result;
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
}
