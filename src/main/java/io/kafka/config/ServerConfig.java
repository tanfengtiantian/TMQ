package io.kafka.config;

import io.kafka.message.Message;
import static io.kafka.utils.Utils.*;
import java.util.Properties;

/**
 * @author tf
 * @version 创建时间：2018年12月30日 下午10:14:46
 * @ClassName 启动配置类
 */
public class ServerConfig {

	private Properties props;
	
	public ServerConfig(Properties props) {
        this.props = props;
    }
    public Properties getProps() { return this.props; }
	//*********************************************************************zookeeper配置****************************************
	public boolean getEnableZookeeper() {
        return getBoolean(props, "enable.zookeeper", false);
    }

    public int getSlave(){return getIntInRange(props, "slaveId", -1,-1,Integer.MAX_VALUE);}

    public boolean isSlave(){return getSlave() >= 0;}
	
	public int getBrokerId() {
		return getIntInRange(props, "brokerid", -1, 0, Integer.MAX_VALUE);
	}
	
	public String getZkConnect() {
        return getString(props, "zk.connect", null);
    }
	
	public int getZkSessionTimeoutMs() {
		return getInt(props, "zk.sessiontimeout.ms", 6000);
	}
	
	public int getZkConnectionTimeoutMs() {
		return getInt(props, "zk.connectiontimeout.ms", 6000);
	}
	
	public String getHostName() {
		return getString(props, "hostname", null);
	}

	public String[] getPlugins(){return getStrings(props, "plugins", null);}
	
	public boolean isTopicAutoCreated() {
		return getBoolean(props,"topic.autocreated",true);
	}
	
	//*********************************************************************zookeeper配置****************************************
	
	//*********************************************************************log配置****************************************
	/**
     * 单个日志文件的最大大小
     * @return size 
     */
    public int getLogFileSize() {
        return getIntInRange(props, "log.file.size", 1 * 1024 * 1024 * 1024, Message.MinHeaderSize, Integer.MAX_VALUE);
    }
    
    public String getLogDir() {
        return getString(props, "log.dir");
    }
    /**
     * 每个主题的默认日志分区数
     * @return
     */
	public int getNumPartitions() {
		return getIntInRange(props, "num.partitions", 1, 1, Integer.MAX_VALUE);
	}
	
	/**
	 * 强制将数据刷新到磁盘之前要接受的消息数
	 * @return
	 */
	public int getFlushInterval() {
	    return getIntInRange(props, "log.flush.interval", 500, 1, Integer.MAX_VALUE);
	}
	
	/**
	 * 定时清理日志时间间隔
	 * @return
	 */
	public int getLogCleanupIntervalMinutes() {
        return getIntInRange(props, "log.cleanup.interval.mins", 10, 1, Integer.MAX_VALUE);
    }
	
	/**
	 * 日志文件保存最大过期时间
	 * @return
	 */
	public int getLogRetentionHours() {
        return getIntInRange(props, "log.retention.hours", 24 * 7, 1, Integer.MAX_VALUE);
    }
	/**
	 * 定期刷盘任务时间间隔
	 * @return
	 */
	public int getFlushSchedulerThreadRate() {
	    return getInt(props, "log.default.flush.scheduler.interval.ms", 3000);
	}
	
	/**
	 * 任何主题中的消息保存在内存中的最长时间（毫秒）
	 * 刷新到磁盘之前
	 * @return 刷新规则
	 */
	public int getDefaultFlushIntervalMs() {
        return getInt(props, "log.default.flush.interval.ms", getFlushSchedulerThreadRate());
    }
	/**
	 * 最大处理器线程数量
	 * 2的幂次
	 * @return
	 */
	public int getNumThreads() {
		return tableSizeFor(
				getIntInRange(props, "num.threads", Runtime.getRuntime().availableProcessors(), 1 , Integer.MAX_VALUE)
		);
	}

	private static final int MAXIMUM_CAPACITY = 1 << 30;

	/**
	 * 返回给定所需容量的2的幂次。
	 * @param c
	 * @return
	 */
	private final int tableSizeFor(int c) {
		int n = c - 1;
		n |= n >>> 1;
		n |= n >>> 2;
		n |= n >>> 4;
		n |= n >>> 8;
		n |= n >>> 16;
		return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
	}
	//*********************************************************************log配置****************************************
	
	//*********************************************************************server-socket配置****************************************
	/**
	 * 启动端口
	 * @return
	 */
	public int getPort() {
		return getInt(props, "port", 9092);
	}

	/**
	 * socket请求的最大字节数。为了防止内存溢出
	 * @return
	 */
	public int getMaxSocketRequestSize() {
		return getIntInRange(props, "max.socket.request.bytes", 100 * 1024 * 1024, 1, Integer.MAX_VALUE);
	}
	/**
	 * socket的发送缓冲区(SO_SNDBUF)
	 * @return
	 */
	public int getSocketSendBuffer() {
		 return getInt(props, "socket.send.buffer", 100 * 1024);
	}
	/**
	 * socket的接收缓冲区 (SO_RCVBUF)
	 * @return
	 */
	public int getSocketReceiveBuffer() {
		return getInt(props, "socket.receive.buffer", 100 * 1024);
	}
	/**
	 * socket最大链接数
	 * @return
	 */
	public int getMaxConnections() {
		return getInt(props, "max.connections", 10000);
	}

	/**
	 * Socket SO_LINGER选项
	 */
	private boolean soLinger = true;

	public boolean isSoLinger() {
		return this.soLinger;
	}

	/**
	 * linger值
	 */
	private int linger = 0;

	public int getLinger() {
		return this.linger;
	}

	/**
	 * socket SO_KEEPALIVE选项
	 */
	private boolean keepAlive = true;

	public boolean isKeepAlive() {
		return this.keepAlive;
	}


	/**
	 * 是否禁止Nagle算法，默认为true 禁止
	 */
	private boolean tcpNoDelay = true;

	public boolean isTcpNoDelay() {
		return this.tcpNoDelay;
	}

	/**
	 * 是否重用端口
	 */
	private boolean reuseAddr = true;
	public boolean isReuseAddr() {
		return this.reuseAddr;
	}

	/**
	 * 连接读缓冲区大小
	 */
	private int readBufferSize = 16 * 1024;
	/**
	 * session默认buffer
	 * @return
	 */
    public int getSessionReadBufferSize() {
    	return readBufferSize;
    }


	public int getReadThreadCount() {
    	return 0;
	}

	public int getWriteThreadCount() {
    	return 0;
	}

	public int getDispatchMessageThreadCount() {
    	return 0;
	}
	//*********************************************************************server配置****************************************
	
}
