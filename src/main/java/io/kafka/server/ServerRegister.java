package io.kafka.server;

import io.kafka.cluster.Broker;
import io.kafka.config.ServerConfig;
import io.kafka.log.ILogManager;
import io.kafka.utils.zookeeper.ZkUtils;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.zkclient.IZkStateListener;
import com.github.zkclient.ZkClient;
import com.github.zkclient.exception.ZkNodeExistsException;

/**
 * @author tf
 * @version 创建时间：2019年1月29日 下午5:11:41
 * @ClassName Register(服务注册)
 */
public class ServerRegister implements IZkStateListener, Closeable {
	
	private static final Logger logger = LoggerFactory.getLogger(ServerRegister.class);
	
	private final ServerConfig config;

    private final ILogManager logManager;
    
    private final String brokerIdPath;
	
    private ZkClient zkClient;
    
    private final Object lock = new Object();
    
    private Set<String> topics = new LinkedHashSet<String>();

	public ServerRegister(ServerConfig config, ILogManager logManager) {
		this.config = config;
        this.logManager = logManager;
        ///brokers/ids
        this.brokerIdPath = ZkUtils.BrokerIdsPath + "/" + config.getBrokerId();
	}
	
	public void startup() {
		logger.info("connecting to zookeeper: " + config.getZkConnect());
        zkClient = new ZkClient(config.getZkConnect(), config.getZkSessionTimeoutMs(),
                config.getZkConnectionTimeoutMs());
        zkClient.subscribeStateChanges(this);
	}
	
	public void registerBrokerInZk() {
		logger.info("Register broker " + brokerIdPath);
		//获取HostName.如未设置.取系统HostName
        String hostname = config.getHostName();
        if (hostname == null) {
            try {
                hostname = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                throw new RuntimeException("未设置HostName");
            }
        }
        //
        final String creatorId = hostname + "-" + System.currentTimeMillis();
        final Broker broker = new Broker(config.getBrokerId(), creatorId, hostname, config.getPort(),config.isTopicAutoCreated(), config.isSlave());
        try {
            ZkUtils.createEphemeralPathExpectConflict(zkClient, brokerIdPath, broker.getZKString());
        } catch (ZkNodeExistsException e) {
            String oldServerInfo = ZkUtils.readDataMaybeNull(zkClient, brokerIdPath);
            logger.error("oldServerInfo:[{}],error:[{}]",oldServerInfo,e.getMessage());
            throw new RuntimeException(e.getMessage());
        }
        //
        logger.info("Register broker [{}] succeeded(成功) to [{}]", brokerIdPath, broker);
	}
	
	public void processTask(TopicTask task) {
	    //  /brokers/topics/tanfeng
		final String topicPath = ZkUtils.BrokerTopicsPath + "/" + task.topic;
		// /brokers/topics/tanfeng/1
        final String brokerTopicPath = ZkUtils.BrokerTopicsPath + "/" + task.topic + "/" + config.getBrokerId();
        synchronized (lock) {
            switch (task.type) {
                case DELETE:
                    topics.remove(task.topic);
                    ZkUtils.deletePath(zkClient, brokerTopicPath);
                    List<String> brokers = ZkUtils.getChildrenParentMayNotExist(zkClient, topicPath);
                    //如果/brokers/topics/tanfeng 无子节点 删除
                    if (brokers != null && brokers.size() == 0) {
                        ZkUtils.deletePath(zkClient, topicPath);
                    }
                    //删除消费offset
                    List<String> consumers = ZkUtils.getChildrenParentMayNotExist(zkClient, ZkUtils.ConsumersPath);
                    if (consumers != null && consumers.size() >= 0) {
                        consumers.forEach(consumer -> {
                            //移除offset
                            String consumersOffsetTopicPath = ZkUtils.ConsumersPath + "/" + consumer + "/offsets/" + task.topic;
                            List<String> consumersOffsets = ZkUtils.getChildrenParentMayNotExist(zkClient, consumersOffsetTopicPath);
                            consumersOffsets.forEach(offset->{
                                String offsetPartitions = consumersOffsetTopicPath + "/" + offset;
                                ZkUtils.deletePath(zkClient, offsetPartitions);

                            });
                            consumersOffsets = ZkUtils.getChildrenParentMayNotExist(zkClient, consumersOffsetTopicPath);
                            if (consumersOffsets != null && consumersOffsets.size() == 0) {
                                ZkUtils.deletePath(zkClient, consumersOffsetTopicPath);
                            }

                            //移除owners
                            String consumersOwnersTopicPath = ZkUtils.ConsumersPath + "/" + consumer + "/owners/" + task.topic;
                            List<String> consumersOwners = ZkUtils.getChildrenParentMayNotExist(zkClient, consumersOwnersTopicPath);
                            consumersOwners.forEach(owners->{
                                String ownerPartitions = consumersOwnersTopicPath + "/" + owners;
                                ZkUtils.deletePath(zkClient, ownerPartitions);
                            });
                            consumersOwners = ZkUtils.getChildrenParentMayNotExist(zkClient, consumersOwnersTopicPath);
                            if (consumersOwners != null && consumersOwners.size() == 0) {
                                ZkUtils.deletePath(zkClient, consumersOwnersTopicPath);
                            }

                        });
                    }
                    break;
                case CREATE:
                    topics.add(task.topic);
                    ZkUtils.createEphemeralPathExpectConflict(zkClient, brokerTopicPath, "" + getPartitions(task.topic));
                    break;
                case ENLARGE:
                    ZkUtils.deletePath(zkClient, brokerTopicPath);
                    ZkUtils.createEphemeralPathExpectConflict(zkClient, brokerTopicPath, "" + getPartitions(task.topic));
                    break;
                default:
                    logger.error("unknow(未知) task: " + task);
                    break;
            }
        }
	}
	
	private int getPartitions(String topic) {
        Integer numParts = logManager.getTopicPartitionsMap().get(topic);
        return numParts == null ? config.getNumPartitions() : numParts.intValue();
    }

	@Override
	public void handleStateChanged(KeeperState state) throws Exception {
		logger.info("handleStateChanged " + config.getBrokerId());
		
	}

	@Override
	public void handleNewSession() throws Exception {
		logger.info("handleNewSession " + config.getBrokerId());
		
	}
	
	@Override
	public void close() throws IOException {
		if (zkClient != null) {
            logger.info("close zookeeper client...");
            zkClient.close();
        }
	}
}
