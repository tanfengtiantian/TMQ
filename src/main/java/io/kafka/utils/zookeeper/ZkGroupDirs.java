package io.kafka.utils.zookeeper;

/**
 * @author tf
 * @see ZookeeperConsumerConnector
 */
public class ZkGroupDirs {

	/** group name */
    public final String group;

    /** '/consumers' */
    public final String consumerDir;

    /** '/consumers/group/' */
    public final String consumerGroupDir;

    /** '/consumers/group/ids' */
    public final String consumerRegistryDir;

    public ZkGroupDirs(String group) {
        super();
        this.group = group;
        this.consumerDir = ZkUtils.ConsumersPath;
        this.consumerGroupDir = this.consumerDir + "/" + group;
        this.consumerRegistryDir = this.consumerGroupDir + "/ids";
    }

}
