package io.kafka.utils.zookeeper;



/**
 * @author tf
 * @see ZookeeperConsumerConnector
 */
public class ZkGroupTopicDirs extends ZkGroupDirs {

    /** topic name */
    public final String topic;

    /** '/consumers/&lt;group&gt;/offsets/&lt;topic&gt;' */
    public final String consumerOffsetDir;

    /** '/consumers/&lt;group&gt;/owners/&lt;topic&gt;' */
    public final String consumerOwnerDir;

    public ZkGroupTopicDirs(String group, String topic) {
        super(group);
        this.topic = topic;
        this.consumerOffsetDir = consumerGroupDir + "/offsets/" + topic;
        this.consumerOwnerDir = consumerGroupDir + "/owners/" + topic;
    }
}
