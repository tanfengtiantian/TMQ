package io.kafka.mx;


import java.util.HashMap;
import java.util.Map;

/**
 * 统计管理器
 *
 * @author tf
 * @Date 2019-7-19
 *
 */
public enum StatsManager {
    INSTANCE;

    /**
     * 程序启动时间
     */
    private long startupTimestamp = System.currentTimeMillis();

    private Map<Class, String> bootedServices = new HashMap<>();

    public Map<Class, String> getBootedServices(){
        return bootedServices;
    }


    public long getStartupTimestamp() {
        return this.startupTimestamp;
    }

    /**
     * 获取topic消息数量
     * @param topic
     * @return
     */
    public long getBrokerTopicMessagesIn(String topic) { return BrokerTopicStat.getBrokerTopicStat(topic).getMessagesIn(); }

    /**
     * 获取topic消息byte总量
     * @param topic
     * @return
     */
    public long getBrokerTopicBytesIn(String topic) { return BrokerTopicStat.getBrokerTopicStat(topic).getBytesIn(); }
}
