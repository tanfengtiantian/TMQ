package io.kafka.mx;

public interface BrokerTopicStatMBean extends IMBeanName{

    /**
     * 消息接收数量
     * @return
     */
    long getMessagesIn();

    /**
     * 消息总量
     * @return
     */
    long getBytesIn();

    long getBytesOut();

    long getFailedProduceRequest();

    long getFailedFetchRequest();
}
