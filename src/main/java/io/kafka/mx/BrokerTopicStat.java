package io.kafka.mx;

import io.kafka.utils.Utils;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class BrokerTopicStat implements BrokerTopicStatMBean {


    static class BrokerTopicStatHolder {

        static BrokerTopicStat allTopicState = new BrokerTopicStat();

        static Map<String, BrokerTopicStat> states = new HashMap<String, BrokerTopicStat>();
        static {
            allTopicState.mBeanName = "kafka:type=kafka.BrokerAllTopicStat";
            Utils.registerMBean(allTopicState);
        }
    }

    public static BrokerTopicStat getBrokerAllTopicStat() {
        return BrokerTopicStatHolder.allTopicState;
    }

    public static BrokerTopicStat getBrokerTopicStat(String topic) {
        return getInstance(topic);
    }

    public static BrokerTopicStat getInstance(String topic) {
        BrokerTopicStat state = BrokerTopicStatHolder.states.get(topic);
        if (state == null) {
            state = new BrokerTopicStat();
            state.mBeanName = "kafka:type=kafka.BrokerTopicStat." + topic;
            if (null == BrokerTopicStatHolder.states.putIfAbsent(topic, state)) {
                Utils.registerMBean(state);
            }
            state = BrokerTopicStatHolder.states.get(topic);
        }
        return state;
    }

    private String mBeanName;
    //
    private final AtomicLong numCumulatedBytesIn = new AtomicLong(0);

    private final AtomicLong numCumulatedBytesOut = new AtomicLong(0);

    private final AtomicLong numCumulatedFailedFetchRequests = new AtomicLong(0);

    private final AtomicLong numCumulatedFailedProduceRequests = new AtomicLong(0);

    private final AtomicLong numCumulatedMessagesIn = new AtomicLong(0);
    @Override
    public long getMessagesIn()  {
        return numCumulatedMessagesIn.get();
    }

    @Override
    public long getBytesIn()   {
        return numCumulatedBytesIn.get();
    }

    @Override
    public long getBytesOut() { return numCumulatedBytesOut.get();
    }

    @Override
    public long getFailedProduceRequest()    {
        return numCumulatedFailedProduceRequests.get();
    }

    @Override
    public long getFailedFetchRequest()    {
        return numCumulatedFailedFetchRequests.get();
    }

    @Override
    public String getMbeanName() {
        return mBeanName;
    }


    public void recordBytesIn(long nBytes) {
        numCumulatedBytesIn.getAndAdd(nBytes);
    }

    public void recordBytesOut(long nBytes) {
        numCumulatedBytesOut.getAndAdd(nBytes);
    }

    public void recordFailedFetchRequest() {
        numCumulatedFailedFetchRequests.getAndIncrement();
    }

    public void recordFailedProduceRequest() {
        numCumulatedFailedProduceRequests.getAndIncrement();
    }

    public void recordMessagesIn(int nMessages) {
        numCumulatedMessagesIn.getAndAdd(nMessages);
    }

}
