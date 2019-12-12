package io.kafka.cluster;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author tf
 * @version 创建时间：2019年1月31日 下午6:23:26
 * @ClassName Cluster
 */
public class Cluster {

    private final Map<Integer, Broker> brokers = new LinkedHashMap<Integer, Broker>();

    public Cluster() {}
    public Cluster(List<Broker> brokerList) {
        for (Broker broker : brokerList) {
            brokers.put(broker.id, broker);
        }
    }

    public Broker getBroker(Integer id) {
        return brokers.get(id);
    }

    public void add(Broker broker) {
        brokers.put(broker.id, broker);
    }

    public void remove(Integer id) {
        brokers.remove(id);
    }

    public int size() {
        return brokers.size();
    }

    @Override
    public String toString() {
        return "Cluster(" + brokers.values() + ")";
    }
}
