package io.kafka.cluster;


/**
 * @author tf
 * @version 创建时间：2019年1月19日 下午2:07:20
 * @ClassName broker
 */
public class Broker {

    /**
     * broker id
     */
    public final int id;

    /**
     * the broker creator (hostname created time)
     *
     */
    public final String creatorId;

    /**
     * broker hostname
     */
    public final String host;

    /**
     * broker port
     */
    public final int port;

    /**
     * 是否自动创建top
     */
    public final boolean autocreated;

    /**
     * 是否是salve节点
     */
    public final boolean isSalve;

    /**
     * create a broker
     *
     * @param id        broker id
     * @param creatorId the creator id
     * @param host      broker hostname
     * @param port      broker port
     * @param autocreated auto-create new topics
     */
    public Broker(int id, String creatorId, String host, int port, boolean autocreated, boolean isSalve) {
        super();
        this.id = id;
        this.creatorId = creatorId;
        this.host = host;
        this.port = port;
        this.autocreated = autocreated;
        this.isSalve = isSalve;
    }

    /**
     *  broker save zookeeper
     * <p>
     * format: <b>creatorId:host:port</b>
     *
     * @return zookeeper保存格式
     */
    public String getZKString() {
        return String.format("%s:%s:%s:%s", creatorId.replace(':', '#'), host.replace(':', '#'), port, autocreated);
    }

    @Override
    public String toString() {
        return getZKString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Broker broker = (Broker) o;

        if (autocreated != broker.autocreated) return false;
        if (id != broker.id) return false;
        if (port != broker.port) return false;
        if (host != null ? !host.equals(broker.host) : broker.host != null) return false;
        if (isSalve != broker.isSalve) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + (host != null ? host.hashCode() : 0);
        result = 31 * result + port;
        result = 31 * result + (autocreated ? 1 : 0);
        return result;
    }

    /**
     * 使用给定的代理信息创建代理
     *
     * @param id               broker id
     * @param brokerInfoString broker format: <b>creatorId:host:port:autocreated</b>
     * @return broker config
     */
    public static Broker createBroker(int id, String brokerInfoString) {
        String[] brokerInfo = brokerInfoString.split(":");
        String creator = brokerInfo[0].replace('#', ':');
        String hostname = brokerInfo[1].replace('#', ':');
        String port = brokerInfo[2];
        boolean autocreated = Boolean.valueOf(brokerInfo.length > 3 ? brokerInfo[3] : "true");
        boolean isSalve = Boolean.valueOf(brokerInfo.length > 4 ? brokerInfo[4] : "true");
        return new Broker(id, creator, hostname, Integer.parseInt(port), autocreated, isSalve);
    }
}