package io.kafka.plugin;

import com.sun.management.UnixOperatingSystemMXBean;
import io.kafka.cluster.Broker;
import io.kafka.common.MqDailyRollingFileAppender;
import io.kafka.config.ServerConfig;
import io.kafka.log.ILogManager;
import io.kafka.mx.BrokerTopicStat;
import io.kafka.mx.StatsManager;
import io.kafka.transaction.store.TransactionStore;
import io.kafka.utils.Closer;
import io.kafka.utils.Utils;
import org.apache.log4j.Logger;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.*;

public class BrokerContext {

    private String VERSION = "2.0.0";

    private final ILogManager logManager;

    private final TransactionStore transactionStore;

    private final ServerConfig config;


    public ILogManager getLogManager() {
        return logManager;
    }

    public TransactionStore getTransactionStore() {
        return transactionStore;
    }

    public BrokerContext(ILogManager logManager, TransactionStore transactionStore, ServerConfig config){
        this.logManager = logManager;
        this.transactionStore = transactionStore;
        this.config = config;
    }

    /** -------------------dashboard------------------ **/
    public Instance getInstance(){
        return new Instance(
                StatsManager.INSTANCE.getStartupTimestamp(),
                config.getHostName(),
                config.getLogDir()
                );
    }

    public JVM getJVM(){
        return new JVM();
    }
    public Broker getBroker(){
        return Broker.createBroker(config.getBrokerId(),
                new StringBuilder()
                    .append(config.getHostName()+"-"+System.currentTimeMillis())//creatorId
                    .append(":")
                    .append(config.getHostName())//hostname
                    .append(":")
                    .append(config.getPort())//port
                    .append(":")
                    .append(config.isSlave())
                    .toString()
            );
    }

    public system getSystem(){
        return new system();
    }

    public String getVersion(){
        return VERSION;
    }

    /** -------------------logger------------------ **/
    public List<String> getlog(long timestamp){
        MqDailyRollingFileAppender appender = (MqDailyRollingFileAppender)Logger.getRootLogger().getAppender("ServerDailyRollingFile");
        return appender.getLogs(timestamp);
    }

    /** -------------------cluster------------------ **/
    public List<Broker> getCluster(){
        List<Broker> list = new ArrayList<>();
        list.add(getBroker());
        return list;
    }

    /** -------------------java-properties------------------ **/
    public Map<String, String> getJavaProperties(){
        return new HashMap<String, String>((Map) System.getProperties());
    }
    /** -------------------threads-dump------------------ **/
    public List<ThreadInfo> getThreadsDump(){
        return Arrays.asList(ManagementFactory.getThreadMXBean().dumpAllThreads(false,false));
    }
    /** -------------------Broker-config------------------ **/
    public String getConfig(){
        InputStreamReader isr = null;
        try {
            isr = new InputStreamReader(Utils.class.getResourceAsStream("/server.properties"));
            BufferedReader reader = new BufferedReader(isr);
            StringBuilder builder = new StringBuilder();
            String line;
            try {
                while ((line = reader.readLine()) != null) {
                    builder.append(line);
                    builder.append("\n");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return builder.toString();
        } finally {
            Closer.closeQuietly(isr);
        }
    }

    /** -------------------Topics------------------- **/
    public List<Topics> getTopics() {
        Map<String, Integer> map = logManager.getTopicPartitionsMap();
        List<Topics> list = new ArrayList<>();
        map.forEach((k,v)->{
            list.add(new Topics(
                    k,
                    v,
                    StatsManager.INSTANCE.getBrokerTopicMessagesIn(k),
                    StatsManager.INSTANCE.getBrokerTopicMessagesIn(k))
            );
        });
        return list;
    }


    class Instance {
        private long start;
        private String host;
        private String data;

        public Instance (long start,String host,String data){
            this.start = start;
            this.host = host;
            this.data = data;
        }
        public long getStart() {
            return System.currentTimeMillis() - start;
        }

        public String getHost() {
            return host;
        }

        public String getData() {
            return data;
        }

        public String getDataLog() {
            return System.getProperty("user.home");
        }

        public String getCmd() {
            return System.getProperty("user.dir");
        }

        @Override
        public String toString() {
            return "Instance [start=" + start + ", host=" + host + ", data=" + data + "]";
        }
    }

    class JVM {
        public String getRuntime(){ return  System.getProperty("java.vm.name"); }
        /**
         * Java虚拟机的可用的处理器数量
         * @return
         */
        public int getProcessors(){ return  Runtime.getRuntime().availableProcessors(); }

        public List<String> getArgs(){ return  ManagementFactory.getRuntimeMXBean().getInputArguments(); }

        @Override
        public String toString() {
            return "JVM [Runtime=" + getRuntime() + ", getProcessors=" + getProcessors() + ", Args=" + getArgs().toString() + "]";
        }
    }

    class system {

        private UnixOperatingSystemMXBean osmxb = (UnixOperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        private int byteToGB = 1024 * 1024 * 1024;
        private int byteToMB = 1024 * 1024;
        /**
         * 操作系统总物理内存
         * @return
         */
        public String getSysMemory(){
            return (osmxb.getTotalPhysicalMemorySize() / byteToGB) +" GB";
        }

        /**
         * 操作系统物理内存已用的空间
         * @return
         */
        public String getSysMemoryUsed(){
            return (osmxb.getFreePhysicalMemorySize() / byteToGB) +" GB";
        }

        /**
         * 虚拟内存的总量
         * @return
         */
        public String getSwapSpace(){
            return (osmxb.getTotalSwapSpaceSize() / byteToGB) +" GB";
        }
        /**
         * 已用虚拟内存的总量
         * @return
         */
        public String getSwapSpaceUsed(){
            return (osmxb.getFreeSwapSpaceSize() / byteToGB) +" GB";
        }

        /**
         * 最大文件描述符
         * @return
         */
        public long getFileDescriptors(){
            return osmxb.getMaxFileDescriptorCount();
        }
        /**
         * 打开的文件描述符的数量
         * @return
         */
        public long getFileDescriptorsUsed(){
            return osmxb.getOpenFileDescriptorCount();
        }

        /**
         * 最大JVM内存总量
         * @return
         */
        public String getJvmMemoryMax(){
            return (Runtime.getRuntime().maxMemory() / byteToMB) +" MB";
        }

        /**
         * 当前JVM内存总量
         * @return
         */
        public String getJvmMemoryTotal(){
            return (Runtime.getRuntime().totalMemory() / byteToMB) +" MB";
        }

        /**
         * 使用的JVM内存
         * @return
         */
        public String getJvmMemoryUsed(){
            return (Runtime.getRuntime().freeMemory() / byteToMB) +" MB";
        }

        @Override
        public String toString() {
            return "System [SysMemory=" + getSysMemory() +
                    ", SysMemoryUsed=" + getSysMemoryUsed() +
                    ", SwapSpace=" + getSwapSpace() +
                    ", SwapSpaceUsed=" + getSwapSpaceUsed() +
                    ", FileDescriptors=" + getFileDescriptors() +
                    ", FileDescriptorsUsed=" + getFileDescriptorsUsed() +
                    ", JvmMemoryMax=" + getJvmMemoryMax() +
                    ", JvmMemoryTotal=" + getJvmMemoryTotal() +
                    ", JvmMemoryUsed=" + getJvmMemoryUsed() +
                    "]";
        }
    }

    class Topics {

        private String topic;
        private int partitions;
        private long messagesNumber;
        private long messagesBytes;

        public Topics(String topic, int partitions, long messagesNumber, long messagesBytes) {
            this.topic = topic;
            this.partitions = partitions;
            this.messagesNumber = messagesNumber;
            this.messagesBytes = messagesBytes;
        }
        public String getTopic() {
            return topic;
        }

        public int getPartitions() {
            return partitions;
        }

        public long getMessagesNumber() {
            return messagesNumber;
        }

        public long getMessagesBytes() {
            return messagesBytes;
        }

        public long getAverageSize() {
            if(messagesNumber == 0){
                return messagesNumber;
            }
            return messagesBytes/messagesNumber;
        }

        public boolean getAcceptPub() {
            return true;
        }

        public boolean getAcceptSub() {
            return true;
        }

        @Override
        public String toString() {
            return "Topics [Topic=" + topic +
                    ", Partitions=" + partitions +
                    ", MessagesNumber=" + messagesNumber +
                    ", MessagesBytes=" + messagesBytes +
                    ", AcceptPub=" + getAcceptPub() +
                    ", AcceptSub=" + getAcceptSub() +
                    "]";
        }
    }
}
