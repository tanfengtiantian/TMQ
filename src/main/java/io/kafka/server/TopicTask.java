package io.kafka.server;


/**
 * @author tf
 * @version 创建时间：2019年1月29日 下午5:46:41
 * @ClassName TopicTask
 */
public class TopicTask {

    public static enum TaskType {
        CREATE, //
        DELETE, //
        ENLARGE, //
        SHUTDOWN;
    }

    public final String topic;

    public final TaskType type;

    public TopicTask(TaskType type, String topic) {
        this.type = type;
        this.topic = topic;
    }

    @Override
    public String toString() {
        return "TopicTask [type=" + type + ", topic=" + topic + "]";
    }
}