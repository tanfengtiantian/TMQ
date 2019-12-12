package io.kafka.api;

import java.nio.ByteBuffer;
import io.kafka.network.request.Request;
import io.kafka.utils.Utils;

/**
 * @author tf
 * @version 创建时间：2019年1月24日 上午9:49:51
 * @ClassName 类名称
 */
public class FetchRequest implements Request {

	/**
     * message topic
     */
    public final String topic;

    /**
     * partition
     */
    public final int partition;

    /**
     * offset topic(log file)
     */
    public final long offset;

    /**
     * 此请求的最大数据大小(bytes) 
     */
    public final int maxSize;

    public FetchRequest(String topic, int partition, long offset) {
        this(topic, partition, offset, 64 * 1024);//64KB
    }
    
	public FetchRequest(String topic, int partition, long offset, int maxSize) {
        this.topic = topic;
        if (topic == null) {
            throw new IllegalArgumentException("no topic");
        }
        this.partition = partition;
        this.offset = offset;
        this.maxSize = maxSize;
    }

	@Override
	public RequestKeys getRequestKey() {
		return RequestKeys.FETCH;
	}

	 /**
     * 从缓冲区读取获取请求
     * 
     * @param buffer  data
     * @return FetchRequest
     */
    public static FetchRequest readFrom(ByteBuffer buffer) {
        String topic = Utils.readShortString(buffer);
        int partition = buffer.getInt();
        long offset = buffer.getLong();
        int size = buffer.getInt();
        return new FetchRequest(topic, partition, offset, size);
    }

	@Override
    public String toString() {
        return "FetchRequest(topic:" + topic + ", part:" + partition + " offset:" + offset + " maxSize:" + maxSize + ")";
    }

}
