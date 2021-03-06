package io.kafka.api;

import io.kafka.message.ByteBufferMessageSet;
import io.kafka.network.request.Request;
import io.kafka.utils.Utils;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;


/**
 * @author tf
 * @version 创建时间：2019年1月18日 下午2:23:29
 * @ClassName 服务提供请求类
 * message producer request
 * <p>
 * Producer format:
 * <pre>
 * size + type + Len(topic) + topic + partition + ttl + messageSize + message
 * =====================================
 * size		  : size(4bytes)
 * type		  : type(2bytes)
 * Len(topic) : Len(2bytes)
 * topic	  : size(2bytes) + data(utf-8 bytes)
 * partition  : int(4bytes)
 * ttl        : long(8bytes)
 * messageSize: int(4bytes)
 * message: bytes
 */
public class ProducerTTLRequest implements Request {

	/**
	 * 分区
	 */
	public static final int RandomPartition = -1;

	/**
     * 从缓冲区读取生产者请求
     *
     * @param buffer data
     * @return ProducerRequest
     */
    public static ProducerTTLRequest readFrom(ByteBuffer buffer) {
    	//[size] + buffer=([type -2bytes] + Len(topic) + topic + partition + messageSize + message)
    	//[size] + buffer=([type -2bytes] + Len[(topic -2bytes)] + [topic-Len] + partition + messageSize + message)
        String topic = Utils.readShortString(buffer);
        //[size] + buffer=([type -2bytes] + Len[(topic -2bytes)] + [topic-Len] + [partition-4bytes] + messageSize + message)
        int partition = buffer.getInt();

        long ttl = buffer.getLong();
        //[size] + buffer=([type -2bytes] + Len[(topic -2bytes)] + [topic-Len] + [partition-4bytes] + [messageSize-4bytes] + message)
        int messageSetSize = buffer.getInt();
        //创建子缓冲区
        ByteBuffer messageSetBuffer = buffer.slice();
        messageSetBuffer.limit(messageSetSize);
        buffer.position(buffer.position() + messageSetSize);
        return new ProducerTTLRequest(topic, partition, ttl, new ByteBufferMessageSet(messageSetBuffer));
    }

    /**
     * 选择top分区Partition
     * @param chooser
     * @return
     */
    public int getTranslatedPartition(PartitionChooser chooser) {
        if (partition == RandomPartition) {
        	partition = chooser.choosePartition(topic);
        }
        return partition;
    }
    /**
     * request messages
     */
    public final ByteBufferMessageSet messages;

    /**
     * topic partition
     */
    public int partition;

    /**
     * topic name
     */
    public final String topic;
    /**
     * topic offset
     */
    public long offset;
    /**
     * topic brokerId
     */
    public int brokerId;

    /**
     * topic ttl
     */
    public final long ttl;

    public ProducerTTLRequest(String topic, int partition, long ttl, ByteBufferMessageSet messages) {
        this.topic = topic;
        this.partition = partition;
        this.ttl = ttl;
        this.messages = messages;
    }

    public String getTopic() {
        return topic;
    }

    public static ByteBuffer serializeProducerTTL(ProducerTTLRequest request) {
        //Len(topic) + topic + partition + offset
        ByteBuffer buffer = ByteBuffer.allocate(Utils.caculateShortString(request.topic) +4+4+8);
        Utils.writeShortString(buffer, request.topic);
        buffer.putInt(request.brokerId);
        buffer.putInt(request.partition);
        buffer.putLong(-1);
        buffer.rewind();
        return buffer;
    }

	@Override
	public RequestKeys getRequestKey() {
		return RequestKeys.TTLPRODUCE;
	}

    public int getSizeInBytes() {
        return  Utils.caculateShortString(topic) //Len(topic) : Len(2bytes)  + topic : bytes
                + 4 //partition : 4bytes
                + 8 //ttl
                + 4 //messagesize : 4bytes
                + (int)messages.getSizeInBytes();//messageSize:: int(4bytes)  +  message : bytes
    }
	@Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("ProducerTTLRequest(");
        buf.append(topic).append(',').append(partition).append(',');
        buf.append(messages.getSizeInBytes()).append(')');
        return buf.toString();
    }

}
