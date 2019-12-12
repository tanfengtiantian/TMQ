package io.kafka.api;

import io.kafka.network.request.Request;
import io.kafka.utils.Utils;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author tf
 * @version 创建时间：2019年2月14日 上午10:09:10
 * @ClassName OffsetRequest
 */
public class OffsetRequest implements Request {

    public static final String SMALLES_TIME_STRING = "smallest";

    public static final String LARGEST_TIME_STRING = "largest";

    /**
     * reading the latest offset
     */
    public static final long LATES_TTIME = -1L;

    /**
     * reading the earilest offset
     */
    public static final long EARLIES_TTIME = -2L;

    ///////////////////////////////////////////////////////////////////////
    /**
     * message topic
     */
    public String topic;

    /**
     * topic partition,default value is 0
     */
    public int partition;

    /**
     * the earliest time of messages(unix milliseconds time)
     *
     * <ul>
     * <li>{@link #LATES_TTIME}: the latest(largest) offset</li>
     * <li>{@link #EARLIES_TTIME}: the earilest(smallest) offset</li>
     * <li>time&gt;0: the log file offset which lastmodified time earlier
     * than the time</li>
     * </ul>
     */
    public long time;

    /**
     * number of offsets
     */
    public int maxNumOffsets;

    /**
     * create a offset request
     * 
     * @param topic topic name
     * @param partition partition id
     * @param time the log file created time {@link #time}
     * @param maxNumOffsets the number of offsets
     * @see #time
     */
    public OffsetRequest(String topic, int partition, long time, int maxNumOffsets) {
        this.topic = topic;
        this.partition = partition;
        this.time = time;
        this.maxNumOffsets = maxNumOffsets;
    }

    public RequestKeys getRequestKey() {
        return RequestKeys.OFFSETS;
    }

    public void writeTo(ByteBuffer buffer) {
        Utils.writeShortString(buffer, topic);
        buffer.putInt(partition);
        buffer.putLong(time);
        buffer.putInt(maxNumOffsets);
    }

    @Override
    public String toString() {
        return "OffsetRequest(topic:" + topic + ", part:" + partition + ", time:" + time + ", maxNumOffsets:" + maxNumOffsets + ")";
    }

    ///////////////////////////////////////////////////////////////////////
    public static OffsetRequest readFrom(ByteBuffer buffer) {
        String topic = Utils.readShortString(buffer);
        int partition = buffer.getInt();
        long offset = buffer.getLong();
        int maxNumOffsets = buffer.getInt();
        return new OffsetRequest(topic, partition, offset, maxNumOffsets);
    }

    public static ByteBuffer serializeOffsetArray(List<Long> offsets) {
        int size = 4 + 8 * offsets.size();
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.putInt(offsets.size());
        for (int i = 0; i < offsets.size(); i++) {
            buffer.putLong(offsets.get(i));
        }
        buffer.rewind();
        return buffer;
    }
}
