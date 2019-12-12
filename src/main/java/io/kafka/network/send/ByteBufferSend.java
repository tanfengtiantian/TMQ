package io.kafka.network.send;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

/**
 * @author tf
 * @version 创建时间：2019年2月11日 上午11:45:35
 * @ClassName ByteBufferSend
 */
public class ByteBufferSend extends AbstractSend {

    final ByteBuffer buffer;

    public ByteBufferSend(ByteBuffer buffer) {
        super();
        this.buffer = buffer;
    }

    
    /**
     * @return buffer
     */
    public ByteBuffer getBuffer() {
        return buffer;
    }
    public ByteBufferSend(int size) {
        this(ByteBuffer.allocate(size));
    }

    public int writeTo(GatheringByteChannel channel) throws IOException {
        expectIncomplete();
        int written = 0;
        written += channel.write(buffer);
        if (!buffer.hasRemaining()) {
            setCompleted();
        }
        return written;
    }

}
