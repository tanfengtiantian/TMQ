package io.kafka.utils;

import java.nio.ByteBuffer;

public class ByteBufferUtils {

    /**
     * byte扩容
     * @param byteBuffer
     * @param increaseSize
     * @return
     */
    public static final ByteBuffer increaseBufferCapatity(final ByteBuffer byteBuffer, final int increaseSize) {
        if (byteBuffer == null) {
            throw new IllegalArgumentException("buffer is null");
        }

        if (increaseSize <= 0) {
            throw new IllegalArgumentException("increaseSize<=0");
        }

        final int capacity = byteBuffer.capacity() + increaseSize;
        if (capacity < 0) {
            throw new IllegalArgumentException("capacity can't be negative");
        }
        final ByteBuffer result =
                byteBuffer.isDirect() ? ByteBuffer.allocateDirect(capacity) : ByteBuffer.allocate(capacity);
        result.order(byteBuffer.order());
        byteBuffer.flip();
        result.put(byteBuffer);
        return result;
    }
}
