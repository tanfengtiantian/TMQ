package io.kafka.transaction;

import io.kafka.transaction.store.Location;

import java.nio.ByteBuffer;

/**
 * Append回调
 *
 * @author tf
 * @date 2019-6-27
 *
 */
public interface AppendCallback {

    /**
     * 在append成功后回调此方法，传入写入的location
     *
     * @param location
     */
    void appendComplete(Location location, ByteBuffer buffer);
}