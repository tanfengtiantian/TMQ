package io.kafka.ttl;

import io.kafka.log.ILog;
import io.kafka.message.ByteBufferMessageSet;
import io.kafka.transaction.store.JournalLocation;
import io.kafka.utils.nettyloc.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * 延迟消息管理
 *
 * @author tf
 * @date 2019-7-31
 *
 */
public interface DelayMessageStore {


    /**
     * 创建直接内存
     * @return
     */
    ByteBuf directBuffer(ILog log, ByteBufferMessageSet message);

    /**
     * 添加延迟消息-直接内存
     * @param buf
     * @param log
     * @param message
     * @param delay
     * @return
     * @throws IOException
     */
    ByteBuffer addMessage(ByteBuf buf, ILog log, ByteBufferMessageSet message, long delay);
    /**
     * 添加延迟消息并持久化
     * @param buf
     * @param log
     * @param message
     * @param delay
     * @return
     * @throws IOException
     */
    JournalLocation addMessageAndWritten(ByteBuf buf, ILog log, ByteBufferMessageSet message, long delay) throws IOException;

    /**
     * 读取直接内存消息，并释放直接内存
     * @param buf
     * @param location
     * @return
     */
    ByteBufferMessageSet readDirectBytesAndRelease(ByteBuf buf,JournalLocation location) throws IOException;

    /**
     * 恢复ttl持久化数据
     */
    void recover() throws IOException;
}
