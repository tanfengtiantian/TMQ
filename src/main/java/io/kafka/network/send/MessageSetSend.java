package io.kafka.network.send;

import io.kafka.common.ErrorMapping;
import io.kafka.message.MessageSet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

/**
 * @author tf
 * @version 创建时间：2019年1月24日 上午10:08:40
 * @ClassName 直接从其中写入所需字节的零拷贝消息响应
 */
public class MessageSetSend extends AbstractSend {

    private long sent = 0;

    private long size;

    private final ByteBuffer header = ByteBuffer.allocate(6);

    //
    public final MessageSet messages;

    public final ErrorMapping errorCode;

    public MessageSetSend(MessageSet messages, ErrorMapping errorCode) {
        super();
        this.messages = messages;
        this.errorCode = errorCode;
        this.size = messages.getSizeInBytes();
        header.putInt((int) (size + 2));
        header.putShort(errorCode.code);
        header.rewind();
    }

    public MessageSetSend(MessageSet messages) {
        this(messages, ErrorMapping.NoError);
    }

    public MessageSetSend() {
        this(MessageSet.Empty);
    }

    public int writeTo(GatheringByteChannel channel) throws IOException {
        expectIncomplete();
        int written = 0;
        if (header.hasRemaining()) {
            written += channel.write(header);
        }
        if (!header.hasRemaining()) {
        	//将字节从此通道的文件传输到给定的可写入字节通道
            int fileBytesSent = (int) messages.writeTo(channel, sent, size - sent);
            written += fileBytesSent;
            sent += fileBytesSent;
        }
        if (sent >= size) {
            setCompleted();
        }
        return written;
    }

    public int getSendSize() {
        return (int) size + header.capacity();
    }

}