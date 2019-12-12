package io.kafka.message;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

/**
 * @author tf
 * @version 创建时间：2019年1月16日 下午2:34:53
 * @ClassName 消息辅助类-fileMessage-byteBufferMessage
 */
public abstract class MessageSet implements Iterable<MessageAndOffset> {

	public static final MessageSet Empty = new ByteBufferMessageSet(ByteBuffer.allocate(0));
	/**
	 *获取此集的总大小（字节）
	 * @return
	 */
	public abstract long getSizeInBytes();
	/**
	 * 写入
	 * @param channel
	 * @param offset
	 * @param maxSize
	 * @return
	 * @throws IOException
	 */
	public abstract long writeTo(GatheringByteChannel channel, long offset, long maxSize) throws IOException;

}
