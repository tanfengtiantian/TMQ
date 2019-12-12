package io.kafka.network.receive;

import io.kafka.network.Transmission;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * @author tf
 * @version 创建时间：2019年1月17日 下午2:04:00
 * @ClassName 接收器
 */
public interface Receive extends Transmission {
	
	 ByteBuffer buffer();

	 /**
	  * 获取size(-4bytes) 读取余下字节码
	  * size + type + Len(topic) + topic + partition + messageSize + message
	  * @param channel
	  * @return
	  * @throws IOException
	  */
	 int readFrom(ReadableByteChannel channel) throws IOException;

	 int readCompletely(ReadableByteChannel channel) throws IOException;
}
