package io.kafka.network.receive;

import io.kafka.network.Transmission;
import io.kafka.network.request.Request;
import io.kafka.network.request.RequestHandlerFactory;
import io.kafka.network.session.SessionEvent;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;

/**
 * @author tf
 * @version 创建时间：2019年1月17日 下午2:04:00
 * @ClassName 接收器
 */
public interface Receive extends Transmission {
	 /**
	  * 获取size(-4bytes) 读取余下字节码
	  * size + type + Len(topic) + topic + partition + messageSize + message
	  * @param channel
	  * @return
	  * @throws IOException
	  */
	 int readFrom(SessionEvent event, SelectionKey key, ReadableByteChannel channel, RequestHandlerFactory requestHandlerFactory) throws IOException;
}
