package io.kafka.network.receive;

import static java.lang.String.format;

import io.kafka.api.RequestKeys;
import io.kafka.common.exception.InvalidRequestException;
import io.kafka.network.AbstractTransmission;
import io.kafka.network.request.Request;
import io.kafka.network.request.RequestHandler;
import io.kafka.network.request.RequestHandlerFactory;
import io.kafka.network.send.Send;
import io.kafka.network.session.NioSession;
import io.kafka.network.session.SessionEvent;
import io.kafka.utils.ByteBufferUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;

/**
 * @author tf
 * @version 创建时间：2019年1月17日 下午3:20:27
 * @ClassName 有界接收数据包
 */
public class BoundedByteBufferReceive extends AbstractTransmission implements Receive {

	private final NioSession session;

	private ByteBuffer readBuffer;

	private int recvBufferSize;

	private Request request = null;

	public BoundedByteBufferReceive(int recvBufferSize, ByteBuffer readBuffer, NioSession session) {
		this.recvBufferSize = recvBufferSize;
		this.readBuffer = readBuffer;
		this.session = session;
	}

	@Override
	public int readFrom(SessionEvent event, SelectionKey key, ReadableByteChannel selectableChannel, RequestHandlerFactory requestHandlerFactory) throws IOException {
		expectIncomplete();
		//buffer空间已满，扩大空间
		if(!this.readBuffer.hasRemaining()) {
			this.readBuffer = ByteBufferUtils.increaseBufferCapatity(this.readBuffer,recvBufferSize);
		}
		int n = -1;
		int readCount = 0;
		try {
			//读满或管道没有字节数据
			while ((n = selectableChannel.read(this.readBuffer)) > 0) {
				readCount += n;
				// readBuffer已经写满，则先开始处理buffer，处理完后在写入buffer
				if (!this.readBuffer.hasRemaining()) {
						break;
					}
			}
			if (readCount > 0) {
				//将buffer写状态切换读状态，并开始进行消息解析
				this.readBuffer.flip();
				this.decode(key,event,requestHandlerFactory);
				//处理完成后，将剩余消息压缩到buffer最前面
				this.readBuffer.compact();
				//切换onRead
				if (this.complete()) {
					key.interestOps(SelectionKey.OP_READ);
					this.reset();
				}
			} else {
				new InvalidRequestException("Unable to read data.");
			}
		}catch (final ClosedChannelException e) {
			event.onClosed(key);
			/*
			ignore，不需要用户知道
			*/
		}

		return readCount;
	}

	private void decode(SelectionKey key, SessionEvent event, RequestHandlerFactory requestHandlerFactory) throws IOException {
		//读取数据总大小
		int size = this.readBuffer.remaining();
		//数据处理
		while (this.readBuffer.hasRemaining()) {
			// 数据容量不足不可解析则break
			if (readBuffer.remaining() >= 4) {
				//标记
				readBuffer.mark();
				int contentBufferSize = readBuffer.getInt();
				if (contentBufferSize <= 0) {
					throw new InvalidRequestException(format("%d 不是有效的请求大小.", size));
				}
				//消息体超可解析容量，则回溯"标记"位置，并break，readFrom函数继续read
				if(readBuffer.remaining() >= contentBufferSize) {
					//解析数据包
					final short requestTypeId = readBuffer.getShort();
					//获取请求type类型
					final RequestKeys requestType = RequestKeys.valueOf(requestTypeId);
					if (requestType == null) {
						throw new InvalidRequestException("未知请求类型  requestTypeId：" + requestTypeId);
					}
					RequestHandlerFactory.Decoder decoder = requestHandlerFactory.getDecoder(requestType);
					if (decoder == null) {
						throw new InvalidRequestException("No handler found request");
					}
					//解析message
					request = decoder.decode(readBuffer);
					this.setCompleted();
					//广播插件message
					session.onMessageReceived(requestType,request);
					//处理message
					event.onMessage(session,request);
					if(session.getSend() != null) {
						//写入消息
						event.OnWrite(key);
					}
					//解析下一条
				} else {
					readBuffer.reset();
					break;
				}
			} else {
				break;
			}
		}
	}

	@Override
	public void reset() {
		super.reset();
	}

}
