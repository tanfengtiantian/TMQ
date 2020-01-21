package io.kafka.network.receive;

import static java.lang.String.format;
import io.kafka.common.exception.InvalidRequestException;
import io.kafka.network.AbstractTransmission;
import io.kafka.utils.Utils;
import io.kafka.utils.nettyloc.ByteBuf;
import io.kafka.utils.nettyloc.PooledByteBufAllocator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;



/**
 * @author tf
 * @version 创建时间：2019年1月17日 下午3:20:27
 * @ClassName 有界接收数据包
 * <p>
 * Producer format:
 * <pre>
 * size + type + Len(topic) + topic + partition + messageSize + message
 * =====================================
 * size		  : size(4bytes)
 * type		  : type(2bytes)
 * Len(topic) : Len(2bytes)
 * topic	  : size(2bytes) + data(utf-8 bytes)
 * partition  : int(4bytes)
 * messageSize: int(4bytes)
 * message: bytes
 */
public class BoundedByteBufferReceive extends AbstractTransmission implements Receive {
	
	    final ByteBuffer sizeBuffer = ByteBuffer.allocate(4);

	    private ByteBuffer contentBuffer = null;

		private PooledByteBufAllocator allocator;

	    private int maxRequestSize;

	    public BoundedByteBufferReceive(int maxRequestSize, PooledByteBufAllocator allocator) {
	        this.maxRequestSize = maxRequestSize;
	        this.allocator = allocator;
	    }

	    /**
	     * 读取字节码数据
	     */
	    public int readFrom(ReadableByteChannel channel) throws IOException {
	        expectIncomplete();
	        int read = 0;
	        //有效数据长度
	        if (sizeBuffer.remaining() > 0) {
	            read += Utils.read(channel, sizeBuffer);
	        }
	        //
	        if (contentBuffer == null && !sizeBuffer.hasRemaining()) {
	            sizeBuffer.rewind();
	            int size = sizeBuffer.getInt();
	            if (size <= 0) {
	                throw new InvalidRequestException(format("%d 不是有效的请求大小.", size));
	            }
	            if (size > maxRequestSize) {
	                final String msg = "请求长度size[%d],超过最大值 maxRequestSize[%d].";
	                throw new InvalidRequestException(format(msg, size, maxRequestSize));
	            }
	            contentBuffer = byteBufferAllocate(size);
	        }
	        //
	        if (contentBuffer != null) {
	            read = Utils.read(channel, contentBuffer);
	            //判断buffer是否读完
	            if (!contentBuffer.hasRemaining()) {
	                contentBuffer.rewind();
	                setCompleted();
	            }
	        }
	        return read;
	    }

	    public int readCompletely(ReadableByteChannel channel) throws IOException {
	        int read = 0;
	        while (!complete()) {
	            read += readFrom(channel);
	        }
	        return read;
	    }

	    public ByteBuffer buffer() {
	        expectComplete();
	        return contentBuffer;
	    }

	    private ByteBuffer byteBufferAllocate(int size) {
	        ByteBuffer buffer = null;
	        try {
				//ByteBuf buf = allocator.directBuffer(size);
	            //buffer = buf.nioBuffer();
				buffer = ByteBuffer.allocate(size);
	        } catch (OutOfMemoryError oome) {
	            throw new RuntimeException("OOME with size " + size, oome);
	        } catch (RuntimeException t) {
	            throw t;
	        }
	        return buffer;
	    }

	@Override
	public void reset() {
		if(contentBuffer != null) {
			sizeBuffer.clear();
			contentBuffer.clear();
			contentBuffer = null;
		}
		super.reset();
	}

	@Override
	    public String toString() {
	        String msg = "BoundedByteBufferReceive [maxRequestSize=%d, expectSize=%d, readSize=%d, done=%s]";
	        return format(msg, maxRequestSize, contentBuffer == null ? -1 : contentBuffer.limit(), //
	                contentBuffer == null ? -1 : contentBuffer.position(), //
	                complete());
	    }
}
