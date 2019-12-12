package io.kafka.message;

import io.kafka.common.CompressionCodec;
import io.kafka.common.IteratorTemplate;
import io.kafka.common.exception.InvalidMessageException;
import io.kafka.common.exception.InvalidMessageSizeException;
import io.kafka.common.exception.MessageSizeTooLargeException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.Iterator;

/**
 * @author tf
 * @version 创建时间：2018年12月29日 下午2:51:40
 * @ClassName 消息buffer
 */
public class ByteBufferMessageSet extends MessageSet {

	private ByteBuffer buffer;
	private long initialOffset;
	private long shallowValidByteCount = -1L;
	public static final int LogOverhead = 4;
	private long validBytes;

	public ByteBufferMessageSet(ByteBuffer buffer) {
		this(buffer,0L);
	}
	
	public ByteBufferMessageSet(ByteBuffer buffer,long initialOffset) {
        this.buffer = buffer;
        this.initialOffset = initialOffset;
        this.validBytes = shallowValidBytes();
    }
	
	public ByteBufferMessageSet(CompressionCodec compressionCodec,
			Message... messages) {
		this(ByteBufferMessageSet.createByteBuffer(compressionCodec, messages),0L);
	}

	public static ByteBuffer createByteBuffer(CompressionCodec compressionCodec, Message... messages) {
        if (compressionCodec == CompressionCodec.NoCompressionCodec) {
            ByteBuffer buffer = ByteBuffer.allocate(messageSetSize(messages));
            for (Message message : messages) {
            	//设置size
                message.serializeTo(buffer);
            }
            buffer.rewind();
            return buffer;
        }
        
        return null;
    }

    /**
     * @return the buffer
     */
    public ByteBuffer getBuffer() {
        return buffer;
    }

	public static int messageSetSize(Message... messages) {
        int size = 0;
        for (Message message : messages) {
            size += entrySize(message);
        }
        return size;
    }
	public static int entrySize(Message message) {
        return LogOverhead + message.getSizeInBytes();
    }
	
	@Override
	public long writeTo(GatheringByteChannel channel, long offset, long maxSize) throws IOException {
		buffer.mark();
        int written = channel.write(buffer);
        buffer.reset();
        return written;
	}
	
	@Override
	public long getSizeInBytes() {
		return buffer.limit();
	}
	
	/**
	 * 获取有效的缓冲区字节
	 * @return
	 */
	private long shallowValidBytes() {
	   if (shallowValidByteCount < 0) {
            Iterator<MessageAndOffset> iter = this.internalIterator(true);
            while (iter.hasNext()) {
                shallowValidByteCount = iter.next().offset;
            }
        }
        if (shallowValidByteCount < initialOffset) {
            return 0;
        } else {
            return shallowValidByteCount - initialOffset;
        }
    }
	/**
	 * 获取有效的缓冲区字节
	 * 缓冲区的大小等于或大于有效消息的大小。
	 * 最后一条信息可能不完整。
	 * @return
	 */
	public long getValidBytes() {
        return validBytes;
    }
	
	@Override
	public Iterator<MessageAndOffset> iterator() {
		return internalIterator(false);
	}
	
	public Iterator<MessageAndOffset> internalIterator(boolean isShallow){
        return new Iter(isShallow);
    }
	class Iter extends IteratorTemplate<MessageAndOffset> {
		boolean isShallow;
        ByteBuffer topIter = buffer.slice();
        long currValidBytes = initialOffset;
        Iterator<MessageAndOffset> innerIter = null;
        long lastMessageSize = 0L;
        Iter(boolean isShallow) {
            this.isShallow = isShallow;
        }
        
        private boolean innerDone() {
            return innerIter == null || !innerIter.hasNext();
        }
		private MessageAndOffset makeNextOuter() {
            if(topIter.remaining() <4)return allDone();
            int size = topIter.getInt();
            lastMessageSize = size;
            if(size<0||topIter.remaining()<size) {
                if(currValidBytes == initialOffset||size<0) {
                    throw new InvalidMessageSizeException("invalid message size: " + size + " only received bytes: " +
                            topIter.remaining() + " at " + currValidBytes + "( possible causes (1) a single message larger than " +
                            "the fetch size; (2) log corruption )");
                }
                return allDone();
            }
            //
            ByteBuffer message = topIter.slice();
            message.limit(size);
            topIter.position(topIter.position() + size);
            Message newMessage = new Message(message);
            if(isShallow) {
                currValidBytes += 4 +size;
                return new MessageAndOffset(newMessage, currValidBytes);
            }
            if(newMessage.compressionCodec() == CompressionCodec.NoCompressionCodec) {
                if(!newMessage.isValid())throw new InvalidMessageException("未压缩的消息无效");
                innerIter = null;
                currValidBytes += 4 +size;
                return new MessageAndOffset(newMessage, currValidBytes);
            }
            return makeNext();
		}
		@Override
		protected MessageAndOffset makeNext() {
			if (isShallow) return makeNextOuter();
			if (innerDone()) return makeNextOuter();
			MessageAndOffset messageAndOffset = innerIter.next();
            if (!innerIter.hasNext()) {
                currValidBytes += 4 + lastMessageSize;
            }
            return new MessageAndOffset(messageAndOffset.message, currValidBytes);
		}	 
	}
	
	public ByteBuffer serialized() {
		return buffer;
	}

	/**
	 * 检查每个消息的最大大小
	 * @param maxMessageSize
	 */
	public void verifyMessageSize(int maxMessageSize) {
		Iterator<MessageAndOffset> shallowIter =  internalIterator(true);
		while(shallowIter.hasNext()) {
           MessageAndOffset messageAndOffset = shallowIter.next();
           int payloadSize = messageAndOffset.message.payloadSize();
           if(payloadSize > maxMessageSize) {
               throw new MessageSizeTooLargeException("message size : [" + payloadSize + "] > maxMessageSize : [" + maxMessageSize + "]");
           }
       }
	}
}
