package io.kafka.message;

import static java.lang.String.format;

import java.nio.ByteBuffer;

import io.kafka.common.CompressionCodec;
import io.kafka.common.exception.UnknownMagicByteException;
import io.kafka.utils.Utils;

/**
 * @author tf
 * @version 创建时间：2018年12月30日 下午10:24:00
 * @ClassName message
 * 
 * <pre>
 * 1. 1 byte "magic"  协议
 * 2. 1 byte "attributes"  属性
 * 3. 4 byte CRC32 
 * 4. N - 6 byte 
 * </pre>
 */
public class Message {

	private static final byte MAGIC_VERSION2 = 1;

    public static final byte MAGIC_OFFSET = 0;

    public static final byte MAGIC_LENGTH = 1;

    /**
     * 属性\偏移  1 字节
     */
    public static final byte ATTRIBUTE_OFFSET = MAGIC_OFFSET + MAGIC_LENGTH;

    /**
     * 属性长度  1 字节
     */
    public static final byte ATTRIBUT_ELENGTH = 1;
    
    /**
     * Crc 4 字节
     */
    public static final byte CrcLength = 4;

    public static final int MinHeaderSize = headerSize((byte) 1);

	private static final byte CurrentMagicValue = 1;
	
	/**
     *指定压缩代码的掩码。2位用于保存
     *压缩编解码器。0被保留以表示没有压缩
     */
    public static final int CompressionCodeMask = 0x03; //
	
    final ByteBuffer buffer;

    private final int messageSize;

    public Message(ByteBuffer buffer) {
        this.buffer = buffer;
        this.messageSize = buffer.limit();
    }
    
    
    public Message(byte[] bytes) {
    	this(bytes, CompressionCodec.NoCompressionCodec);
	}

	public Message(byte[] bytes, CompressionCodec compressionCodec) {
		this(Utils.crc32(bytes), bytes, compressionCodec);
	}


	public Message(long checksum, byte[] bytes, CompressionCodec compressionCodec) {
		//"magic" +  "attributes" + "CRC32" + N内容
		this(ByteBuffer.allocate(Message.headerSize(Message.CurrentMagicValue) + bytes.length));
		//1 byte "magic"  协议
		buffer.put(CurrentMagicValue);
        byte attributes = 0;
        if (compressionCodec.codec > 0) {
            attributes = (byte) (attributes | (CompressionCodeMask & compressionCodec.codec));
        }
        //2. 1 byte "attributes"  属性
        buffer.put(attributes);
        //Utils.putUnsignedInt(buffer, checksum);
        //3. 4 byte CRC32 长度
        buffer.putInt((int) (checksum & 0xffffffffL));
        //4. N - byte 
        buffer.put(bytes);
        buffer.rewind();
	}

	/**
	 * "magic" +  "attributes" + "CRC32" + N内容
	 * @param magic
	 * @return
	 */
	public static int headerSize(byte magic) {
        return payloadOffset(magic);
    }
    public static int payloadOffset(byte magic) {
        return crcOffset(magic) + CrcLength;
    }
    
    public static int crcOffset(byte magic) {
        switch (magic) {
            case MAGIC_VERSION2:
                return ATTRIBUTE_OFFSET + ATTRIBUT_ELENGTH;

        }
        throw new UnknownMagicByteException(format("Magic %d 不正确", magic));
    }
    
    /**
     * 获取不带消息头的真实数据
     * @return nio-ByteBuffer
     */
    public ByteBuffer payload() {
    	//复制缓冲区内容
        ByteBuffer payload = buffer.duplicate();
        //设置位置
        int position = headerSize(magic());
        //位置移动到真实数据，创建子缓冲区，分片
        payload.position(position);
        payload = payload.slice();
        //设置限制
        payload.limit(payloadSize());
        payload.rewind();
        return payload;
    }
    
    /**
     * magic code ( constant 1)
     * 
     * @return 1
     */
    public byte magic() {
        return buffer.get(MAGIC_OFFSET);
    }
    
    public int payloadSize() {
        return getSizeInBytes() - headerSize(magic());
    }
    
    public int getSizeInBytes() {
        return messageSize;
    }

	public boolean isValid() {
		 return checksum() == Utils.crc32(buffer.array(), buffer.position() + buffer.arrayOffset() + payloadOffset(magic()), payloadSize());
	}
	public long checksum() {
        return Utils.getUnsignedInt(buffer, crcOffset(magic()));
    }
	
	public byte attributes() {
        return buffer.get(ATTRIBUTE_OFFSET);
    }
	
	@Override
    public String toString() {
        return format("message(magic = %d, attributes = %d, crc = %d, payload = %s)",//
                magic(), attributes(), checksum(), payload());
    }

	/**
	 * 设置 size+复制buffer
	 * @param serBuffer
	 */
	public void serializeTo(ByteBuffer serBuffer) {
		serBuffer.putInt(buffer.limit());
        serBuffer.put(buffer.duplicate());
	}


	public int serializedSize() {
		 return 4 /* int size */+ buffer.limit();
	}
	
	public CompressionCodec compressionCodec() {
		byte magicByte = magic();
        switch (magicByte) {
            case 0:
                return CompressionCodec.NoCompressionCodec;
            case 1:
                return CompressionCodec.valueOf(buffer.get(ATTRIBUTE_OFFSET) & CompressionCodeMask);
        }
        throw new RuntimeException("Invalid(无效的) magic byte " + magicByte);
	}
}
