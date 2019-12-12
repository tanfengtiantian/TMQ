package io.kafka.message;


import io.kafka.common.IteratorTemplate;
import io.kafka.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tf
 * @version 创建时间：2018年12月31日 上午12:16:26
 * @ClassName 文件辅助类， 磁盘上的消息集  FileChannel内存映射
 * size = size (4字节)  +  version (1字节) + attribute (1字节) + crc32 (4字节) +  data(N字节)
 */
public class FileMessageSet extends MessageSet{

    protected static final Logger logger = LoggerFactory.getLogger(FileMessageSet.class);
	protected FileChannel channel;
	private long offset;
	/**
	 * 是否允许追加消息
	 */
	private boolean mutable;
	/**
	 * 是否恢复
	 */
	private AtomicBoolean needRecover;
	protected final AtomicLong setSize = new AtomicLong();
    protected final AtomicLong setHighWaterMark = new AtomicLong();
	public FileMessageSet(File file, boolean mutable) throws IOException {
		this(Utils.openChannel(file, mutable), mutable);
	}

	public FileMessageSet(FileChannel channel, boolean mutable) throws IOException {
		this(channel, 0, Long.MAX_VALUE, mutable, new AtomicBoolean(false));
	}

	public FileMessageSet(FileChannel channel, long offset, long limit,
			boolean mutable, AtomicBoolean needRecover) throws IOException {
		this.channel = channel;
        this.offset = offset;
        this.mutable = mutable;
        this.needRecover = needRecover;
        //新建log块文件
        if (mutable) {
            if (limit < Long.MAX_VALUE || offset > 0) throw new IllegalArgumentException(
                    "尝试打开带有视图或偏移量的可变消息集.");
            //首次恢复
            if (needRecover.get()) {
                // 将文件位置设置为文件结尾以附加消息
                long startMs = System.currentTimeMillis();
                //将日志恢复到最后一个完整条目。从任何未完成的字节中截断任何字节
                long truncated = recover();
                //logger.info("恢复成功  耗时" + (System.currentTimeMillis() - startMs) / 1000 + " 秒. 截断-" + truncated + " 字节.");
            //第二次读取可写文件
            } else {
                setSize.set(channel.size());
                setHighWaterMark.set(getSizeInBytes());
                //获取文件当前位置
                channel.position(channel.size());
            }
         //已存在log块文件
        } else {
            setSize.set(Math.min(channel.size(), limit) - offset);
            setHighWaterMark.set(getSizeInBytes());
        }
	}
	
	//将字节从此通道的文件传输到给定的可写入字节通道 ,用于messagesend消费端
	@Override
	public long writeTo(GatheringByteChannel destChannel, long writeOffset, long maxSize)
			throws IOException {
		 return channel.transferTo(offset + writeOffset, Math.min(maxSize, getSizeInBytes()), destChannel);
	}

    /**
     * 从指定位置写入bf长度的数据到文件，文件指针<b>不会</b>向后移动
     *
     * @param offset
     * @param bf
     * @throws IOException
     */
    public void write(final long offset, final ByteBuffer bf) throws IOException {
        int size = 0;
        while (bf.hasRemaining()) {
            final int l = this.channel.write(bf, offset + size);
            size += l;
            if (l < 0) {
                break;
            }
        }
    }
	/**
	 * 恢复最后一个可写节点
	 * @return
	 * @throws IOException
	 */
	private long recover() throws IOException {
		checkMutable();
		long len = channel.size();
		ByteBuffer buffer = ByteBuffer.allocate(4);
		long validUpTo = 0;
        long next = 0L;
        do {
            next = validateMessage(channel, validUpTo, len, buffer);
            if (next >= 0) validUpTo = next;
        } while (next >= 0);
        //截断有效字节
        channel.truncate(validUpTo);
        setSize.set(validUpTo);
        setHighWaterMark.set(validUpTo);
        logger.info("恢复当前 mark:" + highWaterMark());
        channel.position(validUpTo);
        needRecover.set(false);
        return len - validUpTo;
	}
	/**
     * 读取、验证和放弃单个消息，返回下一个有效偏移量，以及
     * 正在验证的消息
	 * @throws IOException 
     */
	private long validateMessage(FileChannel channel, long start, long len, ByteBuffer buffer) throws IOException {
		buffer.rewind();
        int read = channel.read(buffer, start);
        if (read < 4) return -1;

        // 检查文件中是否有足够的字节
        int size = buffer.getInt(0);
        if (size < Message.MinHeaderSize) return -1;

        long next = start + 4 + size;
        if (next > len) return -1;

        // read message
        ByteBuffer messageBuffer = ByteBuffer.allocate(size);
        long curr = start + 4;
        while (messageBuffer.hasRemaining()) {
            read = channel.read(messageBuffer, curr);
            if (read < 0) throw new IllegalStateException("恢复过程中更改了文件大小!");
            else curr += read;
        }
        messageBuffer.rewind();
        Message message = new Message(messageBuffer);
        if (!message.isValid()) return -1;
        else return next;
	}

	public FileMessageSet(File file, boolean mutable, AtomicBoolean needRecover) throws IOException {
		this(Utils.openChannel(file, mutable), mutable, needRecover);
	}

	public FileMessageSet(FileChannel channel, boolean mutable,
			AtomicBoolean needRecover) throws IOException {
		 this(channel, 0, Long.MAX_VALUE, mutable, needRecover);
	}


	public long getSizeInBytes() {
        return setSize.get();
    }
	
	public void flush() throws IOException {
        if(channel.isOpen()){
            checkMutable();
            long startTime = System.currentTimeMillis();
            channel.force(true);
            long elapsedTime = System.currentTimeMillis() - startTime;
            setHighWaterMark.set(getSizeInBytes());
            logger.debug("刷盘 flush time:"+elapsedTime+" size mark:" + highWaterMark());
        }
    }
	
	public long highWaterMark() {
        return setHighWaterMark.get();
    }
	/**
	 * 验证文件可否追加消息
	 */
	void checkMutable() {
        if (!mutable) throw new IllegalStateException("此log文件只可读.");
    }
	
	public void close() throws IOException {
        if (mutable) flush();
        channel.close();
	}

	
	/**
	 * 
	 * @param messages
	 * @return 返回写入的大小和第一个偏移量
	 * @throws IOException
	 */
	public long[] append(MessageSet messages) throws IOException {
		checkMutable();
	    long written = 0L;
        while (written < messages.getSizeInBytes())
            written += messages.writeTo(channel, 0, messages.getSizeInBytes());
        long beforeOffset = setSize.getAndAdd(written);
        return new long[]{written, beforeOffset};
	}
	
	public FileMessageSet read(long readOffset, long size) throws IOException {
	   return new FileMessageSet(channel, this.offset + readOffset, //
	             Math.min(this.offset + readOffset + size, highWaterMark()), false, new AtomicBoolean(false));
	}

    /**
     * 从文件的制定位置读取数据到bf，直到读满或者读到文件结尾。 <br />
     * 文件指针不会移动
     *
     * @param bf
     * @param offset
     * @throws IOException
     */
    public void read(final ByteBuffer bf, final long offset) throws IOException {
        int size = 0;
        while (bf.hasRemaining()) {
            final int l = this.channel.read(bf, offset + size);
            size += l;
            if (l < 0) {
                break;
            }
        }
    }
	
	
	@Override
	public Iterator<MessageAndOffset> iterator() {
		return new IteratorTemplate<MessageAndOffset>() {
            long location = offset;
            @Override
            protected MessageAndOffset makeNext() {
                try {
                	//创建size缓冲区
                    ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
                    //读取数据大小   坐标偏移到  4
                    channel.read(sizeBuffer, location);
                    if (sizeBuffer.hasRemaining()) {
                        return allDone();// 没有读到则终止.segment已读完
                    }
                    // 坐标重置 0
                    sizeBuffer.rewind();
                    int size = sizeBuffer.getInt();
                    // 读到的消息长度大于 标题头的消息长度
                    if (size < Message.MinHeaderSize) {
                        return allDone();
                    }
                    //创建data缓冲区
                    ByteBuffer buffer = ByteBuffer.allocate(size);
                    channel.read(buffer, location + 4);
                    if (buffer.hasRemaining()) {
                        return allDone();
                    }
                    buffer.rewind();
                    //offset 移动到下一条
                    location += size + 4;
                    return new MessageAndOffset(new Message(buffer), location);
                } catch (IOException e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            }
        };
	}

    /**
     * 返回一个MessageSet镜像，指定offset和长度
     */
    public FileMessageSet slice(final long offset, final long limit) throws IOException {
        return new FileMessageSet(this.channel, offset, limit, false,new AtomicBoolean(false));
    }
}
