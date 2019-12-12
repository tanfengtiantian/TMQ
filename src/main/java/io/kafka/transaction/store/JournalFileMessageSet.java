package io.kafka.transaction.store;

import io.kafka.message.FileMessageSet;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class JournalFileMessageSet extends FileMessageSet {

    private final AtomicInteger referenceCount = new AtomicInteger(0);
    private final int number;
    private final File file;

    public JournalFileMessageSet(final File file, final int number, boolean mutable) throws IOException {
        super(file, mutable);
        this.number = number;
        this.file = file;
    }

    public long[] append(final ByteBuffer buffer) throws IOException {
        long written = 0L;
        while (buffer.hasRemaining()) {
            written += this.channel.write(buffer);
            if (written < 0) {
                break;
            }
        }
        long beforeOffset = setSize.getAndAdd(written);
        return new long[]{written, beforeOffset};
        //return new long[]{written, this.channel.position()};
    }
    /**
     * 对文件增加一个引用计数
     *
     * @return 增加后的引用计数
     */
    public int increment() {
        return this.referenceCount.incrementAndGet();
    }

    /**
     * 对文件减少一个引用计数
     *
     * @return 减少后的引用计数
     */
    public int decrement() {
        return this.referenceCount.decrementAndGet();
    }

    /**
     * 文件是否还在使用（引用计数是否是0了）
     *
     * @return 文件是否还在使用
     */
    public boolean isUnUsed() {
        return this.referenceCount.get() <= 0;
    }

    public int getNumber() {
        return number;
    }

    public long position() throws IOException {
        return this.channel.position();
    }

    public void truncate(long size) throws IOException {
        this.channel.truncate(size);
    }

    public long getLength() throws IOException {
        return this.channel.size();
    }

    /**
     * 删除文件
     *
     * @return 是否删除成功
     * @throws IOException
     */
    public boolean delete() throws IOException {
        this.close();
        return this.file.delete();
    }
}
