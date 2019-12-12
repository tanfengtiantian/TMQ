package io.kafka.ttl;

import io.kafka.config.ServerConfig;
import io.kafka.log.ILog;
import io.kafka.log.ILogManager;
import io.kafka.message.ByteBufferMessageSet;
import io.kafka.message.FileMessageSet;
import io.kafka.transaction.store.JournalFileMessageSet;
import io.kafka.transaction.store.JournalLocation;
import io.kafka.utils.FileUtils;
import io.kafka.utils.Utils;
import io.kafka.utils.nettyloc.ByteBuf;
import io.kafka.utils.nettyloc.PooledByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DelayStore implements DelayMessageStore , Closeable {

    private static final Logger log = LoggerFactory.getLogger(DelayStore.class);
    public static final short START_STATE = 0; // can go to: 0,1- 0,2  延迟消息未执行状态
    public static final short FINISHED_STATE = 1;//延迟消息未执行完成状态
    public static final short ERROR_STATE = 2;//延迟消息未执行错误状态
    private PooledByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;
    private final Lock writeLock = new ReentrantLock();
    private final ConcurrentHashMap<Integer/* number */, JournalFileMessageSet> dataFiles =
            new ConcurrentHashMap<>();
    final File delayDir;
    final WheelTimerDelay wheelTimerDelay;
    final ILogManager logManager;
    private JournalFileMessageSet currDataFile;
    private final AtomicInteger number = new AtomicInteger(0);
    private final String FILE_PREFIX = "delay.";

    // 每个日志文件最大大小为64M,,67108864
    static int MAX_FILE_SIZE = 67108864;

    // 日志刷盘设置，0表示让操作系统决定，1表示每次commit都刷盘，2表示每隔一秒刷盘一次
    final int flushTxLogAtCommit = 2;

    private ScheduledExecutorService scheduledExecutorService;
    // 修改过的datafile列表
    private final Set<JournalFileMessageSet> modifiedDataFiles = new HashSet<>();

    public DelayStore(ILogManager logManager,WheelTimerDelay wheelTimerDelay, ServerConfig config){
        this.logManager = logManager;
        this.wheelTimerDelay = wheelTimerDelay;
        //不存在创建目录
        FileUtils.makesureDir(new File(config.getLogDir()));
        this.delayDir = new File(config.getLogDir() + File.separator + "delay");
        FileUtils.makesureDir(this.delayDir);
        //每秒刷盘
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                DelayStore.this.force();
            }
            catch (final IOException e) {
                log.error("force datafiles failed", e);
            }

        }, 1, 1, TimeUnit.SECONDS);
    }

    @Override
    public ByteBuf directBuffer(ILog log, ByteBufferMessageSet message) {
        ByteBuf buf = allocator.directBuffer(
                4//size
                        + Utils.caculateShortString(log.getTopicName())//Len(topic) + topic
                        +4//partition
                        +8//ttl
                        +2//state
                        +4//messageSize
                        + (int)message.getSizeInBytes());//message
        return buf;
    }

    @Override
    public ByteBuffer addMessage(ByteBuf buf, ILog log, ByteBufferMessageSet message, long delay) {
        //size Len(topic) + topic + partition + ttl + state + messageSize + message
        ByteBuffer buffer = ByteBuffer.allocate(buf.capacity());
        buffer.putInt(buf.capacity() - 4);
        Utils.writeShortString(buffer, log.getTopicName());
        buffer.putInt(log.getPartition());
        buffer.putLong(delay);
        buffer.putShort(START_STATE);
        //messages
        final ByteBuffer sourceBuffer = message.serialized();
        buffer.putInt(sourceBuffer.limit());
        buffer.put(sourceBuffer);
        sourceBuffer.rewind();
        //write 直接内存
        buf.writeBytes(buffer.array());
        buffer.rewind();
        return buffer;
    }

    @Override
    public JournalLocation addMessageAndWritten(ByteBuf buf, ILog log, ByteBufferMessageSet message, long delay) throws IOException {
        //size Len(topic) + topic + partition + ttl + state + messageSize + message
        ByteBuffer buffer = addMessage(buf,log,message,delay);
        JournalFileMessageSet dataFile = null;
        this.writeLock.lock();
        try {
            dataFile = this.getDataFile(null);
            long[] writtenAndOffset = dataFile.append(buffer);
            this.modifiedDataFiles.add(dataFile);
            this.maybeRoll(dataFile);
            return new JournalLocation(dataFile.getNumber(), writtenAndOffset[1]);
        }finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public ByteBufferMessageSet readDirectBytesAndRelease(ByteBuf buf, JournalLocation location) throws IOException {
        byte[] bt = new byte[buf.capacity()];
        buf.readBytes(bt);
        ByteBuffer buffer = ByteBuffer.wrap(bt);
        //size
        int size = buffer.getInt();
        String topic = Utils.readShortString(buffer);
        int partition = buffer.getInt();
        long ttl = buffer.getLong();
        buffer.mark();
        short state = buffer.getShort();
        int messageSetSize = buffer.getInt();
        //创建子缓冲区
        ByteBuffer messageSetBuffer = buffer.slice();
        messageSetBuffer.limit(messageSetSize);
        buffer.position(buffer.position() + messageSetSize);
        //回写磁盘状态,验证文件滚动
        JournalFileMessageSet dataFile = null;
        this.writeLock.lock();
        try {
            //恢复的mark的位置
            buffer.reset();
            buffer.putShort(FINISHED_STATE);
            //位置重置为0，上限值不会改变，上限值还是重置前的值
            buffer.rewind();
            dataFile = this.getDataFile(location);
            dataFile.write(location.offset,buffer);
            // 递减计数
            dataFile.decrement();
            this.modifiedDataFiles.add(dataFile);
            this.maybeRoll(dataFile);
            return new ByteBufferMessageSet(messageSetBuffer);
        }finally {
            this.writeLock.unlock();
            //释放内存
            buf.release();
        }
    }

    private JournalFileMessageSet getDataFile(final JournalLocation location) throws IOException {
        JournalFileMessageSet dataFile = null;
        if (location == null) {
            dataFile = this.currDataFile;
            // 如果文件超过大小，则生成一个新文件
            if (dataFile.getSizeInBytes() > MAX_FILE_SIZE) {
                dataFile = this.newDataFile();
            }
            // 递增计数
            dataFile.increment();
        }
        else {
            dataFile = this.dataFiles.get(location.number);
        }
        return dataFile;
    }

    /**
     * 生成一个新的数据文件
     *
     * @throws IOException
     */
    protected JournalFileMessageSet newDataFile() throws IOException {
        final int n = this.number.incrementAndGet();
        this.currDataFile =
                new JournalFileMessageSet(new File(this.delayDir + File.separator + this.FILE_PREFIX + n), n ,true);
        this.dataFiles.put(Integer.valueOf(n), this.currDataFile);
        log.info("Created a new delay log：" + this.currDataFile);
        return this.currDataFile;
    }

    private void maybeRoll(final JournalFileMessageSet dataFile) throws IOException {
        // 文件超过大小并且不再被引用，则删除之
        if (dataFile.getSizeInBytes() > MAX_FILE_SIZE && dataFile.isUnUsed()) {
            // 如果要删除的是当前文件，需要生成一个新文件
            if (dataFile == this.currDataFile) {
                this.newDataFile();
            }
            this.dataFiles.remove(dataFile.getNumber());
            dataFile.delete();
        }
    }

    private int getFileNumber(final File file) {
        final int number = Integer.parseInt(file.getName().substring(this.FILE_PREFIX.length()));
        return number;
    }

    @Override
    public void recover() throws IOException {
        log.info("Begin to recover delay...");
        //返回后缀包含redo. 文件列表
        final File[] ls = this.delayDir.listFiles((dir, name) -> name.startsWith(this.FILE_PREFIX));
        // 按照序号升序排序
        Arrays.sort(ls, Comparator.comparingInt(this::getFileNumber));
        // 4个字节的长度buffer
        JournalFileMessageSet dataFile = null;
        final ByteBuffer lenBuf = ByteBuffer.allocate(4);
        for (int i = 0; i < ls.length; i++) {
            final File file = ls[i];
            if (file.isFile() && file.canRead()) {
                dataFile = this.recoverFile(ls, lenBuf, i, file);
            }
            else {
                log.info(file.getName() + " is not a valid delay store file");
            }
        }
        if (dataFile == null) {
            this.currDataFile = this.newDataFile();
        }
        else {
            this.currDataFile = dataFile;
            this.number.set(dataFile.getNumber());
        }
        log.info("Recover delay successfully");
    }
    private JournalFileMessageSet recoverFile(File[] ls, ByteBuffer lenBuf, int i, final File file) throws IOException {
        final int number = this.getFileNumber(file);
        // 读数据的起点
        long readOffset = 0;
        final JournalFileMessageSet dataFile = new JournalFileMessageSet(file, number,true);
        final long startMs = System.currentTimeMillis();
        while (true) {
            lenBuf.clear();
            final long cmdOffset = readOffset;
            dataFile.read(lenBuf, readOffset);
            if (!lenBuf.hasRemaining()) {
                lenBuf.flip();
                final int cmdBufLen = lenBuf.getInt();
                final ByteBuffer cmdBuf = ByteBuffer.allocate(cmdBufLen);
                dataFile.read(cmdBuf, 4 + readOffset);
                if (!cmdBuf.hasRemaining()) {
                    cmdBuf.flip();
                    try {
                        this.processCmd(number, cmdOffset, cmdBuf, dataFile);
                    }
                    catch (final Exception e) {
                        log.error("recoverFile failed", e);
                        // 回放失败，跳出循环，后续的事务日志将被truncate
                        break;
                        // throw new IllegalStateException(e);
                    }
                    readOffset += 4;
                    readOffset += cmdBufLen;
                }
                else {
                    // 没读满cmdBuf，跳出循环
                    break;
                }
            }
            else {
                // 没读满lenBuf，跳出循环
                break;
            }
        }
        // 最后一个命令不完整，truncate掉
        long truncated = 0;
        if (readOffset != dataFile.position()) {
            truncated = dataFile.position() - readOffset;
            dataFile.truncate(readOffset);
            log.info("Recovery truncated delay " + file.getAbsolutePath() + " bytes.");
        }
        log.info("Recovery delay " + file.getAbsolutePath() + " succeeded in "
                + (System.currentTimeMillis() - startMs) / 1000 + " seconds. " + truncated + " bytes.");

        if (dataFile.getLength() > MAX_FILE_SIZE && dataFile.isUnUsed()) {
            dataFile.delete();
            return null;
        }
        else {
            this.dataFiles.put(number, dataFile);
        }
        return dataFile;
    }

    private void processCmd(int number, long offset, ByteBuffer buffer, JournalFileMessageSet dataFile) throws IOException {
        String topic = Utils.readShortString(buffer);
        int partition = buffer.getInt();
        long ttl = buffer.getLong();
        short state = buffer.getShort();
        int messageSetSize = buffer.getInt();
        //创建子缓冲区
        ByteBuffer messageSetBuffer = buffer.slice();
        messageSetBuffer.limit(messageSetSize);
        buffer.position(buffer.position() + messageSetSize);
        if(state == START_STATE){
            dataFile.increment();
            ILog log = logManager.getOrCreateLog(topic,partition);
            wheelTimerDelay.addMessage(log,
                    new ByteBufferMessageSet(messageSetBuffer),
                    (ttl - System.currentTimeMillis()) / 1000,new JournalLocation(number,offset));
        }
    }

    private void force() throws IOException {
        this.writeLock.lock();
        try {
            for (final JournalFileMessageSet df : this.modifiedDataFiles) {
                df.flush();
            }
        }
        finally {
            try {
                this.modifiedDataFiles.clear();
            }
            finally {
                this.writeLock.unlock();
            }
        }
    }

    @Override
    public void close()  {
        this.writeLock.lock();
        try {
            for (final FileMessageSet df : this.dataFiles.values()) {
                try {
                    df.close();
                }
                catch (final Exception e) {
                    log.warn("close error:" + df, e);
                }
            }
            this.dataFiles.clear();
            this.currDataFile = null;
        }
        finally {
            this.writeLock.unlock();
        }
    }
}
