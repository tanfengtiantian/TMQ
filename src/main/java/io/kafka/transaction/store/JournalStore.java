package io.kafka.transaction.store;

import io.kafka.api.TransactionRequest;
import io.kafka.config.ServerConfig;
import io.kafka.log.ILog;
import io.kafka.log.ILogManager;
import io.kafka.message.ByteBufferMessageSet;
import io.kafka.message.FileMessageSet;
import io.kafka.transaction.AppendCallback;
import io.kafka.transaction.TransactionId;
import io.kafka.transaction.TxCommand;
import io.kafka.utils.FileUtils;
import io.kafka.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import io.kafka.transaction.store.JournalTransactionStore.Tx;
import io.kafka.transaction.store.JournalTransactionStore.AddMsgLocation;

import static java.lang.String.format;

public class JournalStore implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(JournalStore.class);
    public static final int APPEND_MSG = 0;
    public static final int TX_OP = 1;
    private final ILogManager logManager;

    private final ConcurrentHashMap<Integer/* number */, JournalFileMessageSet> dataFiles =
            new ConcurrentHashMap<>();

    private final AtomicInteger number = new AtomicInteger(0);

    private final File transactionsDir;

    private final TransactionStore transactionStore;

    private JournalFileMessageSet currDataFile;

    private final Lock writeLock = new ReentrantLock();

    private final String FILE_PREFIX = "redo.";

    private final Object lock = new Object();

    // 每个日志文件最大大小为64M,,67108864
    static int MAX_FILE_SIZE = 67108864;

    // 日志刷盘设置，0表示让操作系统决定，1表示每次commit都刷盘，2表示每隔一秒刷盘一次
    private final int flushTxLogAtCommit;


    public JournalStore(ServerConfig config, ILogManager logManager, int flushTxLogAtCommit, TransactionStore transactionStore) {
        this.logManager = logManager;
        //不存在创建目录
        FileUtils.makesureDir(new File(config.getLogDir()));
        this.transactionsDir = new File(config.getLogDir() + File.separator + "transactions");
        FileUtils.makesureDir(this.transactionsDir);
        this.flushTxLogAtCommit = flushTxLogAtCommit;
        this.transactionStore=transactionStore;
    }

    public void load() throws Exception {
        //恢复
        recover();
    }

    private void recover() throws IOException{
        log.info("Begin to recover transaction journal...");
        //返回后缀包含redo. 文件列表
        final File[] ls = this.transactionsDir.listFiles((dir, name) -> name.startsWith(JournalStore.this.FILE_PREFIX));
        // 按照序号升序排序
        //Arrays.sort(ls, (o1, o2) -> JournalStore.this.getFileNumber(o1) - JournalStore.this.getFileNumber(o2));
        Arrays.sort(ls, Comparator.comparingInt(JournalStore.this::getFileNumber));
        // 4个字节的长度buffer
        JournalFileMessageSet dataFile = null;
        final ByteBuffer lenBuf = ByteBuffer.allocate(4);
        for (int i = 0; i < ls.length; i++) {
            final File file = ls[i];
            if (file.isFile() && file.canRead()) {
                dataFile = this.recoverFile(ls, lenBuf, i, file);
            }
            else {
                log.info(file.getName() + " is not a valid transaction journal store file");
            }
        }
        if (dataFile == null) {
            this.currDataFile = this.newDataFile();
        }
        else {
            this.currDataFile = dataFile;
            this.number.set(dataFile.getNumber());
        }
        log.info("Recover transaction journal successfully");
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
                        log.error("Process tx command failed", e);
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
        }
        log.info("Recovery transaction journal " + file.getAbsolutePath() + " succeeded in "
                + (System.currentTimeMillis() - startMs) / 1000 + " seconds. " + truncated + " bytes truncated.");

        if (dataFile.getLength() > MAX_FILE_SIZE && dataFile.isUnUsed()) {
            dataFile.delete();
            return null;
        }
        else {
            this.dataFiles.put(number, dataFile);
        }
        return dataFile;
    }

    private int processCmd(int number, long offset, ByteBuffer cmdBuf, JournalFileMessageSet dataFile) {
        TxCommand cmd = TxCommand.parseFrom(cmdBuf);
        if (cmd != null) {
            switch (cmd.getCmdType()) {
                case APPEND_MSG:
                    return this.appendMsg(number, offset, cmd, dataFile);
                case TX_OP:
                    int xidlen = Utils.caculateShortString(cmd.getXid().getTransactionKey());
                    // 附加数据的索引位置，起点＋4个字节的offset + size + cmdType + xid + attachmentLen
                    long attachmentoffset = offset + 4 + 2 + xidlen + 4;
                    return this.replayTx(attachmentoffset, cmd, dataFile);
            }
        }
        return 0;
    }



    private int appendMsg(int number, long offset, TxCommand cmd, JournalFileMessageSet dataFile) {
        TransactionRequest request = cmd.getTransactionRequest();
        final TransactionId xid = TransactionId.valueOf(request.getTransactionId());
        try {
            final ILog log = logManager.getOrCreateLog(request.getTopic(), request.getPartition());
            if (log == null) {
                return 0;
            }
            if (this.transactionStore.getInflyTx(xid) == null){
                dataFile.increment();
            }
            this.transactionStore.addMessage(log,cmd.getMsgId(),xid,null,request.getMessages(),new JournalLocation(number, offset));

        }  catch (Exception e) {

        }
        return 0;
    }

    private int replayTx(long dataOffset, TxCommand cmd, JournalFileMessageSet dataFile) {
        final TransactionId xid = cmd.getXid();
        try {
            // 重放事务日志
            final Tx tx = this.transactionStore.replayCommit(xid, false);
            if (tx == null) return 0;
            if (tx.getOperations().isEmpty()) { return 0; }

            final Map<ILog, List<Long>> ids = tx.getMsgIds();
            final Map<ILog, List<ByteBufferMessageSet>> putCmds = tx.getPutCommands();

            // commit buf
            final ByteBuffer buf = cmd.getAttachment();
            // 获取附加数据，添加消息的位置信息
            final int attachmentLen = buf.capacity();

            final Map<String, AddMsgLocation> locations = AddMsgLocationUtils.decodeLocations(buf);

            final AtomicBoolean replayed = new AtomicBoolean(false);
            final AtomicInteger counter = new AtomicInteger();
            if (ids != null && !ids.isEmpty()) {
                for (final Map.Entry<ILog, List<Long>> entry : ids.entrySet()) {
                    final ILog msgStore = entry.getKey();
                    final AddMsgLocation addedLocation = locations.get(msgStore.getDescription());

                    final List<Long> idList = entry.getValue();
                    final List<ByteBufferMessageSet> cmdList = putCmds.get(msgStore);
                    // 没有添加消息，需要重新添加
                    // ps 数据添加成功了，才会有事务commit日志，失败直接rollback讲道理不太可能
                    if (addedLocation == null) {
                        throw new IllegalStateException("error-重放事务日志失败:分区["+msgStore.getDescription()+"]");
                    } else {
                        // 尝试重放
                        counter.incrementAndGet();
                        msgStore.replayAppend(addedLocation.getOffset(), addedLocation.getLength(),
                                addedLocation.checksum, idList, cmdList, (newLocation, buffer) -> {
                                    // 如果重放的时候更新了位置，则需要更新位置信息
                                    if (newLocation != null) {
                                        replayed.set(true);
                                        locations.put(msgStore.getDescription(),
                                                new AddMsgLocation(newLocation.getOffset(), newLocation.getLength(),
                                                        addedLocation.checksum, addedLocation.storeDesc));
                                    }
                                    counter.decrementAndGet();
                                });
                    }
                }
            } //end if

            // 如果有重放，覆写位置信息
            if (replayed.get()) {
                // 等待回调完成
                while (counter.get() > 0) {
                    Thread.sleep(50);
                }

                dataFile.write(dataOffset, AddMsgLocationUtils.encodeLocation(locations));
                dataFile.flush();
            }
            // 返回附加数据大小
            dataFile.decrement();
            return attachmentLen;

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {

        }
        return 0;
    }

    private int getFileNumber(final File file) {
        final int number = Integer.parseInt(file.getName().substring(this.FILE_PREFIX.length()));
        return number;
    }
    /**
     * 生成一个新的数据文件
     *
     * @throws FileNotFoundException
     */
    protected JournalFileMessageSet newDataFile() throws IOException {
        final int n = this.number.incrementAndGet();
        this.currDataFile =
                new JournalFileMessageSet(new File(this.transactionsDir + File.separator + this.FILE_PREFIX + n), n ,true);
        this.dataFiles.put(Integer.valueOf(n), this.currDataFile);
        log.info("Created a new redo log：" + this.currDataFile);
        return this.currDataFile;
    }


    @Override
    public void close() throws IOException {
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

    public void committedOrRollback(TransactionId xid, final ByteBuffer attachment, final JournalLocation location) throws IOException {
        synchronized (lock) {
            final int xidLen = Utils.caculateShortString(xid.getTransactionKey());
            final int attachmentLen= (attachment != null ? attachment.remaining() : 0);
            final ByteBuffer buffer = ByteBuffer.allocate(
                      4             // size
                    + 2             // type
                    + xidLen        // xid
                    + 4             // attachmentSize
                    + attachmentLen // byte
            );
            buffer.putInt(2 + xidLen + 4 + attachmentLen);
            buffer.putShort((short) 1);
            Utils.writeShortString(buffer, xid.getTransactionKey());
            buffer.putInt(attachmentLen);
            if (attachmentLen > 0){
                buffer.put(attachment.duplicate());
            }
            //读写切换
            buffer.flip();
            JournalFileMessageSet dataFile = null;
            this.writeLock.lock();
            try {
                dataFile = this.getDataFile(location);
                long[] writtenAndOffset = dataFile.append(new ByteBufferMessageSet(buffer));
                // 提交或者回滚，递减计数
                dataFile.decrement();
                this.maybeRoll(dataFile);
                new JournalLocation(dataFile.getNumber(), writtenAndOffset[1]);
            }
            catch (final Exception e) {
                log.info("Created a new redo log：" + dataFile.getNumber());
            } finally {
                this.writeLock.unlock();
                //刷盘
                if (dataFile != null)
                    dataFile.flush();
            }
        }
    }
    public JournalLocation write(long msgId, ByteBuffer fullbuf, JournalLocation location) throws IOException {
        synchronized (lock) {
            fullbuf.flip();//重读
            ByteBuffer buffer = ByteBuffer.allocate(4 + 2 + 8 + 4 + fullbuf.limit());
            buffer.putInt(2 + 8 + 4 + fullbuf.limit());
            buffer.putShort((short) 0);
            buffer.putLong(msgId);
            buffer.putInt(fullbuf.limit());
            buffer.put(fullbuf.duplicate());
            buffer.flip();

            JournalFileMessageSet dataFile = null;
            this.writeLock.lock();
            try {
                dataFile = this.getDataFile(location);
                long[] writtenAndOffset = dataFile.append(buffer);
                this.maybeRoll(dataFile);
                return new JournalLocation(dataFile.getNumber(), writtenAndOffset[1]);
            }finally {
                this.writeLock.unlock();
            }
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
}
