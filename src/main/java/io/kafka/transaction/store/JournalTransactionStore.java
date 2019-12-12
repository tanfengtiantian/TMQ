package io.kafka.transaction.store;

import io.kafka.config.ServerConfig;
import io.kafka.log.ILog;
import io.kafka.log.ILogManager;
import io.kafka.message.ByteBufferMessageSet;
import io.kafka.transaction.AppendCallback;
import io.kafka.transaction.TransactionId;
import io.kafka.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;


/**
 * 事务存储引擎
 *
 * @author tf
 * @date 2019-6-18
 *
 */
public class JournalTransactionStore implements TransactionStore {

    private final static Logger logger = LoggerFactory.getLogger(JournalTransactionStore.class);
    private final JournalStore journalStore;
    private final ILogManager logManager;
    private final Map<TransactionId, Tx> inflightTransactions = new LinkedHashMap<>();
    private final Map<TransactionId, Tx> preparedTransactions = new LinkedHashMap<>();

    public JournalTransactionStore(ServerConfig config, ILogManager logManager){
        this.logManager = logManager;
        this.journalStore = new JournalStore(config,logManager,1,this);
    }

    @Override
    public void load() throws Exception {
        journalStore.load();
    }

    @Override
    public void prepare(TransactionId txid) throws IOException {
        Tx tx = null;
        synchronized (this.inflightTransactions) {
            tx = this.inflightTransactions.remove(txid);
        }
        if (tx == null) {
            return;
        }
        synchronized (this.preparedTransactions) {
            this.preparedTransactions.put(txid, tx);
        }
    }
    @Override
    public Tx replayCommit(final TransactionId txid, final boolean wasPrepared) throws IOException {
        if (wasPrepared) {
            synchronized (this.preparedTransactions) {
                return this.preparedTransactions.remove(txid);
            }
        }
        else {
            synchronized (this.inflightTransactions) {
                return this.inflightTransactions.remove(txid);
            }
        }
    }
    @Override
    public void commit(TransactionId txid, boolean wasPrepared) {
        final Tx tx;
        if (wasPrepared) {
            synchronized (this.preparedTransactions) {
                tx = this.preparedTransactions.remove(txid);
            }
        }
        else {
            synchronized (this.inflightTransactions) {
                tx = this.inflightTransactions.remove(txid);
            }
        }
        if (tx == null) {
            return;
        }
        final Map<ILog, List<Long>> msgIds = tx.getMsgIds();
        final Map<ILog, List<ByteBufferMessageSet>> putCommands = tx.getPutCommands();
        final int count = msgIds.size();
        final Map<String, AddMsgLocation> locations =
                new LinkedHashMap<>();
        for (final Map.Entry<ILog, List<Long>> entry : msgIds.entrySet()) {
            final ILog msgStore = entry.getKey();
            final List<Long> ids = entry.getValue();
            final List<ByteBufferMessageSet> mess = putCommands.get(msgStore);
            msgStore.append(mess, (location, buffer) -> {
                final long checkSum = Utils.crc32(buffer.array());
                //top-partition
                final String description = msgStore.getDescription();
                // Store append location
                synchronized (locations) {
                    locations.put(description,new AddMsgLocation(location.getOffset(),location.getLength(),checkSum,description));
                    // 处理完成
                    if (locations.size() == count) {
                        // 将位置信息序列化，并作为tx
                        // command的附加数据存储，这部分数据的长度是固定的，因此可以在replay的时候更改
                        final ByteBuffer localtionBytes = AddMsgLocationUtils.encodeLocation(locations);
                        // 记录commit日志，并附加位置信息
                        try {

                            journalStore.committedOrRollback(txid, localtionBytes, tx.location);
                        }
                        catch (final IOException e) {
                            throw new RuntimeException("Write tx log failed "+e.getMessage(), e);
                        }
                    }
                }
            });
        }
    }

    @Override
    public void rollback(TransactionId txid) throws IOException {
        if (logger.isDebugEnabled()) {
            logger.debug("rollback: " + txid);
        }

        Tx tx = null;
        synchronized (this.inflightTransactions) {
            tx = this.inflightTransactions.remove(txid);
        }
        if (tx == null) {
            synchronized (this.preparedTransactions) {
                tx = this.preparedTransactions.remove(txid);
            }
        }
        if(tx != null) {
            //写入redo.1日志
            this.journalStore.committedOrRollback(txid, null, tx.location);
        }
    }

    @Override
    public void addMessage(ILog log, long msgId, TransactionId xid, ByteBuffer fullbuf, ByteBufferMessageSet message, JournalLocation location) throws IOException {
        if (location == null) {
            final Tx tx = this.getInflyTx(xid);
            if (tx != null) {
                location = this.journalStore.write(msgId, fullbuf, tx.location);
            }
            else {
                location = this.journalStore.write(msgId, fullbuf, null);
            }
        }
        final Tx tx = this.getTx(xid, location);
        tx.add(log, msgId, message);
    }

    @Override
    public Tx getInflyTx(final TransactionId txid) {
        synchronized (this.inflightTransactions) {
            return this.inflightTransactions.get(txid);
        }
    }

    public Tx getTx(final TransactionId txid, final JournalLocation location) {
        synchronized (this.inflightTransactions) {
            Tx tx = this.inflightTransactions.get(txid);
            if (tx == null) {
                tx = new Tx(location);
                this.inflightTransactions.put(txid, tx);
            }
            return tx;
        }
    }

    /**
     * 事务内存对象，保存操作轨迹
     *
     * @author tf
     * @date 2019-6-25
     *
     */
    public static class Tx {

        private final JournalLocation location;
        private final ConcurrentHashMap<ILog, Queue<AddMsgOperation>> operations =
                new ConcurrentHashMap<>();

        public Tx(final JournalLocation location) {
            this.location = location;
        }

        JournalLocation getLocation() {
            return this.location;
        }

        public void add(ILog log, long msgId, ByteBufferMessageSet messageSet) {
            final AddMsgOperation addMsgOperation = new AddMsgOperation(log,msgId,messageSet);
            Queue<AddMsgOperation> ops = this.operations.get(log);
            if (ops == null) {
                ops = new ConcurrentLinkedQueue<>();
                //若不包含key，则放入value返回null。若包含key，则返回key对应的value 解决并发已第一个放入的为准
                final Queue<AddMsgOperation> oldOps = this.operations.putIfAbsent(log, ops);
                if (oldOps != null) {
                    ops = oldOps;
                }
            }
            ops.add(addMsgOperation);
        }

        public Map<ILog, List<Long>> getMsgIds() {
            final Map<ILog, List<Long>> rt = new LinkedHashMap<>();
            for (final Map.Entry<ILog, Queue<AddMsgOperation>> entry : this.operations.entrySet()) {
                final ILog store = entry.getKey();
                final Queue<AddMsgOperation> opQueue = entry.getValue();
                final List<Long> ids = new ArrayList<Long>();
                rt.put(store, ids);
                for (final AddMsgOperation to : opQueue) {
                    ids.add(to.msgId);
                }
            }
            return rt;
        }

        public Map<ILog, List<ByteBufferMessageSet>> getPutCommands() {
            final Map<ILog, List<ByteBufferMessageSet>> rt = new LinkedHashMap<>();
            for (final Map.Entry<ILog, Queue<AddMsgOperation>> entry : this.operations.entrySet()) {
                final ILog store = entry.getKey();
                final Queue<AddMsgOperation> opQueue = entry.getValue();
                final List<ByteBufferMessageSet> ids = new ArrayList<>();
                rt.put(store, ids);
                for (final AddMsgOperation to : opQueue) {
                    ids.add(to.messageSet);
                }
            }
            return rt;
        }

        public Map<ILog, Queue<AddMsgOperation>> getOperations() {
            return this.operations;
        }
    }

    public static class AddMsgOperation {
        public ILog store;
        public long msgId;
        public ByteBufferMessageSet messageSet;


        public AddMsgOperation(final ILog store, final long msgId, final ByteBufferMessageSet messageSet) {
            super();
            this.store = store;
            this.msgId = msgId;
            this.messageSet = messageSet;
        }
    }

    /**
     * 添加消息到store的位置和checksum
     *
     * @author tf
     * @date 2019-6-28
     *
     */
    public static class AddMsgLocation extends Location {
        public final long checksum; // 校验和，整个消息的校验和，注意跟message的校验和区分

        public final String storeDesc; // 分区描述字符串


        public AddMsgLocation(final long offset, final int length, final long checksum, final String storeDesc) {
            super(offset, length);
            this.checksum = checksum;
            this.storeDesc = storeDesc;
        }

        private ByteBuffer buf;


        public static AddMsgLocation decode(final ByteBuffer buf) {
            if (!buf.hasRemaining()) {
                return null;
            }
            final long offset = buf.getLong();
            final int length = buf.getInt();
            final long checksum = buf.getLong();
            final int descLen = buf.getInt();
            final byte[] descBytes = new byte[descLen];
            buf.get(descBytes);
            final String desc = Utils.toString(descBytes);
            return new AddMsgLocation(offset, length, checksum, desc);
        }


        /**
         * 消息位置序列化为:
         * <ul>
         * <li>8个字节的offset</li>
         * <li>4个字节的长度</li>
         * <li>4个字节checksum:这是指整个消息存储数据的checksum，跟message的checksum不同</li>
         * <li>4个字节长度，存储的分区名的长度</li>
         * <li>存储的分区名</li>
         * </ul>
         *
         * @return
         */
        public ByteBuffer encode() {
            if (this.buf == null) {
                final byte[] storeDescBytes = Utils.getBytes(this.storeDesc);
                final ByteBuffer buf = ByteBuffer.allocate(8 + 4 + 8 + 4 + this.storeDesc.length());
                buf.putLong(this.getOffset());
                buf.putInt(this.getLength());
                buf.putLong(this.checksum);
                buf.putInt(storeDescBytes.length);
                buf.put(storeDescBytes);
                buf.flip();
                this.buf = buf;
            }
            return this.buf;
        }

    }
}
