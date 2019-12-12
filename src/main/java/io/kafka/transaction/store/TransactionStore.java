package io.kafka.transaction.store;


import io.kafka.log.ILog;
import io.kafka.message.ByteBufferMessageSet;
import io.kafka.transaction.TransactionId;
import java.io.IOException;
import java.nio.ByteBuffer;
import io.kafka.transaction.store.JournalTransactionStore.Tx;

/**
 * 事务存储引擎
 *
 * @author tf
 * @date 2019-6-18
 *
 */
public interface TransactionStore {

    /**
     * 准备提交
     * @param txid
     * @throws IOException
     */
    void prepare(TransactionId txid) throws IOException;

    /**
     * 提交
     * @param txid
     * @param wasPrepared
     * @throws IOException
     */
    void commit(TransactionId txid, boolean wasPrepared) throws IOException;

    /**
     * 重放事务日志.已提交移除准备队列
     * @param txid
     * @param wasPrepared
     * @return
     * @throws IOException
     */
    Tx replayCommit(final TransactionId txid, final boolean wasPrepared) throws IOException;

    /**
     * 事务回滚
     * @param txid
     * @throws IOException
     */
    void rollback(TransactionId txid) throws IOException;

    /**
     * 返回tx实例
     * @param txid
     * @return
     */
    Tx getInflyTx(final TransactionId txid);

    /**
     * 回放事务快照
     * @throws Exception
     */
    void load() throws Exception;

    /**
     * 添加事务消息
     * @param log
     * @param msgId
     * @param xid
     * @param fullbuf
     * @param message
     * @param location
     * @throws IOException
     */
    void addMessage(ILog log, long msgId, TransactionId xid, ByteBuffer fullbuf, ByteBufferMessageSet message, JournalLocation location) throws IOException;
}
