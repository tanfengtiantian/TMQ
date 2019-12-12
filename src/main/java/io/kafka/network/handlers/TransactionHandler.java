package io.kafka.network.handlers;

import io.kafka.api.RequestKeys;
import io.kafka.config.ServerConfig;
import io.kafka.log.ILog;
import io.kafka.network.send.TransactionSend;
import io.kafka.transaction.*;
import io.kafka.api.TransactionRequest;
import io.kafka.log.ILogManager;
import io.kafka.network.receive.Receive;
import io.kafka.network.send.Send;
import io.kafka.transaction.store.TransactionStore;
import io.kafka.utils.NamedThreadFactory;
import io.kafka.utils.timer.HashedWheelTimer;
import io.kafka.utils.timer.Timer;

import javax.transaction.xa.XAException;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author tf
 * @version 创建时间：2019年6月5日 下午2:12:20
 * @ClassName Transaction消息处理类
 *
 * </pre>
 */
public class TransactionHandler extends AbstractHandler implements TransactionSessionContext {

    private final ConcurrentHashMap<TransactionId, Transaction> transactionMap =
            new ConcurrentHashMap<TransactionId, Transaction>();
    private final TransactionStore transactionStore;
    private final ServerConfig config;
    private final IdWorker idWorker = new IdWorker(0);
    private final Timer txTimeoutTimer;
    // 并行超时事务默认3万个
    private int maxTxTimeoutTimerCapacity = 30000;

    public TransactionHandler(ILogManager logManager, TransactionStore transactionStore, ServerConfig config) {
        super(logManager);
        this.transactionStore = transactionStore;
        this.config=config;
        this.txTimeoutTimer =
                new HashedWheelTimer(new NamedThreadFactory("Tx-Timeout-Timer"), 500, TimeUnit.MILLISECONDS, 512,
                        maxTxTimeoutTimerCapacity);
    }

    @Override
    public Send handler(RequestKeys requestType, Receive receive) throws IOException {
        final long st = System.currentTimeMillis();
        TransactionRequest request = TransactionRequest.readFrom(receive.buffer());
        request.setBrokerId(config.getBrokerId());
        if (logger.isDebugEnabled()) {
            logger.debug("Transaction request " + request.toString());
        }
        Send send = handleTransactionRequest(request);
        long et = System.currentTimeMillis();
        if (logger.isDebugEnabled()) {
            logger.debug("解析消息耗时: " + (et - st) + " ms");
        }
        return send;
    }

    private Send handleTransactionRequest(TransactionRequest request) throws IOException {
        switch (request.getTransactionType()){
            case TransactionRequest.BeginTransaction:
                 beginTransaction(request);
                 return new TransactionSend(request);
            case TransactionRequest.PutCommand:
                 putCommand(request);
                 return new TransactionSend(request);
            case TransactionRequest.Rollback:
                 rollback(request);
                 return new TransactionSend(request);
            case TransactionRequest.Prepare:

            case TransactionRequest.Commit:
                commit(request);
                return new TransactionSend(request);
            default:
                throw new IllegalStateException("Unexpected TransactionType: " + request.getTransactionType());
        }

    }


    private void beginTransaction(TransactionRequest request) {
        final TransactionId xid = TransactionId.valueOf(request.getTransactionId());
        Transaction transaction = transactionMap.get(xid);
        if (transaction != null) {
            return;
        }
        //获取事务文件地址
        transaction = new LocalTransaction(this.transactionStore, (LocalTransactionId) xid, this,txTimeoutTimer);
        transactionMap.put(xid, transaction);
        if (transaction != null) {
            // 设置事务超时
            transaction.setTimeoutRef(request.getTransactionTimeout(),transaction);
        }
    }

    private void putCommand(TransactionRequest request) {
        final TransactionId xid = TransactionId.valueOf(request.getTransactionId());
        Transaction transaction = transactionMap.get(xid);
        if (transaction != null) {
            transaction.setTransactionInUse();
            int partition = request.getTranslatedPartition(logManager);
            try {
                final ILog log = logManager.getOrCreateLog(request.getTopic(), partition);
                if (log == null) {
                    return;
                }
                final long msgId = this.idWorker.nextId();
                this.transactionStore.addMessage(log, msgId, xid, request.getFullbuffer(), request.getMessages(),null);

            }  catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
    }

    private void rollback(TransactionRequest request) throws IOException {
        final TransactionId xid = TransactionId.valueOf(request.getTransactionId());
        Transaction transaction = transactionMap.get(xid);
        if (transaction != null) {
            transaction.rollback();
        }
    }

    private void commit(TransactionRequest request) throws IOException {
        final TransactionId xid = TransactionId.valueOf(request.getTransactionId());
        Transaction transaction = transactionMap.get(xid);
        if (transaction != null) {
            try {
                transaction.prepare();

                transaction.commit(true);
            }
            catch (final IOException e) {
                logger.warn("COMMIT FAILED: ", e);
                transaction.rollback();
            } catch (XAException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public ConcurrentHashMap<TransactionId, Transaction> getTransactions() {
        return transactionMap;
    }
}
