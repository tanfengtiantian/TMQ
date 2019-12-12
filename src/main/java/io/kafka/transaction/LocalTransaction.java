package io.kafka.transaction;

import io.kafka.transaction.store.TransactionStore;
import io.kafka.utils.timer.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.transaction.xa.XAException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * 本地事务实现
 *
 * @author tf
 * @date 2019-6-13
 *
 */
public class LocalTransaction extends Transaction {

    private static final long serialVersionUID = 3488724356970710207L;
    private static final Logger LOG = LoggerFactory.getLogger(LocalTransaction.class);
    private static final int txTimeoutInSeconds = 10;//最大事务超时时间
    //transient 不用被序列化
    private final transient TransactionStore transactionStore;
    private final LocalTransactionId xid;
    private final TransactionSessionContext sessionContext;
    private final Timer txTimeoutTimer;

    public LocalTransaction(final TransactionStore transactionStore, final LocalTransactionId xid,
                            final TransactionSessionContext sessionContext,final Timer timer) {
        this.transactionStore = transactionStore;
        this.xid = xid;
        this.sessionContext = sessionContext;
        this.txTimeoutTimer = timer;
    }


    @Override
    public void commit(final boolean onePhase) throws XAException, IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("commit: " + this.xid);
        }
        // Get ready for commit.
        try {
            this.prePrepare();
        }
        catch (final XAException e) {
            throw e;
        }
        catch (final Throwable e) {
            LOG.warn("COMMIT FAILED: ", e);
            this.rollback();
            // Let them know we rolled back.
            final XAException xae = new XAException("COMMIT FAILED: Transaction rolled back.");
            xae.errorCode = XAException.XA_RBOTHER;
            xae.initCause(e);
            throw xae;
        }
        this.setState(Transaction.FINISHED_STATE);
        this.sessionContext.getTransactions().remove(this.xid);
        try {
            this.transactionStore.commit(this.getTransactionId(), onePhase);
        }
        catch (final Throwable t) {
            LOG.warn("Store COMMIT FAILED: ", t);
            this.rollback();
            final XAException xae = new XAException("STORE COMMIT FAILED: Transaction rolled back.");
            xae.errorCode = XAException.XA_RBOTHER;
            xae.initCause(t);
            throw xae;
        }
    }


    @Override
    public void rollback() throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("rollback: " + this.xid);
        }
        //结束超时任务
        this.setState(Transaction.FINISHED_STATE);
        this.sessionContext.getTransactions().remove(this.xid);
        this.transactionStore.rollback(this.getTransactionId());
    }

    @Override
    public void prepare() throws IOException {
        this.setState(Transaction.PREPARED_STATE);
        transactionStore.prepare(this.getTransactionId());
    }


    @Override
    public TransactionId getTransactionId() {
        return this.xid;
    }

    @Override
    public void setTimeoutRef(int seconds,final Transaction tx) {
        // 0则表示永不超时
        // TODO 是否采用默认的最大超时时间？
        if (seconds <= 0) {
            return;
        }
        if (tx.getTimeoutRef() != null) {
            return;
        }
        if (seconds > txTimeoutInSeconds) {
            seconds = txTimeoutInSeconds;
        }

        this.setTimeoutRef(this.txTimeoutTimer.newTimeout(timeout -> {
            // 没有prepared的到期事务要回滚
            synchronized (tx) {
                if (!tx.isPrepared() && tx.getState() != Transaction.FINISHED_STATE) {
                    LOG.warn("transaction " + tx.getTransactionId() + " is timeout,it is rolled back.");
                    tx.rollback();
                }
            }
        }, seconds, TimeUnit.SECONDS));
    }
}

