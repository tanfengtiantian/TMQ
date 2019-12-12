package io.kafka.transaction;

import io.kafka.utils.timer.Timeout;

import javax.transaction.xa.XAException;
import java.io.IOException;
import java.io.Serializable;

/**
 * 事务基类
 *
 * @author tf
 * @date 2019-6-13
 *
 */
public abstract class Transaction implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = -5907949950359568586L;
    public static final byte START_STATE = 0; // can go to: 1,2,3
    public static final byte IN_USE_STATE = 1; // can go to: 2,3
    public static final byte PREPARED_STATE = 2; // can go to: 3
    public static final byte FINISHED_STATE = 3;
    //XA事物预留
    public static final byte HEURISTIC_COMMIT_STATE = 4;
    public static final byte HEURISTIC_ROLLBACK_STATE = 5;
    public static final byte HEURISTIC_COMPLETE_STATE = 6;

    private volatile byte state = START_STATE;

    private volatile transient Timeout timeoutRef;

    public byte getState() {
        return this.state;
    }


    /**
     * 设置事务正在被使用中
     */
    public void setTransactionInUse() {
        if (this.state == START_STATE) {
            this.state = IN_USE_STATE;
        }
    }

    public void setState(final byte state) {
        if (state == FINISHED_STATE) {
            // 终止超时检测
            if (this.timeoutRef != null) {
                this.timeoutRef.cancel();
            }
        }
        this.state = state;
    }


    public void prePrepare() throws Exception {
        switch (this.state) {
            case START_STATE:
            case IN_USE_STATE:
            case PREPARED_STATE:
                break;
            default:
                final XAException xae = new XAException("Prepare cannot be called now.");
                xae.errorCode = XAException.XAER_PROTO;
                throw xae;
        }
    }

    public Timeout getTimeoutRef() {
        return this.timeoutRef;
    }

    public void setTimeoutRef(final Timeout timeoutRef) {
        this.timeoutRef = timeoutRef;
    }

    public abstract void commit(boolean onePhase) throws XAException, IOException;

    public abstract void rollback() throws IOException;

    public abstract void prepare() throws IOException;

    public abstract TransactionId getTransactionId();

    public boolean isPrepared() {
        return this.getState() == PREPARED_STATE;
    }

    public abstract void setTimeoutRef(int seconds,Transaction tx);
}