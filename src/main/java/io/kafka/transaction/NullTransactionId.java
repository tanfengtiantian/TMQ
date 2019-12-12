package io.kafka.transaction;

/**
 * null transactionId object
 *
 * @author tf
 * @date 2019-06-06
 *
 */
public class NullTransactionId extends TransactionId {

    /**
     *
     */
    private static final long serialVersionUID = -4764469311016709764L;


    NullTransactionId() {

    }


    @Override
    public boolean isNull() {
        return true;
    }


    @Override
    public boolean isXATransaction() {

        return false;
    }


    @Override
    public boolean isLocalTransaction() {

        return false;
    }


    @Override
    public String getTransactionKey() {
        return "null";
    }

}