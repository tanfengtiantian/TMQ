package io.kafka.transaction;

import java.util.concurrent.ConcurrentHashMap;

public interface TransactionSessionContext {

    ConcurrentHashMap<TransactionId, Transaction> getTransactions();
}
