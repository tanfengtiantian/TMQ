package io.kafka.core;


import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.TimeUnit;

public class DispatcherFactory {

    public static Dispatcher newDispatcher(final int size, final RejectedExecutionHandler rejectedExecutionHandler) {
        if (size > 0) {
            return new PoolDispatcher(size, 60, TimeUnit.SECONDS, rejectedExecutionHandler);
        }
        else {
            return null;
        }
    }


    public static Dispatcher newDispatcher(final int size, final String prefix,
                                           final RejectedExecutionHandler rejectedExecutionHandler) {
        if (size > 0) {
            return new PoolDispatcher(size, 60, TimeUnit.SECONDS, prefix, rejectedExecutionHandler);
        }
        else {
            return null;
        }
    }

}