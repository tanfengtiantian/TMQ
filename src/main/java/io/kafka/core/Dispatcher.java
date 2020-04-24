package io.kafka.core;


public interface Dispatcher {

    void dispatch(Runnable r);

    void stop();
}
