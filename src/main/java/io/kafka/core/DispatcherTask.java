package io.kafka.core;

public class DispatcherTask {
    private Dispatcher readEventDispatcher, dispatchMessageDispatcher, writeEventDispatcher;

    public void setReadEventDispatcher(final Dispatcher dispatcher) {
        final Dispatcher oldDispatcher = this.readEventDispatcher;
        this.readEventDispatcher = dispatcher;
        if (oldDispatcher != null) {
            oldDispatcher.stop();
        }
    }
    public Dispatcher getReadEventDispatcher() {
        return this.readEventDispatcher;
    }

    public void setWriteEventDispatcher(final Dispatcher dispatcher) {
        final Dispatcher oldDispatcher = this.writeEventDispatcher;
        this.writeEventDispatcher = dispatcher;
        if (oldDispatcher != null) {
            oldDispatcher.stop();
        }
    }

    public Dispatcher getWriteEventDispatcher() {
        return this.writeEventDispatcher;
    }



    public void setDispatchMessageDispatcher(final Dispatcher dispatcher) {
        final Dispatcher oldDispatcher = this.dispatchMessageDispatcher;
        this.dispatchMessageDispatcher = dispatcher;
        if (oldDispatcher != null) {
            oldDispatcher.stop();
        }
    }

    public Dispatcher getDispatchMessageDispatcher() {
        return this.dispatchMessageDispatcher;
    }
}
