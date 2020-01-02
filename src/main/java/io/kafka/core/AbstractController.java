package io.kafka.core;

import java.util.concurrent.CopyOnWriteArrayList;

public abstract class AbstractController implements Controller, ControllerLifeCycle {

    /**
     * controller state listener list
     */
    protected CopyOnWriteArrayList<ControllerStateListener> stateListeners =
            new CopyOnWriteArrayList<ControllerStateListener>();


    @Override
    public void notifyReady() {
        for (final ControllerStateListener stateListener : this.stateListeners) {
            stateListener.onReady(this);
        }
    }

    @Override
    public void notifyStarted() {
        for (final ControllerStateListener stateListener : this.stateListeners) {
            stateListener.onStarted(this);
        }
    }


    @Override
    public final void notifyStopped() {
        for (final ControllerStateListener stateListener : this.stateListeners) {
            stateListener.onStopped(this);
        }
    }
}
