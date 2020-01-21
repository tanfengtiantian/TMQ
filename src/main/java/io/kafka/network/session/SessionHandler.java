package io.kafka.network.session;

import io.kafka.api.RequestKeys;
import io.kafka.network.receive.Receive;
import io.kafka.network.send.Send;

/**
 * onCreated --> onSessionStarted
 */
public interface SessionHandler {

    void onSessionCreated(NioSession session);


    void onSessionStarted(NioSession session);


    void onMessageReceived(NioSession session, RequestKeys requestType, Receive receive);


    void onMessageSent(NioSession session, Send msg);


    void onExceptionCaught(NioSession session, Throwable throwable);


    void onSessionExpired(NioSession session);


    void onSessionIdle(NioSession session);

    void onSessionClosed(NioSession session);

    void onSessionConnected(NioSession session, Object... args);
}
