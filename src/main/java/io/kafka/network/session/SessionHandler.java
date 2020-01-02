package io.kafka.network.session;

public interface SessionHandler {

    void onSessionCreated(NioSession session);


    void onSessionStarted(NioSession session);


    void onSessionClosed(NioSession session);


    void onMessageReceived(NioSession session, Object msg);


    void onMessageSent(NioSession session, Object msg);


    void onExceptionCaught(NioSession session, Throwable throwable);


    void onSessionExpired(NioSession session);


    void onSessionIdle(NioSession session);


    void onSessionConnected(NioSession session, Object... args);
}
