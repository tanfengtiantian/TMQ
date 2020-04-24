package io.kafka.network.session;

import io.kafka.network.request.Request;
import java.io.IOException;
import java.nio.channels.SelectionKey;

public interface SessionEvent {

    void onRead(final SelectionKey key) throws IOException;

    void onMessage(NioSession session, Request request) throws IOException;

    void OnWrite(SelectionKey key) throws IOException;

    void onClosed(SelectionKey key);
}
