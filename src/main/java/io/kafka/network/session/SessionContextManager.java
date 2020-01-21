package io.kafka.network.session;

import java.util.ArrayList;
import java.util.List;

public class SessionContextManager {

    //private static ThreadLocal<List<SessionHandler>> CONTEXT = InheritableThreadLocal.withInitial(() -> new ArrayList<>());

    private static final List<SessionHandler> CONTEXT = new ArrayList<>();
    /**
     * 注册session监听器
     * @param handler
     */
    public static void registerHandler(SessionHandler handler) {
        CONTEXT.add(handler);
    }

    public static List<SessionHandler> getRegisterHandler() {
        return CONTEXT;
    }

}
