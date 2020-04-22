package io.kafka.rpc;

import io.kafka.api.RequestKeys;
import io.kafka.config.ServerConfig;
import io.kafka.network.receive.Receive;
import io.kafka.network.request.RequestHandler;
import io.kafka.network.request.RequestHandlerFactory;

public class RpcRequestHandlers implements RequestHandlerFactory {

    private final RpcHandler rpcHandler;

    public RpcRequestHandlers(ServerConfig config) {
        rpcHandler = new RpcHandler(config);
    }

    @Override
    public RequestHandler mapping(RequestKeys id, Receive request) {
        switch (id) {
            case RPC:
                return rpcHandler;
        }
        return null;
    }
}
