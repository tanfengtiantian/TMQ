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
    public RequestHandler mapping(RequestKeys id) {
        return selectKey(id);
    }

    public Decoder getDecoder(RequestKeys id) {
        Decoder decoder = selectKey(id);
        return decoder;
    }

    public <T> T selectKey(RequestKeys id) {
        switch (id) {
            case RPC:
                return (T)rpcHandler;
        }
        return null;
    }
}
