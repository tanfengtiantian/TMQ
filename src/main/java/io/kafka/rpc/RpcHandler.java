package io.kafka.rpc;

import io.kafka.api.RequestKeys;
import io.kafka.api.RpcRequest;
import io.kafka.common.ErrorMapping;
import io.kafka.config.ServerConfig;
import io.kafka.network.request.Request;
import io.kafka.network.request.RequestHandler;
import io.kafka.network.request.RequestHandlerFactory;
import io.kafka.network.send.Send;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;

public class RpcHandler implements RequestHandler, RequestHandlerFactory.Decoder {

    private static final Logger logger = LoggerFactory.getLogger(RpcHandler.class);

    final ServerConfig config;

    private BeanLocator beanLocator;

    public RpcHandler(ServerConfig config) {
        this.config = config;
        beanLocator = new DefaultBeanLocator(config.getProps());
    }
    @Override
    public Send handler(RequestKeys requestType, Request request) throws IOException {
        final long st = System.currentTimeMillis();
        RpcRequest rpcRequest = (RpcRequest)request;
        Object bean = this.beanLocator.getBean(rpcRequest.getBeanName());
        if (bean == null) {
            throw new RuntimeException("Could not find bean named " + rpcRequest.getBeanName());
        }
        RpcSkeleton skeleton = new RpcSkeleton(rpcRequest.getBeanName(), bean);
        Object result = null;
        try {
            result = skeleton.invoke(rpcRequest.getMethodName(), rpcRequest.getArgs());
            return new RpcSend(result);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return new RpcSend(result, ErrorMapping.NoError);
    }

    @Override
    public Request decode(ByteBuffer buffer) {
        return RpcRequest.readFrom(buffer);
    }
}
