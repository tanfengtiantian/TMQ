package io.kafka.rpc;

import io.kafka.api.RequestKeys;
import io.kafka.api.RpcRequest;
import io.kafka.common.ErrorMapping;
import io.kafka.config.ServerConfig;
import io.kafka.network.receive.Receive;
import io.kafka.network.request.RequestHandler;
import io.kafka.network.send.Send;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

public class RpcHandler implements RequestHandler {

    private static final Logger logger = LoggerFactory.getLogger(RpcHandler.class);

    final ServerConfig config;

    private BeanLocator beanLocator;

    public RpcHandler(ServerConfig config) {
        this.config = config;
        beanLocator = new DefaultBeanLocator(config.getProps());
    }

    public Send handler(RequestKeys requestType, Receive receive) throws IOException {
        final long st = System.currentTimeMillis();
        RpcRequest request = RpcRequest.readFrom(receive.buffer());
        Object bean = this.beanLocator.getBean(request.getBeanName());
        if (bean == null) {
            throw new RuntimeException("Could not find bean named " + request.getBeanName());
        }
        RpcSkeleton skeleton = new RpcSkeleton(request.getBeanName(), bean);
        Object result = null;
        try {
            result = skeleton.invoke(request.getMethodName(), request.getArgs());
            return new RpcSend(result);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return new RpcSend(result, ErrorMapping.NoError);
    }
}
