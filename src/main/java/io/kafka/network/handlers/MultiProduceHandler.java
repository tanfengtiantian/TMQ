package io.kafka.network.handlers;

import io.kafka.api.MultiProducerRequest;
import io.kafka.api.ProducerRequest;
import io.kafka.api.RequestKeys;
import io.kafka.config.ServerConfig;
import io.kafka.log.ILogManager;
import io.kafka.network.receive.Receive;
import io.kafka.network.request.Request;
import io.kafka.network.send.Send;

import java.nio.ByteBuffer;

public class MultiProduceHandler extends ProducerHandler {

    public MultiProduceHandler(ILogManager logManager, ServerConfig config) {
        super(logManager, config);
    }

    public Send handler(RequestKeys requestType, Request request) {
        MultiProducerRequest multiProducerRequest = (MultiProducerRequest)request;
        if (logger.isDebugEnabled()) {
            logger.debug("Multiproducer request " + multiProducerRequest);
        }
        for (ProducerRequest produce : multiProducerRequest.produces) {
            handleProducerRequest(produce);
        }
        return null;
    }

    public Request decode(ByteBuffer buffer) {
        return MultiProducerRequest.readFrom(buffer);
    }
}
