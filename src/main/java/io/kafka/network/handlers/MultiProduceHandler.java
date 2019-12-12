package io.kafka.network.handlers;

import io.kafka.api.MultiProducerRequest;
import io.kafka.api.ProducerRequest;
import io.kafka.api.RequestKeys;
import io.kafka.config.ServerConfig;
import io.kafka.log.ILogManager;
import io.kafka.network.receive.Receive;
import io.kafka.network.send.Send;

public class MultiProduceHandler extends ProducerHandler {

    public MultiProduceHandler(ILogManager logManager, ServerConfig config) {
        super(logManager, config);
    }

    public Send handler(RequestKeys requestType, Receive receive) {
        MultiProducerRequest request = MultiProducerRequest.readFrom(receive.buffer());
        if (logger.isDebugEnabled()) {
            logger.debug("Multiproducer request " + request);
        }
        for (ProducerRequest produce : request.produces) {
            handleProducerRequest(produce);
        }
        return null;
    }
}
