package io.kafka.api;

import io.kafka.network.request.Request;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class MultiProducerRequest implements Request {

    public final List<ProducerRequest> produces;

    public MultiProducerRequest(List<ProducerRequest> produces) {
        this.produces = produces;
    }

    public static MultiProducerRequest readFrom(ByteBuffer buffer) {
        int count = buffer.getShort();
        List<ProducerRequest> produces = new ArrayList<ProducerRequest>(count);
        for (int i = 0; i < count; i++) {
            produces.add(ProducerRequest.readFrom(buffer));
        }
        return new MultiProducerRequest(produces);
    }

    @Override
    public RequestKeys getRequestKey() {
        return RequestKeys.MULTIPRODUCE;
    }
}
