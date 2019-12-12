package io.kafka.producer.serializer;

import io.kafka.message.Message;

public interface Encoder<T> {

    Message toMessage(T event);
}
