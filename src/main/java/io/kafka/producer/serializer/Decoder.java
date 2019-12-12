package io.kafka.producer.serializer;

import io.kafka.message.Message;


public interface Decoder<T> {

    T toEvent(Message message);
}
