package io.kafka.producer.serializer.imp;

import io.kafka.message.Message;
import io.kafka.producer.serializer.Encoder;
import io.kafka.utils.Utils;


/**
 * UTF-8 bytes encoder
 * 
 * @author tf
 * @since 1.0
 * @see StringDecoder
 */
public class StringEncoder implements Encoder<String> {

    public Message toMessage(String event) {
        return new Message(Utils.getBytes(event, "UTF-8"));
    }

}
