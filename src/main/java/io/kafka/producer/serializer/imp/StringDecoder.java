package io.kafka.producer.serializer.imp;

import io.kafka.message.Message;
import io.kafka.producer.serializer.Decoder;
import io.kafka.utils.Utils;

import java.nio.ByteBuffer;

/**
 * UTF-8 bytes decoder
 * 
 * @author tf
 * @see StringEncoder
 */
public class StringDecoder implements Decoder<String> {

    public String toEvent(Message message) {
        ByteBuffer buf = message.payload();
        byte[] b = new byte[buf.remaining()];
        buf.get(b);
        return Utils.fromBytes(b);
    }

}
