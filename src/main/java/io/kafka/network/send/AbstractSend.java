package io.kafka.network.send;


import io.kafka.network.AbstractTransmission;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;

/**
 * @author tf
 * @version 创建时间：2019年1月21日 下午2:38:41
 * @ClassName Send base类
 */
public abstract class AbstractSend extends AbstractTransmission implements Send {

    public int writeCompletely(GatheringByteChannel channel) throws IOException {
        int written = 0;
        while(!complete()) {
            written += writeTo(channel);
        }
        return written;
    }

}

