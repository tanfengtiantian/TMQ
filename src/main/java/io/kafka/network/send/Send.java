package io.kafka.network.send;


import io.kafka.network.Transmission;

import java.io.IOException;

/**
 * @author tf
 * @version 创建时间：2019年1月17日 下午2:06:14
 * @ClassName 发送数据。
 */
public interface Send extends Transmission {

    int writeTo(java.nio.channels.GatheringByteChannel channel) throws IOException;

    int writeCompletely(java.nio.channels.GatheringByteChannel channel) throws IOException;
}
