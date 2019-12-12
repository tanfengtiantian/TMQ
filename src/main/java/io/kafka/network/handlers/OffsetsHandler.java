package io.kafka.network.handlers;

import io.kafka.api.OffsetRequest;
import io.kafka.api.RequestKeys;
import io.kafka.config.ServerConfig;
import io.kafka.log.ILogManager;
import io.kafka.network.receive.Receive;
import io.kafka.network.send.OffsetArraySend;
import io.kafka.network.send.Send;

/**
 * @author tf
 * @version 创建时间：2019年2月14日 上午10:06:45
 * @ClassName OffsetsHandler  消费端获取有效的offset
 */
public class OffsetsHandler extends AbstractHandler {

    public OffsetsHandler(ILogManager logManager, ServerConfig config) {
        super(logManager);
    }
    
    @Override
    public Send handler(RequestKeys requestType, Receive request) {
        OffsetRequest offsetRequest = OffsetRequest.readFrom(request.buffer());
        return new OffsetArraySend(logManager.getOffsets(offsetRequest));
    }


}
