package io.kafka.network.handlers;

import io.kafka.api.ProducerTTLRequest;
import io.kafka.api.RequestKeys;
import io.kafka.common.ErrorMapping;
import io.kafka.config.ServerConfig;
import io.kafka.log.ILog;
import io.kafka.log.ILogManager;
import io.kafka.network.receive.Receive;
import io.kafka.network.send.ProducerTTLSend;
import io.kafka.network.send.Send;
import io.kafka.ttl.WheelTimerDelay;

import java.io.IOException;

/**
 * @author tf
 * @version 创建时间：2019年7月28日 下午2:12:20
 * @ClassName ProducerTTL消息处理类
 * 
 * </pre>
 */
public class ProducerTTLHandler extends AbstractHandler {

	final ServerConfig config;
	final WheelTimerDelay wheelTimerDelay;

	public ProducerTTLHandler(ILogManager logManager, ServerConfig config) {
		super(logManager);
		this.config=config;
        wheelTimerDelay = new WheelTimerDelay(logManager,config);
	}

	@Override
	public Send handler(RequestKeys requestType, Receive receive) {
        ProducerTTLRequest request = ProducerTTLRequest.readFrom(receive.buffer());
        request.brokerId=config.getBrokerId();
        return handleProducerTTLRequest(request);
	}

	private Send handleProducerTTLRequest(final ProducerTTLRequest request) {
        int partition = request.getTranslatedPartition(logManager);
        try {
            final ILog log = logManager.getOrCreateLog(request.getTopic(), partition);
            if (log == null) {
                return null;
            }
            wheelTimerDelay.addMessage(log,request.messages,request.ttl,null);
        }  catch (IOException e) {
            logger.error(e.getMessage());
            return new ProducerTTLSend(request, ErrorMapping.WrongPartitionCode);
        }
        return new ProducerTTLSend(request);
	}
}
