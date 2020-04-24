package io.kafka.network.handlers;

import io.kafka.api.FetchRequest;
import io.kafka.api.RequestKeys;
import io.kafka.config.ServerConfig;
import io.kafka.log.ILog;
import io.kafka.log.ILogManager;
import io.kafka.message.MessageSet;
import io.kafka.network.receive.Receive;
import io.kafka.network.request.Request;
import io.kafka.network.request.RequestHandlerFactory;
import io.kafka.network.send.MessageSetSend;
import io.kafka.network.send.Send;
import io.kafka.common.ErrorMapping;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author tf
 * @version 创建时间：2019年1月24日 上午9:46:55
 * @ClassName  handler fetchrequest
 */
public class FetchHandler extends AbstractHandler implements RequestHandlerFactory.Decoder {

	public FetchHandler(ILogManager logManager, ServerConfig config) {
		super(logManager);
	}


    @Override
    public Send handler(RequestKeys requestType, Request request) throws IOException {
        FetchRequest fetchRequest = (FetchRequest)request;
        if (logger.isDebugEnabled()) {
            logger.debug("Fetch request " + fetchRequest.toString());
        }
        return readMessageSet(fetchRequest);
    }
	
	protected MessageSetSend readMessageSet(FetchRequest fetchRequest) {
        final String topic = fetchRequest.topic;
        MessageSetSend response = null;
        try {
            ILog log = logManager.getLog(topic, fetchRequest.partition);
            if (logger.isDebugEnabled()) {
                logger.debug("Fetching log segment request={}, log={}",fetchRequest ,log);
            }
            if (log != null) {
                response = new MessageSetSend(log.read(fetchRequest.offset, fetchRequest.maxSize));

            } else {
                response = new MessageSetSend();
            }
        } catch (IOException e) {
            logger.error("error when processing request " + fetchRequest, e);
           
            response = new MessageSetSend(MessageSet.Empty, ErrorMapping.valueOf(e));
        }
        return response;
    }

    @Override
    public Request decode(ByteBuffer buffer) {
        return FetchRequest.readFrom(buffer);
    }
}
