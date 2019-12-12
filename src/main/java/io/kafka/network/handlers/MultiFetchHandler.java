package io.kafka.network.handlers;

import io.kafka.api.FetchRequest;
import io.kafka.api.MultiFetchRequest;
import io.kafka.api.RequestKeys;
import io.kafka.config.ServerConfig;
import io.kafka.log.ILogManager;
import io.kafka.network.receive.Receive;
import io.kafka.network.send.MessageSetSend;
import io.kafka.network.send.MultiMessageSetSend;
import io.kafka.network.send.Send;
import java.util.ArrayList;
import java.util.List;

/**
 * @author tf
 * @version 创建时间：2019年2月11日 上午11:31:18
 * @ClassName MultiFetchHandler  多个Fetch合并请求处理类
 */
public class MultiFetchHandler extends FetchHandler {

    public MultiFetchHandler(ILogManager logManager, ServerConfig config) {
        super(logManager,config);
    }

    public Send handler(RequestKeys requestType, Receive request) {
        MultiFetchRequest multiFetchRequest = MultiFetchRequest.readFrom(request.buffer());
        List<FetchRequest> fetches = multiFetchRequest.getFetches();
        if (logger.isDebugEnabled()) {
            logger.debug("Multifetch request size: " + fetches.size());
            for (FetchRequest fetch : fetches) {
                logger.debug(fetch.toString());
            }
        }
        List<MessageSetSend> responses = new ArrayList<MessageSetSend>(fetches.size());
        for (FetchRequest fetch : fetches) {
            responses.add(readMessageSet(fetch));
        }
        return new MultiMessageSetSend(responses);
    }

}