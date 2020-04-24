package io.kafka.network.handlers;

import static java.lang.String.format;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import io.kafka.api.ProducerRequest;
import io.kafka.api.RequestKeys;
import io.kafka.common.ErrorMapping;
import io.kafka.common.exception.InvalidMessageException;
import io.kafka.config.ServerConfig;
import io.kafka.log.ILog;
import io.kafka.log.ILogManager;
import io.kafka.message.MessageAndOffset;
import io.kafka.mx.BrokerTopicStat;
import io.kafka.network.request.Request;
import io.kafka.network.request.RequestHandlerFactory;
import io.kafka.network.send.ProducerSend;
import io.kafka.network.send.Send;

/**
 * @author tf
 * @version 创建时间：2019年1月18日 下午2:12:20
 * @ClassName Producer消息处理类
 * 
 * </pre>
 */
public class ProducerHandler extends AbstractHandler implements RequestHandlerFactory.Decoder {

	final String errorFormat = "Error processing %s on %s:%d";
	final ServerConfig config;
	public ProducerHandler(ILogManager logManager, ServerConfig config) {
		super(logManager);
		this.config=config;
	}

    @Override
    public Send handler(RequestKeys requestType, Request request) throws IOException {
        final long st = System.currentTimeMillis();
        ((ProducerRequest)request).brokerId=config.getBrokerId();
        if (logger.isDebugEnabled()) {
            logger.debug("Producer request " + request.toString());
        }
        ProducerSend producerSend = handleProducerRequest((ProducerRequest)request);
        long et = System.currentTimeMillis();
        if (logger.isDebugEnabled()) {
            logger.debug("解析消息耗时: " + (et - st) + " ms");
        }
        return producerSend;
    }

    protected ProducerSend handleProducerRequest(ProducerRequest request) {
		//logger.info("top[{}],partition[{}]",request.topic,request.partition);
		//如有指定partition取指定，没有随即取partition
		int partition = request.getTranslatedPartition(logManager);	
		 try {
            final ILog log = logManager.getOrCreateLog(request.topic, partition);
            List<Long> offset = log.append(request.messages);
            long messageSize = request.messages.getSizeInBytes();
            if (logger.isDebugEnabled()) {
                logger.debug(messageSize + " bytes 写入 logs " + log);
                for (MessageAndOffset m : request.messages) {
                    logger.trace("已将消息  [" + m.offset + "] 写入磁盘.");
                }
            }
            request.offset = offset.get(0);
            BrokerTopicStat.getInstance(request.topic).recordBytesIn(messageSize);
            BrokerTopicStat.getBrokerAllTopicStat().recordBytesIn(messageSize);
            return new ProducerSend(request);

        } catch (InvalidMessageException e) {
            if (logger.isDebugEnabled()) {
                logger.error(format(errorFormat, request.getRequestKey(), request.topic, request.partition), e);
            } else {
                logger.error("Producer failed. " + e.getMessage());
            }
            return new ProducerSend(request, ErrorMapping.InvalidMessageCode);
        } catch (IOException e) {
            if (logger.isDebugEnabled()) {
                logger.error(format(errorFormat, request.getRequestKey(), request.topic, request.partition), e);
            } else {
                logger.error("Producer failed. " + e.getMessage());
            }
            return new ProducerSend(request, ErrorMapping.WrongPartitionCode);
        }
	}

    @Override
    public Request decode(ByteBuffer buffer) {
        return ProducerRequest.readFrom(buffer);
    }

}
