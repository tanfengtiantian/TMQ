package io.kafka.network.handlers;

import io.kafka.api.RequestKeys;
import io.kafka.config.ServerConfig;
import io.kafka.log.ILogManager;
import io.kafka.network.receive.Receive;
import io.kafka.network.request.RequestHandler;
import io.kafka.network.request.RequestHandlerFactory;
import io.kafka.transaction.store.TransactionStore;


/**
 * @author tf
 * @version 创建时间：2019年1月17日 下午5:49:43
 * @ClassName Handlers处理集合
 */
public class RequestHandlers implements RequestHandlerFactory {

	private final ProducerHandler producerHandler;
	private final MultiProduceHandler mltiProduceHandler;
	private final FetchHandler fetchHandler;
	private final MultiFetchHandler multiFetchHandler;
	private final OffsetsHandler offsetsHandler;
	private final TransactionHandler transactionHandler;
	private final ProducerTTLHandler producerTTLHandler;
	
	public RequestHandlers(ServerConfig config,ILogManager logManager, TransactionStore transactionStore) {
		producerHandler = new ProducerHandler(logManager,config);
		mltiProduceHandler = new MultiProduceHandler(logManager,config);
		fetchHandler = new FetchHandler(logManager,config);
		multiFetchHandler = new MultiFetchHandler(logManager,config);
		offsetsHandler = new OffsetsHandler(logManager,config);
		transactionHandler = new TransactionHandler(logManager,transactionStore,config);
		producerTTLHandler = new ProducerTTLHandler(logManager,config);
	}
	@Override
	public RequestHandler mapping(RequestKeys id, Receive request) {
		switch (id) {
	        case FETCH:
	            return fetchHandler;
	        case PRODUCE:
	            return producerHandler;
	        case MULTIFETCH:
	            return multiFetchHandler;
	        case MULTIPRODUCE:
	            return mltiProduceHandler;
	        case OFFSETS:
	            return offsetsHandler;
			case TRANSACTION:
				return transactionHandler;
			case TTLPRODUCE:
				return producerTTLHandler;
			case CREATE:
				return null;
			case DELETE:
				return null;
		}
		return null;
	}
}
