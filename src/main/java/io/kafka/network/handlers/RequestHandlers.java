package io.kafka.network.handlers;

import io.kafka.api.RequestKeys;
import io.kafka.config.ServerConfig;
import io.kafka.log.ILogManager;
import io.kafka.network.receive.Receive;
import io.kafka.network.request.RequestHandler;
import io.kafka.network.request.RequestHandlerFactory;
import io.kafka.rpc.RpcHandler;
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
	private final RpcHandler rpcHandler;
	
	public RequestHandlers(ServerConfig config,ILogManager logManager, TransactionStore transactionStore) {
		producerHandler = new ProducerHandler(logManager,config);
		mltiProduceHandler = new MultiProduceHandler(logManager,config);
		fetchHandler = new FetchHandler(logManager,config);
		multiFetchHandler = new MultiFetchHandler(logManager,config);
		offsetsHandler = new OffsetsHandler(logManager,config);
		transactionHandler = new TransactionHandler(logManager,transactionStore,config);
		producerTTLHandler = new ProducerTTLHandler(logManager,config);
		rpcHandler = new RpcHandler(config);
	}
	@Override
	public RequestHandler mapping(RequestKeys id) {
		return selectKey(id);
	}

	public Decoder getDecoder(RequestKeys id) {
		return selectKey(id);
	}

	public <T> T selectKey(RequestKeys id) {
		switch (id) {
			case FETCH:
				return (T) fetchHandler;
			case PRODUCE:
				return (T)producerHandler;
			case MULTIFETCH:
				return (T)multiFetchHandler;
			case MULTIPRODUCE:
				return (T)mltiProduceHandler;
			case OFFSETS:
				return (T)offsetsHandler;
			case TRANSACTION:
				return (T)transactionHandler;
			case TTLPRODUCE:
				return (T)producerTTLHandler;
			case CREATE:
				return null;
			case DELETE:
				return null;
			case RPC:
				return (T)rpcHandler;
		}
		return null;
	}

}
