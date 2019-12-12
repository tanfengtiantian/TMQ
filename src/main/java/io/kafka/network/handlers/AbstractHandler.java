package io.kafka.network.handlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.kafka.log.ILogManager;
import io.kafka.network.request.RequestHandler;



/**
 * @author tf
 * @version 创建时间：2019年1月18日 下午2:14:06
 * @ClassName Handler处理基类
 */
public abstract class AbstractHandler implements RequestHandler {

	protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected final ILogManager logManager;

    public AbstractHandler(ILogManager logManager) {
        this.logManager = logManager;
    }
}
