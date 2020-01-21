package io.kafka.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * @author tf
 * @version 创建时间：2019年1月17日 下午3:17:25
 * @ClassName 传输状态基类
 */
public class AbstractTransmission implements Transmission {
	
	private boolean done = false;

    final protected Logger logger = LoggerFactory.getLogger(getClass());

    public void expectIncomplete() {
        if (complete()) {
            throw new IllegalStateException("This operation cannot be completed on a complete request.");
        }
    }

    public void expectComplete() {
        if (!complete()) {
            throw new IllegalStateException("This operation cannot be completed on an incomplete request.");
        }
    }

    public boolean complete() {
        return done;
    }

    @Override
    public void reset() {
        done = false;
    }

    public void setCompleted() {
        this.done = true;
    }
}
