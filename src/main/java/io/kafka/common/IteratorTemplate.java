package io.kafka.common;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author tf
 * @version 创建时间：2019年1月14日 下午2:19:25
 * @ClassName 消费对象迭代器的模板
 */
public abstract class IteratorTemplate<T> implements Iterator<T> {
	enum State {
		/**完成**/
        DONE, 
        /**已准备**/
        READY, 
        /**未准备**/
        NOT_READY,
        /**失败**/
        FAILED;
    }

    private State state = State.NOT_READY;

    private T nextItem = null;

    public T next() {
        if (!hasNext()) throw new NoSuchElementException();
        state = State.NOT_READY;
        return nextItem;
    }

    public boolean hasNext() {
        switch (state) {
            case FAILED:
                throw new IllegalStateException("error failed state");
            case DONE:
                return false;
            case READY:
                return true;
            case NOT_READY:
                break;
        }
        return maybeComputeNext();
    }

    protected abstract T makeNext();

    private boolean maybeComputeNext() {
        state = State.FAILED;
        nextItem = makeNext();
        if (state == State.DONE) 
        	return false;
        state = State.READY;
        return true;
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }

    protected void resetState() {
        state = State.NOT_READY;
    }

    protected T allDone() {
        state = State.DONE;
        return null;
    }
}
