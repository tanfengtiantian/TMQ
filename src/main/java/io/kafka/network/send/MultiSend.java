package io.kafka.network.send;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;
import java.util.Iterator;
import java.util.List;

/**
 * @author tf
 * @version 创建时间：2019年2月11日 上午11:42:39
 * @ClassName MultiSend
 */
public abstract class MultiSend<S extends Send> extends AbstractSend {

    protected int expectedBytesToWrite;

    private int totalWritten = 0;

    private List<S> sends;

    private Iterator<S> iter;

    private S current;

    public MultiSend() {

    }

    public MultiSend(List<S> sends) {
        setSends(sends);
    }


    protected void setSends(List<S> sends) {
        this.sends = sends;
        this.iter = sends.iterator();
        if (iter.hasNext()) {
            this.current = iter.next();
        }
    }

    public List<S> getSends() {
        return sends;
    }

    public boolean complete() {
        if (current != null) return false;
        if (totalWritten != expectedBytesToWrite) {
            logger.error("mismatch in sending bytes over socket; expected: " + expectedBytesToWrite + " actual: " + totalWritten);
        }
        return true;
    }

    public int writeTo(GatheringByteChannel channel) throws IOException {
        expectIncomplete();
        int written = current.writeTo(channel);
        totalWritten += written;
        if (current.complete()) {//move to next element while current element is finished writting
            current = iter.hasNext() ? iter.next() : null;
        }
        return written;
    }
}