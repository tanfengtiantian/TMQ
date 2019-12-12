package io.kafka.api;

import io.kafka.network.request.Request;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author tf
 * @version 创建时间：2019年2月11日 上午11:33:11
 * @ClassName MultiFetchRequest
 */
public class MultiFetchRequest implements Request {

    public final List<FetchRequest> fetches;

    public MultiFetchRequest(List<FetchRequest> fetches) {
        this.fetches = fetches;
    }

    public RequestKeys getRequestKey() {
        return RequestKeys.MULTIFETCH;
    }

    /**
     * @return fetches
     */
    public List<FetchRequest> getFetches() {
        return fetches;
    }


    public static MultiFetchRequest readFrom(ByteBuffer buffer) {
        int count = buffer.getShort();
        List<FetchRequest> fetches = new ArrayList<FetchRequest>(count);
        for (int i = 0; i < count; i++) {
            fetches.add(FetchRequest.readFrom(buffer));
        }
        return new MultiFetchRequest(fetches);
    }
}