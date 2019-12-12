package io.kafka.network.request;

import io.kafka.api.RequestKeys;
import java.nio.ByteBuffer;

/**
 * @author tf
 * @version 创建时间：2019年1月18日 下午2:18:07
 * @ClassName Request
 */
public interface Request{

    /**
     * request type
     * 
     */
    RequestKeys getRequestKey();

    /**
     * 将请求数据写入缓冲区
     * 
     * @param buffer data
     */
    //void writeTo(ByteBuffer buffer);
}
