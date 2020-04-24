package io.kafka.network.request;

import io.kafka.api.RequestKeys;
import java.nio.ByteBuffer;


/**
 * @author tf
 * @version 创建时间：2019年1月17日 下午2:09:03
 * @ClassName 用于创建请求处理程序的工厂
 */
public interface RequestHandlerFactory {
	 /**
     * 为请求映射请求处理程序
     * 
     * @param id request type
     * @param request body
     * @return handler for the request
     */
    RequestHandler mapping(RequestKeys id);

    interface Decoder {
        Request decode(ByteBuffer buffer);
    }

    RequestHandlerFactory.Decoder getDecoder(RequestKeys id);
}
