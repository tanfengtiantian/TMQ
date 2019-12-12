package io.kafka.network.request;

import io.kafka.api.RequestKeys;
import io.kafka.network.receive.Receive;
import io.kafka.network.send.Send;

import java.io.IOException;


/**
 * @author tf
 * @version 创建时间：2019年1月17日 下午2:05:18
 * @ClassName 处理来自客户端的请求
 */
public interface RequestHandler {
	/**
     * 处理请求
     * 
     * @param 请求类型
     * @param 请求正文
     * @return 处理响应
     */
    Send handler(RequestKeys requestType, Receive request) throws IOException;
}
