package io.kafka.common.exception;
/**
 * @author tf
 * @version 创建时间：2019年1月21日 上午11:20:49
 * @ClassName 指示客户端已请求服务器上不再可用的范围
 */
public class InvalidMessageSizeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public InvalidMessageSizeException() {
        super();
    }

    public InvalidMessageSizeException(String message) {
        super(message);
    }

}