package io.kafka.common.exception;
/**
 * @author tf
 * @version 创建时间：2019年1月17日 下午2:57:24
 * @ClassName 无效的请求异常
 */
public class InvalidRequestException extends RuntimeException{
	
	private static final long serialVersionUID = 1L;

    public InvalidRequestException() {
        super();
    }

    public InvalidRequestException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidRequestException(String message) {
        super(message);
    }

    public InvalidRequestException(Throwable cause) {
        super(cause);
    }
}
