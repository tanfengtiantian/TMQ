package io.kafka.common.exception;
/**
 * @author tf
 * @version 创建时间：2019年1月17日 下午2:57:24
 * @ClassName 发送异常
 */
public class InvalidSendException extends RuntimeException{

	private static final long serialVersionUID = 1L;

    public InvalidSendException() {
        super();
    }

    public InvalidSendException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidSendException(String message) {
        super(message);
    }

    public InvalidSendException(Throwable cause) {
        super(cause);
    }
}
