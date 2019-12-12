package io.kafka.common.exception;
/**
 * @author tf
 * @version 创建时间：2019年1月21日 上午11:01:51
 * @ClassName MessageSize-Exception
 */
public class MessageSizeTooLargeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public MessageSizeTooLargeException() {
        super();
    }

    public MessageSizeTooLargeException(String message) {
        super(message);
    }

}