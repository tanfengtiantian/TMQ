package io.kafka.common.exception;
/**
 * @author tf
 * @version 创建时间：2019年1月21日 上午11:35:08
 * @ClassName 无效的消息代码
 */
public class InvalidMessageException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public InvalidMessageException() {
        super();
    }

    public InvalidMessageException(String message) {
        super(message);
    }

}
