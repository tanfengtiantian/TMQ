package io.kafka.common.exception;
/**
 * @author tf
 * @version 创建时间：2018年12月30日 下午10:29:27
 */
public class UnknownMagicByteException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public UnknownMagicByteException() {
        super();
    }

    public UnknownMagicByteException(String message) {
        super(message);
    }

}
