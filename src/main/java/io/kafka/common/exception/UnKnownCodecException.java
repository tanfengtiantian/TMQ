package io.kafka.common.exception;
/**
 * @author tf
 * @version 创建时间：2019年1月10日 上午9:36:41
 * @ClassName 压缩错误
 */
public class UnKnownCodecException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public UnKnownCodecException() {
        super();
    }

    public UnKnownCodecException(String message) {
        super(message);
    }

}
