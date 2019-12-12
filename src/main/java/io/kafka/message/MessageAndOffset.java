package io.kafka.message;
/**
 * @author tf
 * @version 创建时间：2019年1月11日 下午9:21:41
 * @ClassName 消费迭代
 */
public class MessageAndOffset {

	public final Message message;

    public final long offset;

    public MessageAndOffset(Message message, long offset) {
        this.message = message;
        this.offset = offset;
    }

    @Override
    public String toString() {
        return String.format("MessageAndOffset [offset=%s, message=%s]", offset, message);
    }
}
