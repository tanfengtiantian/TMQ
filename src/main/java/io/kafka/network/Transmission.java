package io.kafka.network;
/**
 * @author tf
 * @version 创建时间：2019年1月17日 下午2:01:36
 * @ClassName 传输
 */
public interface Transmission {
	 /**
     * 预期任务尚未完成，否则将引发异常
     */
    void expectIncomplete();
    /**
     * 预期任务已完成，否则将引发异常
     */
    void expectComplete();
    /**
     * 检查任务是否已完成
     */
    boolean complete();
}
