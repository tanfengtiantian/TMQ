package io.kafka.api;
/**
 * @author tf
 * @version 创建时间：2019年1月18日 下午2:20:30
 * @ClassName 标记可计算对象(request/message/data...)
 */
public interface ICalculable {
	/**
     * 获取当前对象的大小（字节）
     * 
     * @return 
     */
    int getSizeInBytes();

}
