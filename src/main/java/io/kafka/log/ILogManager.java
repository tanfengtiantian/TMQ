package io.kafka.log;

import io.kafka.api.OffsetRequest;
import io.kafka.api.PartitionChooser;
import io.kafka.log.imp.FixedSizeRollingStrategy;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * 获取本地消息log核心类
 * @author tf
 *
 */
public interface ILogManager extends PartitionChooser,Closeable {

	/**
	 * 启动加载本地log.dir目录文件
	 * @throws IOException
	 */
	public void load() throws IOException;
	
	
	/**
	 * 根据topic和分区获取当前Log文件
	 * @throws IOException
	 */
	ILog getOrCreateLog(String topic, int partition) throws IOException;
	
	/**
	 * 根据topic和分区获取当前Log文件并创建Segments文件块
	 * @throws IOException
	 */
	ILog createLog(String topic, int partition) throws IOException;


	/**
	 * 如果存在，则获取日志或返回null
	 * @param topic
	 * @param partition
	 * @return
	 */
	ILog getLog(String topic, int partition);


	/**
	 * 获取topic集合k=topic,v=Partition总数
	 * @return
	 */
	Map<String, Integer> getTopicPartitionsMap();
	
	/**
	 * 获取有效的offset
	 * @return
	 */
	List<Long> getOffsets(OffsetRequest offsetRequest);


	/**
	 * 文件滚动策略
	 * @param fixedSizeRollingStrategy
	 */
	void setRollingStategy(RollingStrategy rollingStrategy);

	/**
	 * 移除topic
	 * @param topic
	 * @return
	 */
	int deleteLogs(String topic);

	/**
	 * 启动服务
	 */
	void startup();
}
