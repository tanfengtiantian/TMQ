package io.kafka.log;


import java.util.List;

/**
 * @author tf
 * @version 创建时间：2018年12月30日 上午9:48:00
 * @ClassName 日志段集合
 * @Description （所有可读消息和最后一个可写文件）
 */
public interface ILogSegmentList {

	/**
	 * 获取当前的最后一段
	 */
	ILogSegment getLastView();

	/**
	 * 获取所有文件块
	 * @return
	 */
	List<ILogSegment> getView();

	/**
	 * 创建新日志块
	 * @param logSegment
	 */
	void append(ILogSegment logSegment);

	/**
	 * 从列表中删除前n项
	 * @param newstart索引小于newstart的日志段将被删除
	 * @return 返回删除的段
	 */
	List<ILogSegment> trunc(int newStart);

}
