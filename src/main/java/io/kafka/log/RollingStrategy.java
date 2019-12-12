package io.kafka.log;

import java.io.Closeable;

/**
 * @author tf
 * @version 创建时间：2018年12月30日 下午10:00:06
 * @ClassName LogSegment 滚动策略
 */
public interface RollingStrategy extends Closeable{

	 /**
	  * 最后一个日志段是否需要新建LogSegment
	  * @param lastSegment
	  * @return
	  */
	 boolean check(ILogSegment lastSegment);
}
