package io.kafka.log.imp;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import io.kafka.log.ILogSegment;
import io.kafka.log.ILogSegmentList;

/**
 * @author tf
 * @version 创建时间：2018年12月31日 下午7:02:50
 * @ClassName LogSegmentList 文件集集合
 */
public class LogSegmentList implements ILogSegmentList {

	private final String name;
	
	private final AtomicReference<List<ILogSegment>> contents;

    public LogSegmentList(final String name, List<ILogSegment> segments) {
        this.name = name;
        contents = new AtomicReference<>(segments);
    }

	@Override
	public ILogSegment getLastView() {
		List<ILogSegment> views = contents.get();
		return views.get(views.size() - 1);
	}

	@Override
	public List<ILogSegment> getView() {
		return contents.get();
	}

	@Override
	public void append(ILogSegment segment) {
		while (true) {
            List<ILogSegment> curr = contents.get();
            List<ILogSegment> updated = new ArrayList<ILogSegment>(curr);
            updated.add(segment);
            if (contents.compareAndSet(curr, updated)) {
                return;
            }
        }
	}

	@Override
	public List<ILogSegment> trunc(int newStart) {
		if (newStart < 0) {
            throw new IllegalArgumentException("起始索引必须为正数.");
        }
		while (true) {
            List<ILogSegment> curr = contents.get();
            int newLength = Math.max(curr.size() - newStart, 0);
            List<ILogSegment> updatedList = new ArrayList<>(curr.subList(Math.min(newStart, curr.size() - 1),
                    curr.size()));
            if (contents.compareAndSet(curr, updatedList)) {
                return curr.subList(0, curr.size() - newLength);
            }
        }
	}
	@Override
    public String toString() {
        return "[" + name + "] " + getView();
    }
}
