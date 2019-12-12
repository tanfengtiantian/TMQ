package io.kafka.log.imp;

import java.io.File;
import java.io.IOException;

import io.kafka.log.ILogSegment;
import io.kafka.message.FileMessageSet;

/**
 * @author tf
 * @version 创建时间：2018年12月31日 上午9:07:23
 * @ClassName 日志片 最小单位
 * @Description 每个段落文件操作
 */
public class LogSegment implements ILogSegment {
	
	private FileMessageSet messageSet;
	private File file;
	private long start;
	private boolean deleted;

	public LogSegment(File file, FileMessageSet messageSet, long start) {
        super();
        this.file = file;
        this.messageSet = messageSet;
        this.start = start;
        this.deleted = false;
    }
	
	@Override
	public long start() {
		return start;
	}

	@Override
	public long size() {
		return messageSet.highWaterMark();
	}

	@Override
	public boolean isEmpty() {
		return size() == 0;
	}

	@Override
	public boolean contains(long offset) {
		long size = size();
        long start = start();
        return ((size == 0 && offset == start) //
                || (size > 0 && offset >= start && offset <= start + size - 1));
	}

	@Override
	public int compareTo(ILogSegment o) {
		return this.start > o.start() ? 1 : this.start < o.start() ? -1 : 0;
	}

	@Override
	public void close() throws IOException {
		
	}
	@Override
	public File getFile() {
		return file;
	}

	@Override
	public FileMessageSet getMessageSet() {
		return messageSet;
	}

	@Override
	public String getName() {
		return file.getName();
	}

	@Override
	public void setDeleted(boolean deleted) {
		this.deleted = deleted;
	}

}
