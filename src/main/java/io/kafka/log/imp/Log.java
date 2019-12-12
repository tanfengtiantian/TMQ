package io.kafka.log.imp;

import static java.lang.String.format;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.kafka.common.exception.InvalidMessageException;
import io.kafka.message.MessageAndOffset;
import io.kafka.mx.BrokerTopicStat;
import io.kafka.transaction.AppendCallback;
import io.kafka.transaction.store.Location;
import io.kafka.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kafka.api.OffsetRequest;
import io.kafka.log.ILog;
import io.kafka.log.ILogSegment;
import io.kafka.log.ILogSegmentList;
import io.kafka.log.LogSegmentFilter;
import io.kafka.log.RollingStrategy;
import io.kafka.message.ByteBufferMessageSet;
import io.kafka.message.FileMessageSet;
import io.kafka.message.MessageSet;
import io.kafka.utils.KV;

/**
 * @author tf
 * @version 创建时间：2018年12月30日 下午11:09:46
 * @ClassName 日志是包含多个文件的消息集。
 */
public class Log implements ILog{

	private final Logger logger = LoggerFactory.getLogger(Log.class);

    private static String FileSuffix = ".zxQueue";

    private final AtomicLong lastflushedTime = new AtomicLong(System.currentTimeMillis());
    
    public final int partition;
    
    private File dir;

    private RollingStrategy rollingStategy;

    private int flushInterval;

    private boolean needRecovery;
    
    private final int maxMessageSize;
    
    public final String name;
    
    private ILogSegmentList segments;
    
    private final Object lock = new Object();
    
    /**
     * 记录最大flushInterval消息数量
     */
    private final AtomicInteger unflushed = new AtomicInteger(0);
    
    /**
     * 
     * param 目录
     * param 分区
     * param 滚动策略
     * param 最大条目flushInterval
     * param 是否需要恢复
     * param 最大消息长度
     * @throws IOException
     */
    public Log(File dir, //
            int partition,//
            RollingStrategy rollingStategy,//
            int flushInterval, //
            boolean needRecovery,//
            int maxMessageSize) throws IOException{
	 this.dir = dir;
     this.partition = partition;
     this.rollingStategy = rollingStategy;
     this.flushInterval = flushInterval;
     this.needRecovery = needRecovery;
     this.maxMessageSize = maxMessageSize;
     this.name = dir.getName();
     this.segments = loadSegments();
    	
    }
    
    private ILogSegmentList loadSegments() throws IOException {
    	List<ILogSegment> accum = new ArrayList<>();
    	File[] ls = dir.listFiles(f -> f.isFile() && f.getName().endsWith(FileSuffix));
    	logger.info("load  ILogSegmentList [" + dir.getAbsolutePath() + "]: " + ls.length);
    	//如果分区目录有日志段（ILogSegment）
    	int n = 0;
    	for (File f : ls) {
    		n++;
    		String filename = f.getName();
            long start = Long.parseLong(filename.substring(0, filename.length() - FileSuffix.length()));
            final String logFormat = "LOADING_LOG_FILE[%2d], start(offset)=%d, size=%d, path=%s";
            logger.info(String.format(logFormat, n, start, f.length(), f.getAbsolutePath()));
            //加载时，文件都不可改变追加
            FileMessageSet messageSet = new FileMessageSet(f, false);
            accum.add(new LogSegment(f, messageSet, start));
    	}
    	if (accum.size() == 0) {
    		//没有现有的段，创建一个新的可变段
    		File newFile = new File(dir, Log.nameFromOffset(0));
    		FileMessageSet fileMessageSet = new FileMessageSet(newFile, true);
    		accum.add(new LogSegment(newFile, fileMessageSet, 0));
    	} else {
    		//至少有一个现有段，验证并恢复它们
    		//按升序对段进行排序，以便快速搜索
            Collections.sort(accum);
            validateSegments(accum);
        }
    	ILogSegment last = accum.remove(accum.size() - 1);
    	//关闭
    	last.getMessageSet().close();
    	logger.info("加载最后一段 " + last.getFile().getAbsolutePath() + " , recovery " + needRecovery);
    	//最后的一个文件作为可读写，其他文件只要只读
    	LogSegment mutable = new LogSegment(last.getFile(), new FileMessageSet(last.getFile(), true, new AtomicBoolean(
                needRecovery)), last.start());
    	accum.add(mutable);
		return new LogSegmentList(name,accum);
    }
    
	
	@Override
	public List<Long> append(ByteBufferMessageSet messages) throws InvalidMessageException {
		synchronized (lock) {
			try {
				int numberOfMessages = 0;
				for (MessageAndOffset messageAndOffset : messages) {
					if (!messageAndOffset.message.isValid()) {
						throw new InvalidMessageException();
					}
					numberOfMessages += 1;
				}
				//监控MBean
				BrokerTopicStat.getBrokerTopicStat(getTopicName()).recordMessagesIn(numberOfMessages);
				BrokerTopicStat.getBrokerAllTopicStat().recordMessagesIn(numberOfMessages);

				ILogSegment lastSegment = segments.getLastView();
				//写入的大小和第一个偏移量
				long[] writtenAndOffset = lastSegment.getMessageSet().append(messages);
				if(logger.isDebugEnabled()){
					logger.debug(String.format("[分区 -%s,文件块-%s] save %d messages, bytes %d", name, lastSegment.getName(),
						numberOfMessages, writtenAndOffset[0]));
				}
				// 如果超过flushInterval个消息没有刷盘则同步刷盘
				maybeFlush(numberOfMessages);
				// 根据滚动策略创建segment
                maybeRoll(lastSegment);
                // start + offset
                return Arrays.asList(lastSegment.start() + writtenAndOffset[1]);
			} catch (IOException e) {
				logger.error("append error: ", e);
				throw new InvalidMessageException(e.getMessage());
	            //Runtime.getRuntime().halt(1);
			}
		}
	}

	@Override
	public void append(List<ByteBufferMessageSet> messages, AppendCallback callback) {
		int capacity = 0;
		for (final ByteBufferMessageSet req : messages) {
			capacity += req.getValidBytes();
		}
		final ByteBuffer buffer = ByteBuffer.allocate(capacity);
		for (ByteBufferMessageSet entry : messages) {
			// 将消息集的缓冲区截断为有效字节，然后将其追加到磁盘日志中
			buffer.put(entry.getBuffer().duplicate());
		}
		buffer.flip();
		try {
			int numberOfMessages = 1;
			ILogSegment lastSegment = segments.getLastView();
			long[] writtenAndOffset = lastSegment.getMessageSet().append(new ByteBufferMessageSet(buffer));
			if(logger.isDebugEnabled()){
				logger.debug(String.format("[分区 -%s,文件块-%s] save %d messages, bytes %d", name, lastSegment.getName(),
						numberOfMessages, writtenAndOffset[0]));
			}
			maybeFlush(numberOfMessages);
			// 根据滚动策略创建segment
			maybeRoll(lastSegment);
			if(callback != null){
				// start + offset , length
				callback.appendComplete(Location.create(lastSegment.start() + writtenAndOffset[1],(int)writtenAndOffset[0]),buffer);
			}
		} catch (Exception e) {
			logger.error("append error: ", e);
			Runtime.getRuntime().halt(1);
		}
	}

	@Override
	public void replayAppend(long offset, int length, long checksum, List<Long> msgIds, List<ByteBufferMessageSet> reqs, AppendCallback cb) throws IOException {
		List<ILogSegment> views = segments.getView();
		ILogSegment segment = findRange(views, offset, views.size());
		//如果消息没有存储成功，则重新存储，并返回新的位置
		if (segment == null) {
			this.append(reqs,cb);
		}else {//校验消息是否一致
			final FileMessageSet messageSet = segment.getMessageSet().slice(offset - segment.start(), offset - segment.start() + length);
			final ByteBuffer buf = ByteBuffer.allocate(length);
			messageSet.read(buf, offset - segment.start());
			buf.flip();

			final byte[] bytes = new byte[buf.remaining()];
			buf.get(bytes);
			// 这个校验和是整个消息的校验和，这跟message的校验和不一样，注意区分
			final long checkSumInDisk = Utils.crc32(bytes);
			// 没有存入，则重新存储
			if (checksum != checkSumInDisk) {
				this.append(reqs,cb);
			}else {
				// 正常存储了消息，无需处理
				if (cb != null) {
					cb.appendComplete(null,null);
				}
			}

		}

	}

	@Override
	public MessageSet read(long offset, int length) throws IOException {
		List<ILogSegment> views = segments.getView();
		ILogSegment found = findRange(views, offset, views.size());
		if (found == null) {
            if (logger.isDebugEnabled()) {
                logger.debug(format("not found message  from Log[%s], offset=%d, length=%d", name, offset, length));
            }
            return MessageSet.Empty;
        }
		return found.getMessageSet().read(offset - found.start(), length);
	}
	
	@Override
	public File getFile() {
		return dir;
	}
	
	@Override
	public List<ILogSegment> markDeletedWhile(LogSegmentFilter filter) throws IOException {
		synchronized (lock) {
			List<ILogSegment> view = segments.getView();
            List<ILogSegment> deletable = new ArrayList<ILogSegment>();
            for (ILogSegment seg : view) {
                if (filter.filter(seg)) {
                    deletable.add(seg);
                }
            }
            for (ILogSegment seg : deletable) {
                seg.setDeleted(true);
            }
            int numToDelete = deletable.size();
            //
            // 如果要删除所有内容,
            if (numToDelete == view.size()) {
            	//最后日志块有数据则创建一个新的空段
                if (view.get(numToDelete - 1).size() > 0) {
                    roll();
                } else {
                	//如果要删除的最后一个段为空，只需重复使用最后一段并重置修改的时间。
                    view.get(numToDelete - 1).getFile().setLastModified(System.currentTimeMillis());
                    numToDelete -= 1;
                }
            }
            return segments.trunc(numToDelete);
		}
	}

	@Override
	public void close() {
		synchronized (lock) {
			for (ILogSegment seg : segments.getView()) {
				try {
					seg.getMessageSet().close();
				} catch (IOException e) {
					logger.error("close file message set failed", e);
				}
			}
		}
	}
	
	/**
	 * 二分查找法，发现文件块
	 * @param ranges
	 * @param offset
	 * @param arraySize
	 * @return
	 */
	private ILogSegment findRange(List<ILogSegment> ranges, long offset, int arraySize) {
		if (ranges.size() < 1) return null;
		ILogSegment first = ranges.get(0);
		ILogSegment last = ranges.get(arraySize - 1);
		if (offset < first.start() || offset > last.start() + last.size()) {
            throw new RuntimeException(format("offset %s 不再范围内 (%s, %s)",//
            		offset,first.start(),last.start()+last.size()));
        }
		//off check end last
		if (offset == last.start() + last.size()) return null;
		// 按范围中的值在范围列表中查找给定的范围对象
		int low = 0;
        int high = arraySize - 1;
        while (low <= high) {
            int mid = (high + low) / 2;
            ILogSegment found = ranges.get(mid);
            if (found.contains(offset)) {
                return found;
            } else if (offset < found.start()) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
		return null;
	}
	
	private void maybeRoll(ILogSegment lastSegment) throws IOException {
		if (rollingStategy.check(lastSegment)) {
            roll();
        }
	}

	private void roll() throws IOException {	
		 synchronized (lock) {
			 long newOffset = nextAppendOffset();
			 File newFile = new File(dir, nameFromOffset(newOffset));
			 if (newFile.exists()) {
	                logger.warn("new rolled logsegment [" + newFile.getName() + "] exists");
	                if (!newFile.delete()) {
	                    logger.error("delete exist file : " + newFile.getName());
	                    throw new RuntimeException(
	                            "delete exist file: " + newFile.getName());
	                }
	         } 
			 logger.info("滚动文件目录：'" + name + "' 名称： " + newFile.getName());
			 segments.append(new LogSegment(newFile, new FileMessageSet(newFile, true), newOffset));
		 }
	}

	/**
	 * 刷盘，获取当前文件块 offset
	 * @return
	 * @throws IOException
	 */
	private long nextAppendOffset() throws IOException {
		flush();
        ILogSegment lastView = segments.getLastView();
        return lastView.start() + lastView.size();
	}

	/**
	 * 超过最大条目，刷盘
	 * @param numberOfMessages
	 * @throws IOException
	 */
	private void maybeFlush(int numberOfMessages) throws IOException {
        if (unflushed.addAndGet(numberOfMessages) >= flushInterval) {
            flush();
        }
    }
	/**
	 * 刷盘
	 * @throws IOException 
	 */
	public void flush() throws IOException {
		 if (unflushed.get() == 0) return;
		 synchronized (lock) {
			 if (logger.isDebugEnabled()) {
	                logger.debug("Flush log(top-分区) '" + name + "' current time: " + System
	                        .currentTimeMillis());
	            }
			 //FileChannel.force 保存磁盘
			 segments.getLastView().getMessageSet().flush();
			 //清理 unflushed
			 unflushed.set(0);
			 lastflushedTime.set(System.currentTimeMillis());
		 }
	}

	@Override
	public int delete() {
		//关闭文件快filechannel
		close();
		//删除分区所有文件块
		int count = segments.trunc(Integer.MAX_VALUE).size();
		//
		Utils.deleteDirectory(dir);
		return count;
	}

	private static String nameFromOffset(long offset) {
		NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset) + Log.FileSuffix;
	}

	private void validateSegments(List<ILogSegment> accum) {
        synchronized (lock) {
            for (int i = 0; i < accum.size() - 1; i++) {
            	ILogSegment curr = accum.get(i);
            	ILogSegment next = accum.get(i + 1);
                if (curr.start() + curr.size() != next.start()) {
                    throw new IllegalStateException("消息段验证失败: " + curr.getFile()
                            .getAbsolutePath() + ", " + next.getFile().getAbsolutePath());
                }
            }
        }
    }

	@Override
	public long getLastFlushedTime() {
		return lastflushedTime.get();
	}

	@Override
	public String getTopicName() {
		 return this.name.substring(0, name.lastIndexOf("-"));
	}

	@Override
	public int getPartition() {
		return Utils.getInt(this.name.substring(1, name.lastIndexOf("-")),0);
	}

	@Override
	public String getDescription() {
		return this.name;
	}

	@Override
    public String toString() {
        return "Log [dir=" + dir + ", lastflushedTime=" + //
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(lastflushedTime.get())) + "]";
    }

	@Override
	public List<Long> getOffsetsBefore(OffsetRequest offsetRequest) {
		List<ILogSegment> logSegments = segments.getView();	
		ILogSegment lastLogSegent = segments.getLastView();
		final boolean lastSegmentNotEmpty = lastLogSegent.size() > 0;
        List<KV<Long, Long>> offsetTimes = new ArrayList<KV<Long, Long>>();
        for (ILogSegment ls : logSegments) {
        	offsetTimes.add(new KV<Long, Long>(//
        			//文件创建名offset,最后一次被修改的时间
                    ls.start(), ls.getFile().lastModified()));
		}
        
        if (lastSegmentNotEmpty) {
            offsetTimes.add(new KV<Long, Long>(lastLogSegent.start() + lastLogSegent.getMessageSet().highWaterMark(),
                    System.currentTimeMillis()));
        }
        
        int startIndex = -1;
        final long requestTime = offsetRequest.time;
        //那最后一条offset还是第一条offset
        if (requestTime == OffsetRequest.LATES_TTIME) {
            startIndex = offsetTimes.size() - 1;
        } else if (requestTime == OffsetRequest.EARLIES_TTIME) {
            startIndex = 0;
        } else {
            boolean isFound = false;
            startIndex = offsetTimes.size() - 1;
            for (; !isFound && startIndex >= 0; startIndex--) {
                if (offsetTimes.get(startIndex).v <= requestTime) {
                    isFound = true;
                }
            }
        }
        
        final int retSize = Math.min(offsetRequest.maxNumOffsets, startIndex + 1);
        
        final List<Long> ret = new ArrayList<Long>(retSize);
        for (int j = 0; j < retSize; j++) {
            ret.add(offsetTimes.get(startIndex).k);
            startIndex -= 1;
        }
        return ret;
	}
}
