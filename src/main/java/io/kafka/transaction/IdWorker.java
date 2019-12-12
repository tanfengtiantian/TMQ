package io.kafka.transaction;


/**
 * 来自于twitter项目<a
 * href="https://github.com/twitter/snowflake">snowflake</a>的id产生方案，全局唯一，时间有序
 *
 *
 * @see https://github.com/twitter/snowflake
 * @author tf
 * @Date 2019-6-25
 *
 */
public class IdWorker {
    private final long workerId;
    private final static long twepoch = 1303895660503L;
    private long sequence = 0L;
    private final static long workerIdBits = 10L;
    private final static long maxWorkerId = -1L ^ -1L << workerIdBits;
    private final static long sequenceBits = 12L;

    private final static long workerIdShift = sequenceBits;
    private final static long timestampLeftShift = sequenceBits + workerIdBits;
    private final static long sequenceMask = -1L ^ -1L << sequenceBits;

    private long lastTimestamp = -1L;


    public IdWorker(final long workerId) {
        super();
        if (workerId > this.maxWorkerId || workerId < 0) {
            throw new IllegalArgumentException(String.format("worker Id can't be greater than %d or less than 0",
                    this.maxWorkerId));
        }
        this.workerId = workerId;
    }


    public synchronized long nextId() {
        long timestamp = this.timeGen();
        if (this.lastTimestamp == timestamp) {
            this.sequence = this.sequence + 1 & this.sequenceMask;
            if (this.sequence == 0) {
                timestamp = this.tilNextMillis(this.lastTimestamp);
            }
        }
        else {
            this.sequence = 0;
        }
        if (timestamp < this.lastTimestamp) {
            throw new IllegalArgumentException(String.format(
                    "Clock moved backwards.  Refusing to generate id for %d milliseconds", this.lastTimestamp - timestamp));
        }

        this.lastTimestamp = timestamp;
        return timestamp - twepoch << this.timestampLeftShift | this.workerId << this.workerIdShift | this.sequence;
    }


    private long tilNextMillis(final long lastTimestamp) {
        long timestamp = this.timeGen();
        while (timestamp <= lastTimestamp) {
            timestamp = this.timeGen();
        }
        return timestamp;
    }


    private long timeGen() {
        return System.nanoTime() / 1000000;
    }

}