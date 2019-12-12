package io.kafka.ttl;

import io.kafka.config.ServerConfig;
import io.kafka.log.ILog;
import io.kafka.log.ILogManager;
import io.kafka.message.ByteBufferMessageSet;
import io.kafka.transaction.store.JournalLocation;
import io.kafka.utils.NamedThreadFactory;
import io.kafka.utils.nettyloc.ByteBuf;
import io.kafka.utils.timer.HashedWheelTimer;
import io.kafka.utils.timer.Timeout;
import io.kafka.utils.timer.Timer;
import io.kafka.utils.timer.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class WheelTimerDelay {
    private final static Logger logger = LoggerFactory.getLogger(WheelTimerDelay.class);
    ConcurrentHashMap<TimerTask, JournalLocation> delayTimerMap =
            new ConcurrentHashMap<>();
    final Timer ttlTimeoutTimer;
    final DelayMessageStore delayStore;
    // 延迟消息最大堆积10万个
    private int maxTxTimeoutTimerCapacity = 100000;

    public WheelTimerDelay(ILogManager logManager,ServerConfig config) {
        this.ttlTimeoutTimer =
                new HashedWheelTimer(new NamedThreadFactory("TTL-Timeout-Timer"), 500, TimeUnit.MILLISECONDS, 512,
                        maxTxTimeoutTimerCapacity);
        delayStore= new DelayStore(logManager,this,config);
        try {
            delayStore.recover();
        }catch (IOException e){
            logger.error("delay-recover-error:"+e.getMessage());
        }
    }

    public void addMessage(ILog log, ByteBufferMessageSet message, long seconds,JournalLocation location) throws IOException {
        //step1 new直接内存 size Len(topic) + topic + partition + ttl + messageSize + message
        final ByteBuf buf = delayStore.directBuffer(log,message);
        //设置ttl
        Timeout ttl=ttlTimeoutTimer.newTimeout(timeout -> {
            //要保证写入磁盘顺序
            synchronized (buf) {
                ByteBufferMessageSet messages = delayStore.readDirectBytesAndRelease(buf,delayTimerMap.remove(timeout.getTask()));
                //归档消息
                List<Long> offset = log.append(messages);
                long messageSize = messages.getSizeInBytes();
                logger.info("延迟消息写入 logs ["+log+"] 已将消息("+messageSize+") [" + offset.get(0) + "] 写入磁盘 .");
            }
        }, seconds, TimeUnit.SECONDS);
        if(location == null){
            //写入直接内存 持久化磁盘
            delayTimerMap.put(ttl.getTask(),delayStore.addMessageAndWritten(buf,log,message,ttl.getDeadline()));
        }else {
            //写入直接内存
            delayStore.addMessage(buf,log,message,ttl.getDeadline());
            delayTimerMap.put(ttl.getTask(),location);
        }
    }
}
