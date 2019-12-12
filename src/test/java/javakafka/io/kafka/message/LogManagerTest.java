package javakafka.io.kafka.message;

import io.kafka.config.ServerConfig;
import io.kafka.log.ILog;
import io.kafka.log.imp.FixedSizeRollingStrategy;
import io.kafka.log.imp.LogManager;
import io.kafka.message.MessageAndOffset;
import io.kafka.message.MessageSet;
import io.kafka.utils.Scheduler;
import io.kafka.utils.Utils;

import java.io.IOException;
import java.util.Properties;

/**
 * @author tf
 * @version 创建时间：2019年2月14日 下午3:58:41
 * @ClassName LogManagerTest
 */
public class LogManagerTest {

	public static void main(String[] args) throws IOException {
		Properties p = new Properties();
		//文件滚动大小
		p.setProperty("log.file.size", "1024");
		//强制将数据刷新到磁盘之前要接受的消息数
		p.setProperty("log.flush.interval", "10000");
		p.setProperty("log.dir", "E:\\tmp\\jafka-data");
		//定期日志过期任务执行间隔时间  分钟单位
		p.setProperty("log.cleanup.interval.mins", "1");
		//日志最大保留时间                          小时单位
		p.setProperty("log.retention.hours", 7*24+"");
		//定期任务刷盘时间                          毫秒单位
		p.setProperty("log.default.flush.scheduler.interval.ms", "1000");
		//刷盘文件时间间隔                           毫秒单位
		p.setProperty("log.default.flush.interval.ms", "1000");
		//默认分区数
		p.setProperty("num.partitions", "5");
		
		ServerConfig config = new ServerConfig(p);
		LogManager m =new LogManager(config,//
				new Scheduler(1, "kafka-logcleaner-", false),//
                1000L * 60 * config.getLogCleanupIntervalMinutes(),//
                1000L * 60 * 60 * config.getLogRetentionHours(),//
                true);
		m.setRollingStategy(new FixedSizeRollingStrategy(config.getLogFileSize()));
		m.load();
		m.startup();
		ILog log = m.getOrCreateLog("demo3", 0);
		
		long offset = 0;//1043;
		while (true) {
			long pre_offset = 0;
			// 读  messageAndOffset.offset 只能获取当前文件filechannel 的 offset
			MessageSet filechannel = log.read(offset, 64 * 1024); //64KB
			for (MessageAndOffset messageAndOffset : filechannel) {
				System.out.println(Utils.toString(messageAndOffset.message.payload(), "UTF-8"));
				//获取offset间距 累加  当前offset - 上一个 offset
		        offset = offset + (messageAndOffset.offset - pre_offset);
		        pre_offset = messageAndOffset.offset;
		        //System.err.println("offset = " + offset);
			}
		}
		
		
		/* 写
		Encoder encoder = new StringEncoder();
		for (int i = 0; i < 100; i++) {
			System.err.println(i);
			Message message = encoder.toMessage("Hello world #1-"+i);
			ByteBufferMessageSet bbms = new ByteBufferMessageSet(CompressionCodec.NoCompressionCodec, message);
			log.append(bbms);
		}*/
		
		//System.err.println("#####################end");


	}
	
}