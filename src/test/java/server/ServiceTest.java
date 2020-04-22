package server;

import io.kafka.StartupHelp;
import io.kafka.config.ServerConfig;
import io.kafka.server.Service;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.PosixParser;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author tf
 * @version 创建时间：2019年1月17日 下午6:02:19
 * @ClassName 服务测试启动类
 */
public class ServiceTest {

	@SuppressWarnings("resource")
	public static void main(String[] args) throws IOException, InterruptedException {

		args = new String[]{"-FjettyBroker=jettyBroker.properties"};

		final CommandLine line = StartupHelp.parseCmdLine(args, new PosixParser());

		final Map<String, Properties> pluginsInfo = getPluginsInfo(line);

		Properties p = new Properties();
		//文件滚动大小
		//p.setProperty("log.file.size", 536870912+"");
		p.setProperty("log.file.size", (1024*1024)+"");
		//强制将数据刷新到磁盘之前要接受的消息数
		p.setProperty("log.flush.interval", "10000");
		//p.setProperty("log.dir", "E:\\tmp\\zx-data");
		p.setProperty("log.dir","/Users/zxcs/Desktop/jafka-data");
		//定期日志过期任务执行间隔时间  分钟单位
		p.setProperty("log.cleanup.interval.mins", "1");
		//日志最大保留时间                          小时单位
		p.setProperty("log.retention.hours", 7+"");
		//定期任务刷盘时间                          毫秒单位
		p.setProperty("log.default.flush.scheduler.interval.ms", "1000");
		//刷盘文件时间间隔                           毫秒单位
		p.setProperty("log.default.flush.interval.ms", "1000");
		//##################server
		//最大处理线程数
		p.setProperty("num.threads","3");
		//服务器接收包最大缓冲区10 * 1024 * 1024 10M
		p.setProperty("max.socket.request.bytes", 10 * 1024 * 1024+"");
		//通道最大发送数据
		p.setProperty("socket.send.buffer", 100 * 1024 +"");
		//通道最大接收数据
		p.setProperty("socket.receive.buffer", 100 * 1024 +"");
		//默认分区数
		p.setProperty("num.partitions", "2");
		//brokerid
		p.setProperty("brokerid", "1");
		p.setProperty("enable.zookeeper", "false");
		p.setProperty("zk.connect", "localhost:2181");
		p.setProperty("hostname", "localhost");
		//;slave编号,大于等于0表示作为slave启动,同一个master下的slave编号应该设不同值.
		//;没配置或小于0时作为master启动
		p.setProperty("slaveId", "-1");
		ServerConfig config = new ServerConfig(p);
		new Service(config, pluginsInfo).startup();
	}

	static Map<String, Properties> getPluginsInfo(final CommandLine line) {
		final Properties properties = line.getOptionProperties("F");
		final Map<String, Properties> pluginsInfo = new HashMap<>();
		if (properties != null) {
			for (final Map.Entry<Object, Object> entry : properties.entrySet()) {
				pluginsInfo.put(String.valueOf(entry.getKey()), StartupHelp.getProps(String.valueOf(entry.getValue())));
			}
		}
		return pluginsInfo;
	}
	public static void getProcessId(){
		RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
		System.err.println(runtimeMXBean.getName());
	}

}
