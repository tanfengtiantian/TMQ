package server.example.rpc;

import io.kafka.StartupHelp;
import io.kafka.config.ServerConfig;
import io.kafka.server.RpcService;
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
 * @ClassName rpc服务测试启动类
 */
public class RpcServiceTest {

	public static void main(String[] args) {

		Properties p = new Properties();
		//##################server
		//port
		p.setProperty("port","9093");
		//最大处理线程数
		p.setProperty("num.threads","3");
		//服务器接收包最大缓冲区10 * 1024 * 1024 10M
		p.setProperty("max.socket.request.bytes", 10 * 1024 * 1024+"");
		//通道最大发送数据
		p.setProperty("socket.send.buffer", 100 * 1024 +"");
		//通道最大接收数据
		p.setProperty("socket.receive.buffer", 100 * 1024 +"");
		//##################rpc 配置
		//bean
		p.setProperty("bean.hello", "io.kafka.api.rpc.imp.Hello");
		ServerConfig config = new ServerConfig(p);
		new RpcService(config).startup();
	}
}
