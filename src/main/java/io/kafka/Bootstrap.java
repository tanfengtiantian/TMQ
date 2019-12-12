package io.kafka;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.kafka.config.ServerConfig;
import io.kafka.server.Service;
import io.kafka.utils.Utils;

public class Bootstrap implements Closeable {

	private volatile Thread shutdownHook;
	private Service service;
	private static final Logger logger = LoggerFactory.getLogger(Bootstrap.class);
	private final Map<String, Properties> pluginsInfo;

	public Bootstrap(Map<String, Properties> pluginsInfo){
		this.pluginsInfo = pluginsInfo;
	}
	public void start(){
		logger.info("load server.properties");
		start(Utils.loadProps("server.properties"));
	}
	public void start(Properties mainProperties){
		logger.info("load server start");
		start(new ServerConfig(mainProperties));
	}
	
	public void start(ServerConfig config){
		service =new Service(config,pluginsInfo);
		shutdownHook = new Thread() {
            @Override
            public void run() {
            	service.close();
            	service.awaitShutdown();
            }
        };
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        //
        service.startup();
	}
	public void awaitShutdown() {
        if (service != null) {
        	service.awaitShutdown();
        }
    }
	public static void main(String[] args) throws IOException {
		final CommandLine line = StartupHelp.parseCmdLine(args, new PosixParser());
		Bootstrap bootstrap = new Bootstrap(getPluginsInfo(line));
		bootstrap.start();
		bootstrap.awaitShutdown();
		bootstrap.close();
	}
	@Override
	public synchronized void close() {
       if (shutdownHook != null) {
          try {
             Runtime.getRuntime().removeShutdownHook(shutdownHook);
          } catch (IllegalStateException ex) {
        	  
          }
          shutdownHook.run();
          shutdownHook = null;
      }
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
	
}
