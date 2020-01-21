package io.kafka.http;

import io.kafka.network.session.SessionContextManager;
import io.kafka.plugin.AbstractBrokerPlugin;
import io.kafka.plugin.BrokerContext;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.NetworkConnector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.ThreadPool;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * 代表一个基于Jetty支持Http的tfkafka Server
 */
public class JettyBroker extends AbstractBrokerPlugin {

    private static final Logger logger = LoggerFactory.getLogger(JettyBroker.class);

    private Server server;

    private int port;

    @Override
    public void init(BrokerContext context, Properties props) {
        JettyProcessor jettyProcessor = new JettyProcessor(context, props);
        this.port = Integer.valueOf(props.getProperty("serverPort", "8888"));
        server = new Server(createThreadPool());
        server.addConnector(createConnector(port));
        server.setHandler(createHandlers(jettyProcessor));
        //注册会话监听
        SessionContextManager.registerHandler(jettyProcessor);

    }

    public void start() {
        try {
            server.start();
            logger.info("The JettyBroker run on port {}", port);
        } catch (Exception e) {
            logger.error("JettyBroker start:" + e.getMessage());
        }
    }

    public void stop() {
        try {
            server.stop();
        } catch (Exception e) {
            logger.error("JettyBroker stop:" + e.getMessage());
        }
    }

    @Override
    public String name() {
        return "jettyBroker";
    }

    private ThreadPool createThreadPool () {
        QueuedThreadPool threadPool = new QueuedThreadPool();
        threadPool.setMinThreads(2);
        threadPool.setMaxThreads(5);
        return threadPool;
    }

    private NetworkConnector createConnector (Integer port) {
        ServerConnector connector = new ServerConnector(server);
        connector.setPort(port);
        return connector;
    }

    private HandlerCollection createHandlers (JettyProcessor jettyProcessor) {
        // 静态资源访问
        ContextHandler contextHandler = new ContextHandler("/");
        ResourceHandler handler = new ResourceHandler();
        handler.setDirectoriesListed(true);
        handler.setWelcomeFiles(new String[]{"index.html"});
        handler.setBaseResource(Resource.newResource(JettyBroker.class.getClassLoader().getResource("dist")));
        contextHandler.setHandler(handler);

        // 数据接口
        ServletContextHandler servletHandler = new ServletContextHandler();
        //servletHandler.addServlet(JettyProcessor.class, "/rest");
        servletHandler.addServlet(new ServletHolder(jettyProcessor),"/rest");

        HandlerCollection handlerCollection = new HandlerCollection();
        handlerCollection.setHandlers(new Handler[] {contextHandler, servletHandler});
        return handlerCollection;
    }
}
