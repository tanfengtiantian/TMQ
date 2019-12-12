package io.kafka.plugin;

import io.kafka.common.exception.ServerStartupException;
import io.kafka.config.ServerConfig;
import io.kafka.http.JettyBroker;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class BrokerPlugins extends AbstractBrokerPlugin {

    /**
     * 已注册的plugins
     */
    private final Map<String/* plugin name */, BrokerPlugin> plugins = new HashMap<>();

    /**
     * 需要启动的plugins,上下文参数
     */
    private final Map<String/* plugin name */, Properties> pluginsInfo = new HashMap<>();

    private final AtomicBoolean isInited = new AtomicBoolean(false);

    public BrokerPlugins(final Map<String, Properties> pluginsInfo, ServerConfig config){

        this.config = config;

        this.register(JettyBroker.class);

        //需要启动的plugins 赋值
        if (pluginsInfo != null) {
            this.pluginsInfo.putAll(pluginsInfo);
        }

        this.checkPluginsInfo(this.plugins, this.pluginsInfo);

    }

    private void checkPluginsInfo(Map<String, BrokerPlugin> plugins, Map<String, Properties> pluginsInfo) {
        if (pluginsInfo == null || pluginsInfo.isEmpty()) {
            logger.info("no broker plugin");
            return;
        }

        // 作为异步复制Slave启动时特殊处理，不启动其他plugin
        if (config.isSlave()) {
            logger.info("start slaver,unstart other plugins");
            final Properties slaveProperties = pluginsInfo.get("slave");
            pluginsInfo.clear();
            pluginsInfo.put("slave", slaveProperties);
        }

        for (final String name : pluginsInfo.keySet()) {
            logger.info("cmd line require start plugin:" + name);
            if (plugins.get(name) == null) {
                throw new ServerStartupException("unknown broker plugin:" + name);
            }
        }
    }


    void register(final Class<? extends BrokerPlugin> pluginClass) {
        try {
            final BrokerPlugin plugin = pluginClass.getConstructor(new Class[0]).newInstance();
            this.plugins.put(plugin.name(), plugin);
        }
        catch (final Exception e) {
            throw new ServerStartupException("Register broker plugin failed", e);
        }
    }

    @Override
    public void init(BrokerContext context, Properties props) {
        if (this.isInited.compareAndSet(false, true)) {
            new InnerPluginsRunner() {
                @Override
                protected void doExecute(final BrokerPlugin plugin) {
                    logger.info("Start inited broker plugin:[" + plugin.name() + ":" + plugin.getClass().getName() + "]");
                    plugin.init(context, BrokerPlugins.this.pluginsInfo.get(plugin.name()));
                    logger.info("Inited broker plugin:[" + plugin.name() + ":" + plugin.getClass().getName() + "]");
                }
            }.execute();
        }
    }

    @Override
    public void start() {
        if (!this.isInited.get()) {
            logger.warn("Not inited yet");
            return;
        }

        new InnerPluginsRunner() {
            @Override
            protected void doExecute(final BrokerPlugin plugin) {
                plugin.start();
                logger.info("Started broker plugin:[" + plugin.name() + ":" + plugin.getClass().getName() + "]");
            }
        }.execute();
    }

    @Override
    public void stop() {
        new InnerPluginsRunner() {
            @Override
            protected void doExecute(final BrokerPlugin plugin) {
                plugin.stop();
                BrokerPlugins.logger.info("stoped broker plugin:[" + plugin.name() + ":" + plugin.getClass().getName()
                        + "]");
            }
        }.execute();
    }

    @Override
    public String name() {
        return null;
    }

    private abstract class InnerPluginsRunner {

        public void execute() {
            for (final BrokerPlugin plugin : BrokerPlugins.this.plugins.values()) {
                if (BrokerPlugins.this.pluginsInfo.containsKey(plugin.name())) {
                    BrokerPlugins.logger.warn("Starting plugin:" + plugin.name());
                    this.doExecute(plugin);
                    BrokerPlugins.logger.warn("Start plugin:" + plugin.name() + " successfully.");
                }
            }
        }


        protected abstract void doExecute(BrokerPlugin plugin);
    }
}
