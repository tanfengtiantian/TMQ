package io.kafka.plugin;

import java.util.Properties;

public interface BrokerPlugin {

    void init(BrokerContext context, Properties props);

    void start();

    void stop();

    String name();
}
