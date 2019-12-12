package io.kafka.plugin;

import io.kafka.config.ServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;


abstract public class AbstractBrokerPlugin implements BrokerPlugin {

    protected static final Logger logger = LoggerFactory.getLogger(AbstractBrokerPlugin.class);

    protected ServerConfig config;

    protected Properties props;


    @Override
    public boolean equals(final Object obj) {

        if (this == obj) {
            return true;
        }
        if (obj instanceof BrokerPlugin) {
            final BrokerPlugin that = (BrokerPlugin) obj;
            if (this.name() == that.name()) {
                return true;
            }
            if (this.name() != null) {
                return this.name().equals(that.name());
            }
            if (this.name() == null) {
                return that.name() == null;
            }
        }
        return false;
    }


    @Override
    public int hashCode() {
        return this.name() != null ? this.name().hashCode() : 0;
    }
}