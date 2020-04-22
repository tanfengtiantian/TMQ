package io.kafka.utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultExceptionMonitor extends ExceptionMonitor {
    private static final Logger log = LoggerFactory.getLogger(DefaultExceptionMonitor.class);


    @Override
    public void exceptionCaught(final Throwable cause) {
        if (this.log.isErrorEnabled()) {
            this.log.error("Gecko unexpected exception", cause);
        }
    }
}