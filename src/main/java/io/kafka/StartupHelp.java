package io.kafka;

import io.kafka.common.exception.ServerStartupException;
import io.kafka.utils.Utils;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Properties;

public class StartupHelp {

    private static final Logger log = LoggerFactory.getLogger(StartupHelp.class);

    public static CommandLine parseCmdLine(final String[] args, final CommandLineParser parser) {
        return parseCmdLine(args, options(), parser);
    }


    public static CommandLine parseCmdLine(final String[] args, final Options options, final CommandLineParser parser) {
        final HelpFormatter hf = new HelpFormatter();
        try {
            return parser.parse(options, args);
        } catch (final ParseException e) {
            hf.printHelp("ServerStartup", options, true);
            log.error("Parse command line failed", e);
            throw new ServerStartupException("Parse command line failed", e);
        }
    }


    public static Options options() {
        final Options options = new Options();
        final Option brokerFile = new Option("f", true, "Broker configuration file path");
        final Option localMode = new Option("l", false, "Broker configuration file path");
        //是否必输项
        localMode.setRequired(false);
        brokerFile.setRequired(false);
        options.addOption(brokerFile);
        options.addOption(localMode);
        // 此处定义参数类似于 java 命令中的 -D<name>=<value>
        final Option pluginParams =
                OptionBuilder.withArgName("pluginname=configfile").hasArgs(2).withValueSeparator()
                        .withDescription("use value for given param").create("F");
        options.addOption(pluginParams);

        return options;
    }


    public static Properties getProps(final String path) {
        try {
            return Utils.getResourceAsProperties(path, "GBK");
        } catch (final IOException e) {
            throw new ServerStartupException("Parse configuration failed,path=" + path, e);
        }
    }
}
