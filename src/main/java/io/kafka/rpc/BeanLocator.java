package io.kafka.rpc;

/**
 * Bean查找器
 *
 */
public interface BeanLocator {
     Object getBean(String name);
}
