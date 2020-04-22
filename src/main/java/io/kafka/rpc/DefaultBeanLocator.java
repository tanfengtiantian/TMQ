package io.kafka.rpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultBeanLocator implements BeanLocator {

    private static final Logger logger = LoggerFactory.getLogger(DefaultBeanLocator.class);

    final ConcurrentHashMap<String,Object> beans = new ConcurrentHashMap<>();

    final Properties props;

    public DefaultBeanLocator(Properties props) {
        this.props = props;
    }

    @Override
    public Object getBean(String name) {
        synchronized (this){
            if(beans.containsKey(name)){
                return beans.get(name);
            }else {
                Object bean = null;
                String className = props.getProperty("bean."+name);
                if(className != null && className != "") {
                    try {
                        bean = createObj(className);
                        beans.put(name,bean);
                    } catch (Exception e) {
                        logger.error("BeanLocator:createBean is Error");
                        e.printStackTrace();
                    }finally {
                        return bean;
                    }
                }
                return bean;
            }
        }
    }

    public Object createObj(String className) throws Exception{
        return Class.forName(className).newInstance();
    }

    public static void main(String[] args) throws Exception {
        DefaultBeanLocator defaultBeanLocator = new DefaultBeanLocator(null);
        Object obj = defaultBeanLocator.createObj("io.kafka.api.rpc.imp.Hello");
        System.out.println(obj.toString());
    }
}
