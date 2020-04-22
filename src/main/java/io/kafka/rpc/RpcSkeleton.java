package io.kafka.rpc;

import java.lang.reflect.Method;

public class RpcSkeleton {

    private final Object bean;
    private final String beanName;


    public RpcSkeleton(String beanName, Object bean) {
        super();
        this.beanName = beanName;
        this.bean = bean;
    }


    public Object invoke(String methodName, Object[] args) throws Exception {
        Method method = this.getMethod(methodName, args);
        if (method == null) {
            throw new Exception("Unknow method in " + this.beanName + ":" + methodName);
        }
        try {
            return method.invoke(this.bean, args);
        }
        catch (Exception e) {
            throw new Exception("Invoke " + this.beanName + "." + methodName + " failure", e);
        }
    }


    private Method getMethod(String methodName, Object[] args) {
        Method method = null;
        Class<?> clazz = this.bean.getClass();
        try {
            if (args != null && args.length > 0) {
                Class<?>[] parameterTypes = new Class<?>[args.length];
                for (int i = 0; i < args.length; i++) {
                    parameterTypes[i] = args[i] != null ? args[i].getClass() : null;
                }
                method = clazz.getMethod(methodName, parameterTypes);
            }
            else {
                method = clazz.getMethod(methodName);
            }

        }
        catch (NoSuchMethodException e) {
        }
        if (method == null) {
            Method[] methods = clazz.getMethods();
            for (Method m : methods) {
                if (m.getName().equals(methodName) && m.getParameterTypes().length == args.length) {
                    method = m;
                    break;
                }
            }
        }
        return method;
    }
}
