package io.kafka.api;

import io.kafka.network.request.Request;
import io.kafka.utils.Utils;
import java.nio.ByteBuffer;

public class RpcRequest implements Request {

    private final String beanName;

    private final String methodName;

    private Object[] args;

    public RpcRequest(String beanName, String methodName, Object[] args) {
        this.beanName = beanName;
        this.methodName = methodName;
        this.args = args;
    }

    public static RpcRequest readFrom(ByteBuffer buffer) {
        String className = Utils.readShortString(buffer);
        String methodName = Utils.readShortString(buffer);
        Object[] arguments = null;
        final int argumentCount = buffer.getInt();
        if(argumentCount > 0) {
            arguments = new Object[argumentCount];
            int argumentsDataSize = buffer.getInt();
            byte[] data = new byte[argumentsDataSize];
            buffer.get(data);
            Utils.jdkDeserialization(arguments, data);
        }
        return new RpcRequest(className,methodName,arguments);
    }

    public String getBeanName() {
        return beanName;
    }

    public Object[] getArgs() {
        return args;
    }

    public String getMethodName() {
        return methodName;
    }


    @Override
    public RequestKeys getRequestKey() {
        return RequestKeys.RPC;
    }
}
