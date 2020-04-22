package io.kafka.rpc;


import io.kafka.common.ErrorMapping;
import io.kafka.network.send.AbstractSend;
import io.kafka.utils.Utils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

public class RpcSend extends AbstractSend {

    final ByteBuffer header = ByteBuffer.allocate(6);
    final ByteBuffer contentBuffer;
    public RpcSend(Object result){
        this(result,ErrorMapping.NoError);
    }

    public RpcSend(Object result, ErrorMapping errorCode){
        switch (errorCode){
            case NoError:
                //header
                byte[] bytes = Utils.jdkSerializable(result);
                header.putInt(	2 //type(2bytes) ErrorMapping.NoError
                        + 4
                        + bytes.length //
                );
                header.putShort(errorCode.code);
                header.rewind();
                contentBuffer = ByteBuffer.allocate(bytes.length+4);
                contentBuffer.putInt(bytes.length);
                contentBuffer.put(bytes);
                contentBuffer.rewind();
                break;
            default:
                header.putInt(	2 //type(2bytes) ErrorMapping.NoError
                );
                header.putShort(ErrorMapping.UnkonwCode.code);
                header.rewind();
                contentBuffer = ByteBuffer.allocate(0);

        }
    }

    @Override
    public int writeTo(GatheringByteChannel channel) throws IOException {
        expectIncomplete();
        int written = 0;
        if(header.hasRemaining()) {
            written += channel.write(header);
        }
        if(!header.hasRemaining() && contentBuffer.hasRemaining()) {
            written += channel.write(contentBuffer);
        }
        if(!contentBuffer.hasRemaining()) {
            setCompleted();
        }
        return written;
    }
}
