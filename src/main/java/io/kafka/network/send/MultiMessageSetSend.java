package io.kafka.network.send;

import java.util.ArrayList;
import java.util.List;

/**
 * @author tf
 * @version 创建时间：2019年2月11日 上午11:44:38
 * @ClassName MultiMessageSetSend
 */
public class MultiMessageSetSend extends MultiSend<Send> {

    public MultiMessageSetSend(List<MessageSetSend> sets) {
        super();
        final ByteBufferSend sizeBuffer = new ByteBufferSend(6);
        List<Send> sends = new ArrayList<Send>(sets.size() + 1);
        sends.add(sizeBuffer);
        int allMessageSetSize = 0;
        for (MessageSetSend send : sets) {
            sends.add(send);
            allMessageSetSize += send.getSendSize();
        }
        //write head size
        sizeBuffer.getBuffer().putInt(2 + allMessageSetSize);//4
        sizeBuffer.getBuffer().putShort((short) 0);//2
        sizeBuffer.getBuffer().rewind();
        super.expectedBytesToWrite = 4 + 2 + allMessageSetSize;
        //
        super.setSends(sends);
    }

}