package io.kafka.api;

import io.kafka.message.ByteBufferMessageSet;
import io.kafka.network.request.Request;
import io.kafka.utils.Utils;

import java.nio.ByteBuffer;


/**
 * @author tf
 * @version 创建时间：2019年6月5日 下午2:23:29
 * @ClassName 服务提供请求类
 * message transaction request
 * <p>
 * TransactionRequest format:
 * <pre>
 * BeginTransaction,Rollback,Prepare,Commit
 * size + type + transactionType + Len(xid) + xid
 * PutCommand
 * size + type + transactionType + Len(xid) + xid + Len(topic) + topic + partition + messageSize + message
 * =====================================
 * size		       : size(4bytes)   32位
 * type		       : type(2bytes)   16位
 * transactionType : type(2bytes)  0-BeginTransaction,1-PutCommand,2-Rollback,3-Prepare,4-Commit
 * Len(xid)        : Len(2bytes)
 * xid             : data(utf-8 bytes)
 * Len(topic)      : Len(2bytes)
 * topic	       : data(utf-8 bytes)
 * partition       : int(4bytes)
 * messageSize:    : int(4bytes)
 * message         : bytes
 */
public class TransactionRequest implements Request {

    public static final int BeginTransaction=0;
    public static final int PutCommand=1;
    public static final int Rollback=2;
    public static final int Prepare=3;
    public static final int Commit=4;
    private String topic = null;
    private Integer partition = null;
    private int transactionType;
    private int transactionTimeout = 10;
    private final String xid;
    private int brokerId;

    /**
     * 分区
     */
    public static final int RandomPartition = -1;
    /**
     * request messages
     */
    private ByteBufferMessageSet messages;

    public static ByteBuffer serializeTransactionProducer(TransactionRequest request) {
        switch (request.getTransactionType()){
            case BeginTransaction:
            case Rollback:
            case Prepare:
            case PutCommand:
            case Commit:
                ByteBuffer buffer = ByteBuffer.allocate(2 + 4 + Utils.caculateShortString(request.getTransactionId()));
                buffer.putShort((short)request.getTransactionType());
                buffer.putInt(request.getBrokerId());
                Utils.writeShortString(buffer, request.getTransactionId());
                buffer.rewind();
                return buffer;
        }

        return ByteBuffer.allocate(0);
    }


    /**
     * 选择top分区Partition
     * @param chooser
     * @return
     */
    public int getTranslatedPartition(PartitionChooser chooser) {
        if (partition == RandomPartition) {
            partition = chooser.choosePartition(topic);
        }
        return partition;
    }
    @Override
    public RequestKeys getRequestKey() { return RequestKeys.TRANSACTION; }

    /**
     * Len(topic) + topic + partition + transactionType + messageSize + message
     * @param buffer
     * @return
     */
    public static TransactionRequest readFrom(ByteBuffer buffer){
        int transactionType = buffer.getShort();
        String xid = Utils.readShortString(buffer);
        switch (transactionType){
            case BeginTransaction:
                int transactionTimeout = buffer.getInt();
                return new TransactionRequest(transactionType, xid, transactionTimeout);
            case Rollback:
            case Prepare:
            case Commit:
                return new TransactionRequest(transactionType, xid);
            case PutCommand:
                String topic = Utils.readShortString(buffer);
                int partition = buffer.getInt();
                int messageSetSize = buffer.getInt();
                //创建子缓冲区
                ByteBuffer messageSetBuffer = buffer.slice();
                messageSetBuffer.limit(messageSetSize);
                buffer.position(buffer.position() + messageSetSize);
                return new TransactionRequest(topic,partition,transactionType,xid,new ByteBufferMessageSet(messageSetBuffer));
        }
        return null;
    }

    public TransactionRequest(int transactionType, String xid) {
        this.transactionType = transactionType;
        this.xid = xid;
    }

    public TransactionRequest(int transactionType, String xid, int transactionTimeout) {
        this.transactionType = transactionType;
        this.xid = xid;
        this.transactionTimeout = transactionTimeout;
    }


    public TransactionRequest(String topic, int partition, int transactionType, String xid, ByteBufferMessageSet messages) {
        this.topic = topic;
        this.partition = partition;
        this.transactionType=transactionType;
        this.xid=xid;
        this.messages=messages;
    }

    public ByteBufferMessageSet getMessages() {
        return messages;
    }

    public int getTransactionType() {
        return transactionType;
    }

    public int getPartition() {
        return partition;
    }

    public String getTopic() {
        return topic;
    }

    public String getTransactionId() { return xid; }

    public int getBrokerId() {
        return brokerId;
    }

    public int getTransactionTimeout() {
        return transactionTimeout;
    }
    public void setBrokerId(int brokerId) { this.brokerId = brokerId; }

    public ByteBuffer getFullbuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(getSizeInBytes());
        buffer.putShort((short) transactionType);
        Utils.writeShortString(buffer, xid);
        if(topic != null){
            Utils.writeShortString(buffer, topic);
        }
        if(partition != null){
            buffer.putInt(partition);
        }
        if(messages != null){
            final ByteBuffer sourceBuffer = messages.serialized();
            buffer.putInt(sourceBuffer.limit());
            buffer.put(sourceBuffer);
            sourceBuffer.rewind();
        }
        return buffer;
    }

    public int getSizeInBytes() {
        return  (int)(2 // transactionType : type(2bytes)
                + Utils.caculateShortString(xid)//Len(xid) : Len(2bytes)  + xid:bytes
                + (topic == null ? 0 : Utils.caculateShortString(topic))  //Len(topic) : Len(2bytes)  + topic : bytes
                + (partition == null ? 0 : 4) //partition : 4bytes
                + (messages == null ? 0 : 4 + messages.getSizeInBytes()));//messageSize:: int(4bytes)  +  message : bytes
    }

}
