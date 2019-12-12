package io.kafka.transaction;


import io.kafka.api.TransactionRequest;
import io.kafka.transaction.store.JournalLocation;
import io.kafka.transaction.store.JournalStore;
import io.kafka.utils.Utils;
import java.nio.ByteBuffer;

public class TxCommand {

    private final short cmdType;

    private final TransactionId xid;

    private final ByteBuffer attachment;

    private final JournalLocation location;

    private final TransactionRequest request;

    private final long msgId;

    public TxCommand(short cmdType, long msgId, TransactionId xid, TransactionRequest request, ByteBuffer attachment, JournalLocation location){
        this.cmdType=cmdType;
        this.msgId=msgId;
        this.xid=xid;
        this.request=request;
        this.attachment=attachment;
        this.location=location;
    }


    public static TxCommand parseFrom(ByteBuffer cmdBuf){
        short cmdType = cmdBuf.getShort();
        switch (cmdType){
            case JournalStore.APPEND_MSG:
                long msgId = cmdBuf.getLong();
                int messageSetSize = cmdBuf.getInt();
                ByteBuffer messageSetBuffer = cmdBuf.slice();
                messageSetBuffer.limit(messageSetSize);
                cmdBuf.position(cmdBuf.position() + messageSetSize);
                return new TxCommand(cmdType, msgId,null, TransactionRequest.readFrom(messageSetBuffer), null, null);
            case JournalStore.TX_OP:
                String xid = Utils.readShortString(cmdBuf);
                int attachmentSetSize = cmdBuf.getInt();
                if(attachmentSetSize <= 0)
                    return new TxCommand(cmdType, -1,TransactionId.valueOf(xid), null, null, null);
                ByteBuffer attachmentSetBuffer = cmdBuf.slice();
                attachmentSetBuffer.limit(attachmentSetSize);
                cmdBuf.position(cmdBuf.position() + attachmentSetSize);
                return new TxCommand(cmdType,-1, TransactionId.valueOf(xid), null, attachmentSetBuffer, null);
            default:
                throw new IllegalStateException("Unexpected cmdType: " + cmdType);
        }
    }

    public TransactionId getXid() {
        return xid;
    }

    public TransactionRequest getTransactionRequest() {
        return request;
    }

    public ByteBuffer getAttachment() {
        return attachment;
    }

    public JournalLocation getLocation() {
        return location;
    }

    public int getCmdType() {
        return cmdType;
    }

    public long getMsgId() {
        return msgId;
    }
}
