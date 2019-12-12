package io.kafka.network.send;


import io.kafka.api.TransactionRequest;
import io.kafka.common.ErrorMapping;
import io.kafka.utils.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

/**
 * @author tf
 * @version 创建时间：2019年6月22日 上午10:37:03
 * @ClassName TransactionSend
 * message transaction send
 * <p>
 * transaction header format:
 * <pre>
 * size + ErrorMapping
 * <p>
 * transaction content format:
 * <pre>
 * Len(xid) + xid
 *
 * BeginTransaction,Rollback,Prepare,Commit
 * size + type + transactionType + Len(xid) + xid
 * =====================================
 * size		  : size(4bytes)
 * type		  : type(2bytes)
 * Len(topic) : Len(2bytes)
 * topic	  : size(2bytes) + data(utf-8 bytes)
 * partition  : int(4bytes)
 * offset     : long(8bytes)
 */
public class TransactionSend extends AbstractSend {

    protected final ByteBuffer header = ByteBuffer.allocate(6);
    protected ByteBuffer contentBuffer;

    public TransactionSend(TransactionRequest request) {
        //header
        header.putInt(	2 //type(2bytes) ErrorMapping.NoError
                + 2 //transactionType
                + 4 //brokerId
                + Utils.caculateShortString(request.getTransactionId()) //xid
        );
        header.putShort(ErrorMapping.NoError.code);
        header.rewind();
        contentBuffer = TransactionRequest.serializeTransactionProducer(request);
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
