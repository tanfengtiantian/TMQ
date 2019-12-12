package io.kafka.network.send;

import io.kafka.api.ProducerTTLRequest;
import io.kafka.common.ErrorMapping;
import io.kafka.common.exception.InvalidSendException;
import io.kafka.utils.Utils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import static io.kafka.common.ErrorMapping.NoError;

/**
 * @author tf
 * @version 创建时间：2019年2月28日 上午10:37:03
 * @ClassName ProducerTTLSend
 * message producerttl send
 * <p>
 * Producer header format:
 * <pre>
 * size + ErrorMapping
 *
 * <p>
 * Producer content format:
 * <pre>
 * Len(topic) + topic + partition + offset
 * =====================================
 * size		  : size(4bytes)
 * type		  : type(2bytes)
 * Len(topic) : Len(2bytes)
 * topic	  : size(2bytes) + data(utf-8 bytes)
 * partition  : int(4bytes)
 * offset     : long(8bytes)
 */
public class ProducerTTLSend extends AbstractSend {

	final ByteBuffer header = ByteBuffer.allocate(6);

	final ByteBuffer contentBuffer;

	public ProducerTTLSend(ProducerTTLRequest request, ErrorMapping errorCode) {
		switch (errorCode){
			case NoError:
				//header
				header.putInt(	2 //type(2bytes) ErrorMapping.NoError
						+ Utils.caculateShortString(request.topic) //topic
						+ 4 //int(4bytes)brokerId
						+ 4 //int(4bytes)partition
						+ 8 //long(8bytes)offset
				);
				header.putShort(errorCode.code);
				header.rewind();
				//content
				contentBuffer = ProducerTTLRequest.serializeProducerTTL(request);
				break;
			case InvalidMessageCode:
			case WrongPartitionCode:
				//header
				header.putInt(	2 //type(2bytes) ErrorMapping.NoError
				);
				header.putShort(errorCode.code);
				header.rewind();
				contentBuffer = ByteBuffer.allocate(0);
				break;
			default:
				throw new InvalidSendException("未知错误类型！");
		}
	}

	public ProducerTTLSend(ProducerTTLRequest request) {
		this(request, NoError);
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
