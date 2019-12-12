package io.kafka.network.send;


import io.kafka.api.OffsetRequest;
import io.kafka.common.ErrorMapping;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.List;

/**
 * @author tf
 * @version 创建时间：2019年2月14日 上午11:12:49
 * @ClassName OffsetArraySend
 */
public class OffsetArraySend extends AbstractSend {

	   final ByteBuffer header = ByteBuffer.allocate(6);
	   final ByteBuffer contentBuffer;
	   public OffsetArraySend(List<Long> offsets) {
	       header.putInt(4 + offsets.size()*8 +2);
	       header.putShort(ErrorMapping.NoError.code);
	       header.rewind();
	       contentBuffer = OffsetRequest.serializeOffsetArray(offsets);
	   }
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