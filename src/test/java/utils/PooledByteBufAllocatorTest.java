package utils;



import io.kafka.utils.nettyloc.ByteBuf;
import io.kafka.utils.nettyloc.PooledByteBufAllocator;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;


public class PooledByteBufAllocatorTest {
    private static PooledByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;
    private static int ByteBufSize = 100;
    public static void main(String args[]) throws InterruptedException {
        List<ByteBuf> list =new LinkedList<ByteBuf>();
        for(int t = 0; t < ByteBufSize; t++){
            ByteBuffer buffer = ByteBuffer.allocate(10);
            for(int i = 0; i < 10; i++){
                buffer.put((byte)i);
            }
            byte[] bt = buffer.array();
            ByteBuf byteBuf = allocator.directBuffer(bt.length);
            byteBuf.writeBytes(bt);
            list.add(byteBuf);
        }


        for(int t = 0; t < list.size(); t++){
            ByteBuf byteBuf = list.get(t);
            byte[] bt = new byte[byteBuf.capacity()];
            byteBuf.readBytes(bt);
            byteBuf.release();
        }


        Thread.sleep(Integer.MAX_VALUE);
    }
}


/*
public class PooledByteBufAllocatorTest {

    private static PooledByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;

    public static void main(String[] args) {
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        Integer pid = Integer.valueOf(runtimeMXBean.getName().split("@")[0]);
        System.out.println(pid);
        List<ByteBuf> list =new LinkedList<>();
        while (true) {
            try {
                // 分配 100M
                ByteBuf byteBuf = allocator.directBuffer(1024 * 1024 * 100);
                list.add(byteBuf);
                Thread.sleep(3000);

                // 释放
                // byteBuf.release();
            } catch (Exception e) {

            }
        }
    }
}
*/