package utils;

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class UnsafeUtilTest {

    public static Unsafe UNSAFE;

    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        ByteBuffer buffer = ByteBuffer.allocateDirect(4);
        long addresses = ((DirectBuffer) buffer).address();
        byte[] data = new byte[4];
        data[0]=1;
        data[1]=2;
        data[2]=3;
        data[3]=4;
        System.out.println(Arrays.toString(data));
        //内存堆内-拷贝堆外
        UNSAFE.copyMemory(data, 16, null, addresses, 4);

        System.out.println(buffer.get());
        System.out.println(buffer.get());
        System.out.println(buffer.get());
        System.out.println(buffer.get());
        //堆外堆内-拷贝堆内
        byte[] bytes = new byte[4];
        UNSAFE.copyMemory(null, addresses, bytes, 16, 4);
        System.out.println(Arrays.toString(bytes));
    }
}
