package javakafka.io.kafka.message;

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class MappedByteBufferTest {

    static FileChannel fileChannel;
    static MappedByteBuffer mappedByteBuffer;
    static String path="E:\\tmp\\Test";
    static long address;
    static long wrotePosition;
    static int _8B=8;
    public static void main(String[] args) throws IOException {

        File dirFile = new File(path);
        if (!dirFile.exists()) {
            dirFile.mkdirs();
        }
        File file = new File(path + "/index.polar");
        if (!file.exists()) {
            file.createNewFile();
        }
        fileChannel = new RandomAccessFile(file, "rw").getChannel();
        for (int i = 0; i < 64000; i++){
            write(long2bytes(i));
        }

        read();
    }
    public static void read(){
        mappedByteBuffer.flip();
        for (int i = 0; i <mappedByteBuffer.limit() ; i++) {
            System.out.println(mappedByteBuffer.get());
        }
    }

    public static void write(byte[] key) {

        if (mappedByteBuffer == null) {
            try {
                //500k
                mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, _8B * 64000);
            } catch (IOException e) {

            }
            address = ((DirectBuffer) mappedByteBuffer).address();
            wrotePosition = 0;
        }
        if (wrotePosition >= mappedByteBuffer.limit() - _8B) {
            try {
                mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, _8B * 203000);
            } catch (IOException e) {

            }
            address = ((DirectBuffer) mappedByteBuffer).address();
        }
        UNSAFE.copyMemory(key, 16, null, address + wrotePosition, _8B);
        wrotePosition += _8B;
    }

    public static Unsafe UNSAFE;

    public static byte[] long2bytes(long values) {
        byte[] buffer = new byte[8];
        for (int i = 0; i < 8; ++i) {
            int offset = 64 - (i + 1) * 8;
            buffer[i] = (byte) ((values >> offset) & 0xff);
        }
        return buffer;
    }

    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
