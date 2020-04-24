package utils;

import java.nio.ByteBuffer;

/**
 * @author tf
 * @version 创建时间：2019年1月10日 下午5:08:03
 * @ClassName 类名称
 */
public class ByteBufferTest
{
 
    public static void main(String args[]) {
        //byte a = (byte) 255;
        //System.out.println(a);
        //allocateTest();
        //flipFunTest();
        //clearFunTest();
        //rewindFunTest();
        //markFunTest();
        compactFunTest();
    }
 
    /**
     * 测试ByteBuffer.allocate(int capacity)方法
     * 作用：分配指定capacity大小的缓冲区，默认存的数据为0
     * 例如：分配指定15字节的缓冲区，那么缓冲区中默认存的是15个0
     */
    public static void allocateTest(){
        ByteBuffer buffer = ByteBuffer.allocate(15); //15字节缓冲区，注意：分配的缓冲区，默认存的是15个0
        System.out.println("【allocateTest】上限：" + buffer.limit() + " 容量：" + buffer.capacity() + " 位置：" + buffer.position());
        System.out.println("【allocateTest】刚分配(allocate)的buffer缓冲区中的数据：");
        for(int i = 0; i < buffer.limit(); i++){
              System.out.println(buffer.get());
        }
    }
 
    /**
     * 测试ByteBuffer.flip()方法
     * 作用：重置位置为0，上限值变成重置前的位置
     * 例如：有个容量为15字节的buffer，重置前的位置为10，那么重置后，位置 0，上限 10 ，容量 15
     */
    public static void flipFunTest(){
        ByteBuffer buffer = ByteBuffer.allocate(15); //15字节缓冲区，注意：分配的缓冲区，默认存的是15个0
        System.out.println("1-【flipFunTest】上限：" + buffer.limit() + " 容量：" + buffer.capacity() + " 位置：" + buffer.position());
        for(int i = 0; i < 10; i++){
            buffer.put((byte)i);
        }
        System.out.println("2-【flipFunTest】上限：" + buffer.limit() + " 容量：" + buffer.capacity() + " 位置：" + buffer.position());
        buffer.flip();
        System.out.println("3-【flipFunTest】上限：" + buffer.limit() + " 容量：" + buffer.capacity() + " 位置：" + buffer.position());
        for(int i = 0; i < 4; i++){
        	System.out.println(buffer.get());
        }
        System.out.println("4-【flipFunTest】上限：" + buffer.limit() + " 容量：" + buffer.capacity() + " 位置：" + buffer.position());
        buffer.flip();
        System.out.println("5-【flipFunTest】上限：" + buffer.limit() + " 容量：" + buffer.capacity() + " 位置：" + buffer.position());
        System.out.println("6-【flipFunTest】buffer缓冲区中的数据：");
        for(int i = 0; i < buffer.limit(); i++){
              System.out.println(buffer.get());
        }
    }
 
 
    /**
     * 测试ByteBuffer.clear()方法
     * 作用：位置重置为0，但是和flip()方法不同，上限值为重置前的缓冲区的容量大小
     */
    public static void clearFunTest(){
        ByteBuffer buffer = ByteBuffer.allocate(15); //15字节缓冲区，注意：分配的缓冲区，默认存的是15个0
        System.out.println("【clearFunTest】上限：" + buffer.limit() + " 容量：" + buffer.capacity() + " 位置：" + buffer.position());
        for(int i = 0; i < 10; i++){
            buffer.put((byte)i);
        }
        System.out.println("【clearFunTest】上限：" + buffer.limit() + " 容量：" + buffer.capacity() +  "位置：" + buffer.position());
        buffer.clear();
        System.out.println("【clearFunTest】上限：" + buffer.limit() + " 容量：" + buffer.capacity() +  "位置：" + buffer.position());
        for(int i = 0; i < 4; i++){
            buffer.get();
        }
        System.out.println("【clearFunTest】上限：" + buffer.limit() + " 容量：" + buffer.capacity() +  " 位置：" + buffer.position());
        buffer.clear();
        System.out.println("【clearFunTest】上限：" + buffer.limit() + " 容量：" + buffer.capacity() +  " 位置：" + buffer.position());
        System.out.println("【clearFunTest】buffer缓冲区中的数据：");
        for(int i = 0; i < buffer.limit(); i++){
              System.out.println(buffer.get());
        }
    }
 
 
    /**
     * 测试ByteBuffer.rewind()方法
     * 作用：位置重置为0，上限值不会改变，上限值还是重置前的值
     */
    public static void rewindFunTest(){
        ByteBuffer buffer = ByteBuffer.allocate(15); //15字节缓冲区，注意：分配的缓冲区，默认存的是15个0
        System.out.println("【rewindFunTest】上限：" + buffer.limit() + " 容量：" + buffer.capacity() + " 位置：" + buffer.position());
        for(int i = 0; i < 10; i++){
            buffer.put((byte)i);
        }
        System.out.println("【rewindFunTest】上限：" + buffer.limit() + " 容量：" + buffer.capacity() +  " 位置：" + buffer.position());
        buffer.rewind();
        System.out.println("【rewindFunTest】上限：" + buffer.limit() + " 容量：" + buffer.capacity() +  " 位置：" + buffer.position());
        for(int i = 0; i < 4; i++){
            buffer.get();
        }
        System.out.println("【rewindFunTest】上限：" + buffer.limit() + " 容量：" + buffer.capacity() +  " 位置：" + buffer.position());
        buffer.rewind();
        System.out.println("【rewindFunTest】上限：" + buffer.limit() + " 容量：" + buffer.capacity() +  " 位置：" + buffer.position());
        for(int i = 0; i < 4; i++){
            buffer.get();
        }
        buffer.flip();
        System.out.println("【rewindFunTest】上限：" + buffer.limit() + " 容量：" + buffer.capacity() +  " 位置：" + buffer.position());
        buffer.rewind();
        System.out.println("【rewindFunTest】上限：" + buffer.limit() + " 容量：" + buffer.capacity() +  " 位置：" + buffer.position());
        System.out.println("【rewindFunTest】buffer缓冲区中的数据：");
        for(int i = 0; i < buffer.limit(); i++){
              System.out.println(buffer.get());
        }
    }
 
    /**
     * 测试ByteBuffer.mark()方法
     * 作用：可以对当前位置进行标记，以后使用ByteBuffer.reset()方法
     *      可以使缓冲区的位置重置为以前标记的位置,从而可以返回到标记的位置
     *      对缓冲区的数据进行操作
     */
    public static void markFunTest(){
        ByteBuffer buffer = ByteBuffer.allocate(15); //15字节缓冲区，注意：分配的缓冲区，默认存的是15个0
        for(int i=0; i < 10; i++){
            buffer.put((byte)i);
        }
        buffer.clear();
        System.out.println("【markFunTest】buffer缓冲区中的数据：");
        for(int i = 0; i < buffer.limit(); i++){
              System.out.println(buffer.get());
        }
        buffer.clear();
        //使用mark方法进行标记，在位置为4处进行标记
        buffer.position(4);
        buffer.mark();
        System.out.println("标志的位置：" + buffer.position());
        System.out.println("【markFunTest】上限：" + buffer.limit() + " 容量：" + buffer.capacity() +  " 位置：" + buffer.position());
        buffer.reset(); //将此缓冲区的位置重置为以前标记的位置
        System.out.println("【markFunTest】上限：" + buffer.limit() + " 容量：" + buffer.capacity() +  " 位置：" + buffer.position());
        //判断在当前位置和上限(最大的位置)之间是否有元素
        boolean isFirst = true;
        while(buffer.hasRemaining()){
            if(isFirst){
                System.out.println("【markFunTest】当前位置和上限(最大的位置)之间的数据：");
                isFirst = false;
            }
            System.out.println(buffer.get());
        }
        //修改标志的位置的元素值
        buffer.reset();
        buffer.put((byte)100);
        buffer.clear();
        System.out.println("【markFunTest】buffer缓冲区中的数据：");
        for(int i = 0; i < buffer.limit(); i++){
              System.out.println(buffer.get());
        }
    }
    public static void compactFunTest() {
        ByteBuffer buffer = ByteBuffer.allocate(15); //15字节缓冲区，注意：分配的缓冲区，默认存的是15个0
        for(int i=0; i < 15; i++){
            buffer.put((byte)i);
        }
        System.out.println("put标志的位置：" + buffer.position());
        System.out.println("【compactFunTest】上限：" + buffer.limit() + " 容量：" + buffer.capacity() +  " 位置：" + buffer.position());
        System.out.println("【compactFunTest】上限：" + buffer.limit() + " 容量：" + buffer.capacity() +  " 位置：" + buffer.position());
        buffer.flip();
        System.out.println("flip标志的位置：" + buffer.position());
        System.out.println("【compactFunTest】上限：" + buffer.limit() + " 容量：" + buffer.capacity() +  " 位置：" + buffer.position());
        System.out.println("【compactFunTest】上限：" + buffer.limit() + " 容量：" + buffer.capacity() +  " 位置：" + buffer.position());
        for (int i = 0; i <10 ; i++) {
            System.out.println(buffer.get());
        }
        System.out.println("get标志的位置：" + buffer.position());
        System.out.println("【compactFunTest】上限：" + buffer.limit() + " 容量：" + buffer.capacity() +  " 位置：" + buffer.position());
        System.out.println("【compactFunTest】上限：" + buffer.limit() + " 容量：" + buffer.capacity() +  " 位置：" + buffer.position());
        buffer.compact();
        System.out.println("compact标志的位置：" + buffer.position());
        System.out.println("【compactFunTest】上限：" + buffer.limit() + " 容量：" + buffer.capacity() +  " 位置：" + buffer.position());
        System.out.println("【compactFunTest】上限：" + buffer.limit() + " 容量：" + buffer.capacity() +  " 位置：" + buffer.position());
        for (int i = 0; i <5 ; i++) {
            System.out.println(buffer.get());
        }

    }
 
}
