package utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class WriteTest {
    static ExecutorService executor = Executors.newFixedThreadPool(64);
    static AtomicLong position = new AtomicLong(0);
    public static void main(String[] args) {

        /*
        //并发写（达到随机写的描述）
        for(int i=0;i<1024;i++) {
            final int index = i;
            executor.execute(() -> {
                System.out.println(position.getAndAdd(1));
            });
        }*/

        //加锁保证了顺序写
        for(int i=0;i<1024;i++){
            final int index = i;
            executor.execute(()->{
                write(new byte[4*1024]);
            });
        }
    }

    public static synchronized void write(byte[] data){
        System.out.println(position.getAndAdd(1));
    }
}
