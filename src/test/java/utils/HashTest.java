package utils;

import java.util.concurrent.locks.ReentrantLock;

public class HashTest {

    private static final int MAXIMUM_CAPACITY = 1 << 30;

    private static ReentrantLock lock =new ReentrantLock(false);
    private static final int tableSizeFor(int c) {
        int n = c - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
    }

     private static int count = 0;
     public static void main(String args[]) {
        /*
        String a="谭枫tanfeng123";
         lock.unlock();
         char[] b= a.toCharArray();
         a.equals(b);
         for (int i = 0; i < b.length; i++) {
             System.out.println(""+b[i]);
         }
        //System.out.println(tableSizeFor(17));
        */
         for (int i = 0; i < 10000; i++) {
             Thread t1 = new Thread(new Runnable() {
                 @Override
                 public void run() {
                     lock.lock();
                     try {
                         count = count + 1;
                         //Thread.sleep(1);
                     } catch (Exception e) {
                         e.printStackTrace();
                     }finally{
                         lock.unlock();

                     }
                     System.out.println(count);

                 }
             });
             t1.start();
        }
     }
}
