package utils;

import io.kafka.utils.NamedThreadFactory;
import io.kafka.utils.timer.HashedWheelTimer;
import io.kafka.utils.timer.Timeout;
import io.kafka.utils.timer.Timer;

import java.util.concurrent.TimeUnit;

public class HashedWheelTimerTest {
    // 最大事务超时时间个数，默认3万个
    private static int maxTxTimeoutTimerCapacity = 3000;

    public static void main(String args[]) throws InterruptedException {
          Timer txTimeoutTimer =
                 new HashedWheelTimer(new NamedThreadFactory("Tx-Timeout-Timer"), 500, TimeUnit.MILLISECONDS, 512,
                         maxTxTimeoutTimerCapacity);
         final long st = System.currentTimeMillis();
         Timeout timeoutRef = txTimeoutTimer.newTimeout(timeout -> {
              System.out.println("timeoutRef!!");
             long et = System.currentTimeMillis();
             System.out.println("解析消息耗时: " + (et - st) + " ms");
             Thread.sleep(5000);
             // 没有prepared的到期事务要回滚
         }, 1, TimeUnit.SECONDS);

         /*
         Timeout timeoutRef2 = txTimeoutTimer.newTimeout(timeout -> {
             System.out.println("timeoutRef2!!");
             long et = System.currentTimeMillis();
             System.out.println("解析消息耗时: " + (et - st) + " ms");
             // 没有prepared的到期事务要回滚
         }, 5, TimeUnit.SECONDS);


         Timeout timeoutRef3 = txTimeoutTimer.newTimeout(timeout -> {
             System.out.println("timeoutRef3!!");
             long et = System.currentTimeMillis();
             System.out.println("解析消息耗时: " + (et - st) + " ms");
             // 没有prepared的到期事务要回滚
         }, 3, TimeUnit.SECONDS);


         Timeout timeoutRef4 = txTimeoutTimer.newTimeout(timeout -> {
             System.out.println("timeoutRef4!!");
             long et = System.currentTimeMillis();
             System.out.println("解析消息耗时: " + (et - st) + " ms");
             // 没有prepared的到期事务要回滚
         }, 10, TimeUnit.SECONDS);

         System.out.println("start!!");
         while (true){
             txTimeoutTimer.newTimeout(timeout -> {
                 System.out.println("true!!");
             }, 10, TimeUnit.SECONDS);
         }*/
         //Thread.sleep(1000*10);
     }
}
