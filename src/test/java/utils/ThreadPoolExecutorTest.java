package utils;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadPoolExecutorTest implements Runnable{

    //记录线程池状态和线程数量（总共32位，前三位表示线程池状态，后29位表示线程数量）
    private final AtomicInteger ctl = new AtomicInteger(ctlOf(RUNNING, 0));
    //线程数量统计位数29  Integer.SIZE=32
    private static final int COUNT_BITS = Integer.SIZE - 3;
    //容量 000 11111111111111111111111111111
    private static final int CAPACITY   = (1 << COUNT_BITS) - 1;

    //运行中 111 00000000000000000000000000000
    private static final int RUNNING    = -1 << COUNT_BITS;
    //关闭 000 00000000000000000000000000000
    private static final int SHUTDOWN   =  0 << COUNT_BITS;
    //停止 001 00000000000000000000000000000
    private static final int STOP       =  1 << COUNT_BITS;
    //整理 010 00000000000000000000000000000
    private static final int TIDYING    =  2 << COUNT_BITS;
    //终止 011 00000000000000000000000000000
    private static final int TERMINATED =  3 << COUNT_BITS;

    //获取运行状态（获取前3位）
    //c & 高3位为1，低29位为0的~CAPACITY，用于获取高3位保存的线程池状态
    private static int runStateOf(int c)     { return c & ~CAPACITY; }
    //获取线程个数（获取后29位）
    //c & 高3位为0，低29位为1的CAPACITY，用于获取低29位的线程数量
    private static int workerCountOf(int c)  { return c & CAPACITY; }
    //或运算符   0|1  1
    //参数rs表示runState，参数wc表示workerCount，即根据runState和workerCount打包合并成ctl
    private static int ctlOf(int rs, int wc) { return rs | wc; }


     public static void main(String args[]){
         //ThreadPoolExecutor threadPoolExecutor =new ThreadPoolExecutor()
         System.out.println("%="+3%4);
         System.out.println("COUNT_BITS="+COUNT_BITS);
         System.out.println("CAPACITY="+CAPACITY);
         System.out.println("RUNNING="+RUNNING);
         System.out.println("SHUTDOWN="+SHUTDOWN);
         System.out.println("STOP="+STOP);
         System.out.println("TIDYING="+TIDYING);
         System.out.println("TERMINATED="+TERMINATED);
         System.out.println("TERMINATED="+TERMINATED);
         new ThreadPoolExecutorTest().advanceRunState(STOP);
         //new ThreadPoolExecutor(4,10,10000, TimeUnit.SECONDS, new ArrayBlockingQueue<>(10));

         //BlockingQueue<Runnable> workQueue =new ArrayBlockingQueue<>(2);

         //System.out.println("offer="+workQueue.offer(new ThreadPoolExecutorTest()));
         //System.out.println("offer="+workQueue.offer(new ThreadPoolExecutorTest()));
         //System.out.println("offer="+workQueue.offer(new ThreadPoolExecutorTest()));
         //System.out.println("workerCountOf="+workerCountOf(536870913));

         //System.out.println("runStateOf="+runStateOf(1));
         //int c = ctl.get();
         //ctl.compareAndSet(c, ctlOf(STOP, workerCountOf(c)))
         //ctlOf(TIDYING, 0)
     }

    /**
     * Attempts to CAS-increment the workerCount field of ctl.
     */
    private boolean compareAndIncrementWorkerCount(int expect) {
        return ctl.compareAndSet(expect, expect + 1);
    }
    /**
     * Attempts to CAS-decrement the workerCount field of ctl.
     */
    private boolean compareAndDecrementWorkerCount(int expect) {
        return ctl.compareAndSet(expect, expect - 1);
    }

    /**
     * Decrements the workerCount field of ctl. This is called only on
     * abrupt termination of a thread (see processWorkerExit). Other
     * decrements are performed within getTask.
     */
    private void decrementWorkerCount() {
        do {} while (! compareAndDecrementWorkerCount(ctl.get()));
    }


    private static boolean runStateAtLeast(int c, int s) {
        return c >= s;
    }

    /**
     * Transitions runState to given target, or leaves it alone if
     * already at least the given target.
     *
     * @param targetState the desired state, either SHUTDOWN or STOP
     *        (but not TIDYING or TERMINATED -- use tryTerminate for that)
     */
    //改变状态
    public void advanceRunState(int targetState) {
        for (;;) {
            int c = ctl.get();
            System.out.println("runStateOf="+runStateOf(c));
            System.out.println("workerCountOf="+workerCountOf(c));
            if (runStateAtLeast(c, targetState) ||
                    ctl.compareAndSet(c, ctlOf(targetState, workerCountOf(c))))
                break;
        }
        int c = ctl.get();
        //System.out.println("c="+c);
        System.out.println("compareAndIncrementWorkerCount="+compareAndIncrementWorkerCount(c));
       // System.out.println("c="+c);
        System.out.println("runStateOf="+runStateOf(ctl.get()));
        System.out.println("workerCountOf="+workerCountOf(ctl.get()));
    }

    @Override
    public void run() {

    }
}
