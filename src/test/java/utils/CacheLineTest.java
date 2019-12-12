package utils;

import sun.misc.Contended;

public final class CacheLineTest implements Runnable {
    public static int NUM_THREADS = 4;
    public final static long ITERATIONS = 500L * 1000L * 1000L;
    private final int arrayIndex;
    private static VolatileLong[] longs;
    private static VolatileCacheLong[] cachelongs;
    private final Volatile[] vol;
    public CacheLineTest(final int arrayIndex,final Volatile[] vol) {
        this.arrayIndex = arrayIndex;
        this.vol = vol;
    }

    public static void main(final String[] args) throws Exception {
        System.out.println("VolatileLong starting....");
        longs = new VolatileLong[NUM_THREADS];
        for (int i = 0; i < longs.length; i++) {
            longs[i] = new VolatileLong();
        }
        final long startVolatileLong = System.nanoTime();
        runTest(longs);
        System.out.println("duration = " + (System.nanoTime() - startVolatileLong));


        System.out.println("VolatileCacheLong starting....");
        cachelongs = new VolatileCacheLong[NUM_THREADS];
        for (int i = 0; i < cachelongs.length; i++) {
            cachelongs[i] = new VolatileCacheLong();
        }
        final long startVolatileCacheLong = System.nanoTime();
        runTest(cachelongs);
        System.out.println("duration = " + (System.nanoTime() - startVolatileCacheLong));


    }

    private static void runTest(Volatile[] vol) throws InterruptedException {
        Thread[] threads = new Thread[NUM_THREADS];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new CacheLineTest(i,vol));
        }
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }
    }

    public void run() {
        long i = ITERATIONS;
        while (0 != --i) {
            vol[arrayIndex].value = i;
        }
    }
    public static class Volatile{
        public volatile long value = 0L;
    }

    public final static class VolatileLong extends Volatile{

    }
    @Contended //注释用来测试
    public final static class VolatileCacheLong extends Volatile{
        public long p1, p2, p3, p4, p5, p6;

        public long getValue() {
            return p1 + p2 + p3+ p4 +p5+p6;
        }
    }

}
