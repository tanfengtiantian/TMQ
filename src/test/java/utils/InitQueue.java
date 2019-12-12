package utils;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class InitQueue {
    // 队头
    private int front = 0;
    // 队尾
    private int rear = 0;

    private final int[] queue ;

    public InitQueue(int capacity){
        queue = new int[capacity + 1];
    }
    public boolean enQueue(int v){
        //队列满
        if(((rear+1) % queue.length)==front)
            return false;

        this.queue[rear]=v;
        rear=(rear+1)%queue.length;
        return false;
    }

    public int deQueue(){
        if(front != rear){
            int v = queue[front];
            front = (front + 1) % queue.length;
            System.out.println(v);
            return v;
        }
        return -1;
    }


    public static void main(String[] args) throws InterruptedException {

        InitQueue init =new InitQueue(4);
        init.enQueue(1);
        init.enQueue(2);
        init.enQueue(3);
        init.enQueue(4);

        init.deQueue();//0
        init.deQueue();//1
        init.deQueue();//2
        init.deQueue();//3


        ArrayBlockingQueue<Integer> arrayBlockingQueuequeue = new ArrayBlockingQueue<>(10);
        arrayBlockingQueuequeue.offer(1);

        LinkedList<Integer> list = new LinkedList<Integer>();
        list.add(1);

        LinkedBlockingQueue<Integer> linkedBlockingQueue = new LinkedBlockingQueue<Integer>();
        linkedBlockingQueue.offer(1);
        linkedBlockingQueue.take();

    }
}
