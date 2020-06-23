package com.conan.bigdata.common.concurrent.thread;

import com.conan.bigdata.common.util.Tools;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 使用阻塞队列实现的生产者消费者
 * 主要是使用BlockingQueue的put，take方法，这是阻塞方法
 * put满了会等待，没有元素被take时会等待
 */
public class BlockQueueProducerConsumer {

    private static BlockingQueue<String> queue = new LinkedBlockingQueue<>(1);

    public static void main(String[] args) {
        Thread producer = new Thread(new Runnable() {
            @Override
            public void run() {
                int i = 0;
                String msg;
                while (true) {
                    try {
                        msg = "msg-" + i;
                        System.out.println("producer:" + msg);
                        queue.put(msg);
                        Tools.randomSleep(1000, false);
                        i++;
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }, "producer");
        producer.start();

        Thread consumer = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        System.out.println("consumer:" + queue.take());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }, "consumer");
        consumer.start();
    }
}
