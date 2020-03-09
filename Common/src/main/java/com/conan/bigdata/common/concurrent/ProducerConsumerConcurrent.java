package com.conan.bigdata.common.concurrent;

/**
 * 线程互斥同步
 * 互斥同步最主要的问题就是进行线程阻塞和唤醒所带来的性能问题，因此这种同步也被称为阻塞同步。而且加锁方式属于悲观锁
 * 一个生产者，一个消费者，并且让他们使用同一个共享资源，并且我们期望的是生产者生产一条放到共享资源中，消费者就会对应地消费一条
 * 为了实现交替生产消费，需要使用wait()和notify()来限制生产者线程和消费者线程，互相唤醒
 */

public class ProducerConsumerConcurrent {

    /**
     * 成员内部类
     * 内部类是依附外部类而存在的， 也就是说要创建成员内部类的对象，前提是创建一个外部类的对象，创建成员内部类的方式如下
     * new ProducerConsumerConcurrent().new Producer();
     */
    /**
     * 静态内部类也是定义在另一个类里面的类，只不过在类前加上了static。静态内部类是不需要依赖于外部类的，与静态成员变量类似
     * 外部创建该静态类时可以如下创建：
     * ProducerConsumerConcurrent.Producer mi = new ProducerConsumerConcurrent.Producer();
     * 也可以直接创建
     * Producer mi = new Producer();
     */
    private static class Producer implements Runnable {

        private ProducerConsumerConcurrent producerConsumerConcurrent;

        private Producer(ProducerConsumerConcurrent producerConsumerConcurrent) {
            this.producerConsumerConcurrent = producerConsumerConcurrent;
        }

        @Override
        public void run() {
            for (int i = 0; i < 50; i++) {
                if (i % 2 == 0) {
                    this.producerConsumerConcurrent.push("张三", 1);
                } else {
                    this.producerConsumerConcurrent.push("李四", 2);
                }
            }
        }
    }

    private static class Consumer implements Runnable {

        private ProducerConsumerConcurrent producerConsumerConcurrent;

        private Consumer(ProducerConsumerConcurrent producerConsumerConcurrent) {
            this.producerConsumerConcurrent = producerConsumerConcurrent;
        }

        @Override
        public void run() {
            for (int i = 0; i < 50; i++) {
                this.producerConsumerConcurrent.popup();
            }
        }
    }

    private String name;
    private int gender;
    // 增加一个标志位，表示是否有数据
    private boolean isEmpty = true;

    // 生产数据 synchronized 实现生产过程同步，即生产的时候原子性操作，也就是这个对象一定要等到这个方法执行完才会释放
    // 在此期间，别的线程不可以访问该对象，不会出现脏数据
    public synchronized void push(String name, int gender) {
        try {
            // 调用Object的wait和notify实现生产者生产一条数据就休眠，唤醒消费者线程去消费
            while (!isEmpty) {
                // 对象锁，调用 this 方法，表示释放该对象的锁，本线程开始休眠
                this.wait();
            }
            this.name = name;
            Thread.sleep(100);
            this.gender = gender;
            isEmpty = false;
            // 对象锁，调用该方法，唤醒其它基于该对象，并处于休眠状态的线程，这里表示消费者线程
            // 这里只有一个消费者线程，如果是生产环境，可能会有多个线程，建议使用 notifyAll
            this.notify();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 消费数据
    public synchronized void popup() {
        try {
            // // 调用Object的wait和notify实现消费者消费一条数据就休眠，唤醒生产者线程去生产
            while (isEmpty) {
                // 如果数据为空，表示还没有数据消费，则该消费者线程休眠，释放对象锁，
                this.wait();
            }
            System.out.println(this.name + " - " + this.gender);
            Thread.sleep(100);
            isEmpty = true;
            // 对象锁，调用该方法，唤醒其它基于该对象，并处于休眠状态的线程，这里表示生产者线程
            this.notify();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        // 这是对象锁，当对象调用​一个synchronized方法时，其他同步方法需要等待其执行结束并释放锁之后才能执行，前提是同一个对象
        // 生产者消费者处理同一个对象
        ProducerConsumerConcurrent producerConsumerConcurrent = new ProducerConsumerConcurrent();
        new Thread(new Producer(producerConsumerConcurrent)).start();
        new Thread(new Consumer(producerConsumerConcurrent)).start();
    }
}