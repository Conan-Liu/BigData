package com.conan.bigdata.common.concurrent;

/**
 * 死锁演示
 */
public class DeadLock {

    private static class A implements Runnable {

        private Object objA;
        private Object objB;

        public A(Object objA, Object objB) {
            this.objA = objA;
            this.objB = objB;
        }

        @Override
        public void run() {
            synchronized (objA) {
                System.out.println("访问到objA");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                synchronized (objB) {
                    System.out.println("访问到objB");
                }
            }
        }
    }

    private static class B implements Runnable {

        private Object objA;
        private Object objB;

        public B(Object objA, Object objB) {
            this.objA = objA;
            this.objB = objB;
        }

        @Override
        public void run() {
            synchronized (objB) {
                System.out.println("访问到objB");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                synchronized (objA) {
                    System.out.println("访问到objA");
                }
            }
        }
    }

    // 声明两个资源
    private static final String RESOURCE_A = "A";
    private static final String RESOURCE_B = "B";

    public static void main(String[] args) {
        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                synchronized (RESOURCE_A) {
                    System.out.println("t1 get resource A");
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    synchronized (RESOURCE_B) {
                        System.out.println("t1 get resource B");
                    }
                }
            }
        }, "Thread-B");

        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                synchronized (RESOURCE_B) {
                    System.out.println("t1 get resource B");
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    synchronized (RESOURCE_A) {
                        System.out.println("t1 get resource A");
                    }
                }
            }
        }, "Thread-B");

        t1.start();
        t2.start();


        // 下面是另外一种死锁
        Object objA = new Object();
        Object objB = new Object();
        Thread tt1 = new Thread(new A(objA, objB), "tt1");
        Thread tt2 = new Thread(new B(objA, objB), "tt2");
        tt1.start();
        tt2.start();
    }
}