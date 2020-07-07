package com.conan.bigdata.common.concurrent.thread;

/**
 * 多线程顺序打印的经典问题
 */
public class SortedPrintExp {

    public static void main(String[] args) {
        printABCMethod();
    }

    /**
     * 开启三个线程，三个线程ID分别为A，B，C
     * 每个线程将自己的ID在屏幕上打印10遍，要求输出结果显示为
     * ABCABCA...
     */
    private static class PrintABC implements Runnable {

        // 定义线程ID，该线程ID，只是代码中定义的ID，不是运行时操作系统分配的ID
        private Integer threadId;
        // 加锁的共享变量
        private volatile Integer obj;

        private PrintABC(Integer threadId, Integer obj) {
            this.threadId = threadId;
            this.obj = obj;
        }

        @Override
        public void run() {
            synchronized (obj) {
                try {
                    for (int i = 0; i < 10; i++) {
                        if (obj == threadId) {
                            System.out.println(Thread.currentThread().getName());
                        }
                        obj++;
                        obj.notifyAll();
                        obj.wait();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void printABCMethod() {
        // 1 A   2 B   3 C
        Integer obj = 1;
        for (int i = 0; i < 3; i++) {
            // 线程的命名只能从Thread的构造方法传参数
            Thread t = new Thread(new PrintABC(i + 1, obj), (char) (i + 65) + "");
            t.start();
        }
    }
}
