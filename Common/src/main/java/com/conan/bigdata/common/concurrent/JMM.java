package com.conan.bigdata.common.concurrent;

/**
 * Java 内存模型， Java 内存结构是两个不同的概念
 * JMM Java Memory Model  来屏蔽掉各层硬件和操作系统的内存访问差异，以实现让 Java 程序在各种平台下都能达到一致的内存访问效果
 */
public class JMM {

    // volatile 内存可见性， 如果没有这个，这个变量的改动，其它线程将感应不到，也就是说thread线程将死循环
    // 但是不保证原子性，++ -- 都不是原子操作，这里假设刚开始 count = 0
    // count++ 执行了变成 1，但是数据还没写入主内存，count-- 也执行了变成 -1，接着大家写入主内存，发现要不是 1，要不就是-1，不是我们想要的 0
    // 无论是哪个值，都已经不正确了，后面的累加同理，最后数据错误
    private volatile static boolean isOver = false;

    private volatile static long count = 0L;
    private static final int NUM = 10000;

    public static void main(String[] args) throws Exception {
        Thread thread = new Thread(() -> {
            while (!isOver) {

            }
            System.out.println("线程已感知到 isOver 置为 true， 线程正常返回");

            for (int i = 0; i < NUM; i++) {
                count--;
            }
        });
        thread.start();
        isOver = true;
        System.out.println("isOver 已置为 true");

        for (int i = 0; i < NUM; i++) {
            count++;
        }
        while (thread.isAlive()) {

        }
        System.out.println("count 最后的值为: " + count);

        // 单例演示
        JMM singleton=getInstance2();
        System.out.println(singleton);
    }


    // 正确的singleton单例模式实现
    private volatile static JMM _instance = null;

    // 由于内存可见性和原子性的特点，如果不是原子性操作，内存可见性可能会存在时间上的误差
    // 假设一个线程进入这个方法，另一个也进入这个方法，但是两个线程都发现 _instance = null
    // 都会创建一个实例，就不是单例模式了
    // 所以只需要加上 synchronized 保证该方法原子性执行即可，但是效率低
    public synchronized static JMM getInstance1() {
        if (_instance == null) {
            _instance = new JMM();
        }
        return _instance;
    }

    // 或者二次检查
    public static JMM getInstance2() {
        if (_instance == null) {
            synchronized (JMM.class) {
                if (_instance == null) {
                    _instance = new JMM();
                }
            }
        }
        return _instance;
    }
}