package com.conan.bigdata.common.concurrent.thread;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 重点讲解条件变量{@link java.util.concurrent.locks.Condition}
 * 存款取款
 * 定义存款和取款两个线程，主类作为用户账户，存款和取款就可以处理为这两个线程操作用户账户
 */
public class ConditionExp {
    // 普通的银行账户，不可以透支
    private String oid;   // 账户人
    private int cash;   // 账户金额
    private Lock lock = new ReentrantLock();
    private Condition saveCondition = lock.newCondition();
    private Condition drawCondition = lock.newCondition();

    public ConditionExp(String oid, int cash) {
        this.oid = oid;
        this.cash = cash;
    }

    public static void main(String[] args) {
        // 创建同一个账户，大家并发存钱取钱操作
        ConditionExp account=new ConditionExp("123456789",10000);
        ExecutorService pool= Executors.newFixedThreadPool(3);
        Runnable t1 = new SaveRunnable("张三", account, 1000);
        Runnable t2 = new SaveRunnable("李四", account, 1000);
        Runnable t3 = new DrawRunnable("王五", account, 12600);
        Runnable t4 = new SaveRunnable("老张", account, 600);
        Runnable t5 = new DrawRunnable("老牛", account, 1300);
        Runnable t6 = new DrawRunnable("胖子", account, 800);
        Runnable t7 = new SaveRunnable("测试", account, 2100);

        // 执行各个线程
        pool.execute(t1);
        pool.execute(t2);
        pool.execute(t3);
        pool.execute(t4);
        pool.execute(t5);
        pool.execute(t6);
        pool.execute(t7);
        // 关闭线程池
        pool.shutdown();
    }

    // 存款
    public void saving(int x, String name) {
        lock.lock();
        try {
            this.cash += x;
            System.out.println(name + "存款" + x + "，当前余额为" + cash);
            drawCondition.signal();
        } finally {
            lock.unlock();
        }
    }

    public void drawing(int x, String name) {
        lock.lock();
        try {
            while (this.cash < x) {
                System.out.println("余额不够，阻塞");
                drawCondition.await();
            }
            this.cash -= x;
            System.out.println(name + "取款" + x + "，当前余额为" + cash);
            saveCondition.signalAll();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }


    // 存款线程
    private static class SaveRunnable implements Runnable {

        private String name;   // 存款人
        private ConditionExp account;   // 存款账户
        private int x;   // 存款金额

        SaveRunnable(String name, ConditionExp account, int x) {
            this.name = name;
            this.account = account;
            this.x = x;
        }

        @Override
        public void run() {
            account.saving(x, name);
        }
    }

    // 取款线程
    private static class DrawRunnable implements Runnable {

        private String name;   // 存款人
        private ConditionExp account;   // 存款账户
        private int x;   // 存款金额

        DrawRunnable(String name, ConditionExp account, int x) {
            this.name = name;
            this.account = account;
            this.x = x;
        }

        @Override
        public void run() {
            account.drawing(x, name);
        }
    }

}
