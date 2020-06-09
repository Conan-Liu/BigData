package com.conan.bigdata.common.concurrent;

import com.conan.bigdata.common.util.Tools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 线程工厂，为线程提供统一管理
 */
public class ThreadFactoryExp implements ThreadFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ThreadFactoryExp.class);
    private final Thread.UncaughtExceptionHandler ueh;
    private final AtomicInteger threadNum = new AtomicInteger(1);
    private final String prefixName;

    public ThreadFactoryExp() {
        this.ueh = new LoggingUncaughtExceptionHandler();
        this.prefixName = "thread";
    }

    public ThreadFactoryExp(String prefixName) {
        //  如果不是静态内部类，那么这里会报错，涉及到父子类变量初始化顺序的问题
        this(new LoggingUncaughtExceptionHandler(), prefixName);
    }

    public ThreadFactoryExp(UncaughtExceptionHandler ueh) {
        this(ueh, "thread");
    }

    public ThreadFactoryExp(UncaughtExceptionHandler ueh, String prefixName) {
        this.ueh = ueh;
        this.prefixName = prefixName;
    }

    /**
     * 线程工厂用来创建线程的方法，需要自己实现
     *
     * 例子：该功能为每个线程统一配置通用属性
     */
    @Override
    public Thread newThread(Runnable r) {
        Thread t = doMakeThread(r);
        t.setUncaughtExceptionHandler(this.ueh);
        t.setName(this.prefixName + "-" + this.threadNum.getAndIncrement());
        if (t.isDaemon()) {
            t.setDaemon(false);
        }
        if (t.getPriority() != Thread.NORM_PRIORITY) {
            t.setPriority(Thread.NORM_PRIORITY);
        }
        return t;
    }

    protected Thread doMakeThread(final Runnable r) {
        // 初始化一个线程，并重写toString方法
        return new Thread(r) {
            @Override
            public String toString() {
                ThreadGroup group = getThreadGroup();
                String groupName = group == null ? "" : group.getName();
                return String.format("TTT[%s,%d,%s]@%s", getName(), getId(), groupName, hashCode());
            }
        };
    }

    /**
     * 写一个默认的UncaughtExceptionHandler
     */
    private static class LoggingUncaughtExceptionHandler implements UncaughtExceptionHandler {

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            LOG.warn("Thread {} terminated:", t, e);
        }
    }


    public static void main(String[] args) {
        final ThreadPoolExecutor executor = new ThreadPoolExecutor(4, 8, 4, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(8), new ThreadPoolExecutor.CallerRunsPolicy());
        // 线程工厂调用时，就需要set一下，其它操作不变
        final ThreadFactory tf = new ThreadFactoryExp("worker");
        executor.setThreadFactory(tf);
        final Random random = new Random();
        for (int i = 0; i < 10; i++) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    Thread t=Thread.currentThread();
                    LOG.info("running...{}, toString={}",t.getName(),t);
                    // 模拟随机性抛异常
                    while (true) {
                        if (random.nextInt(100) < 30) {
                            throw new RuntimeException("test");
                        }
                        Tools.randomSleep(1000, false);
                    }
                }
            });
        }
    }
}
