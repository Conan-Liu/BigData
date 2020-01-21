package com.conan.bigdata.common.concurrent;

import java.util.concurrent.TimeUnit;

/**
 * 实际开发过程中，我们有时候会遇到主线程调用子线程，要等待子线程返回的结果来进行下一步动作的业务
 * 主线程等待
 * Join方法等待
 * 通过 Callable 接口实现：通过FutureTask Or 线程池获取线程返回值
 */
class Simple implements Runnable {
    private String value;

    @Override
    public void run() {
        try {
            value = "this is example";
            TimeUnit.SECONDS.sleep(1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getValue() {
        return this.value;
    }
}

public class ThreadWithReturn {
    public static void main(String[] args) throws Exception {
        Simple s = new Simple();
        Thread t = new Thread(s);
        t.start();
        // 主线程等待
        while (s.getValue() == null) {
            TimeUnit.SECONDS.sleep(1);
        }

        // Join方法等待
        t.join();

        /**
         * Callable 接口参考
         * {@link CallableExecutor}
         */
        System.out.println("value : " + s.getValue());
    }
}