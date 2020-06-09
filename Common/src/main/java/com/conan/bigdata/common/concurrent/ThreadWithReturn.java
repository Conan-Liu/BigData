package com.conan.bigdata.common.concurrent;

import java.util.concurrent.TimeUnit;

/**
 * 实际开发过程中，我们有时候会遇到主线程调用子线程，要等待子线程返回的结果来进行下一步动作的业务
 * 主线程等待
 * Join方法等待
 * 通过 Callable 接口实现：通过FutureTask Or 线程池获取线程返回值
 */
public class ThreadWithReturn {

    private static class Simple implements Runnable {
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

    private static int sum = 0;
    private static volatile boolean isTrue = false;

    private static class WhileBreak implements Runnable {

        @Override
        public void run() {
            for (int i = 0; i < 1000; i++) {
                sum++;
                if (sum >= 100) {
                    isTrue = true;
                    break;
                }
            }
        }
    }

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
         * {@link ThreadPoolExecutorExp}
         */
        System.out.println("value : " + s.getValue());


        Thread tt=new Thread(new WhileBreak(),"tt");
        tt.start();
        // 利用线程标记位来获取返回值
        // while (!isTrue) ;
        tt.join();
        System.out.println("sum = " + sum);
    }
}