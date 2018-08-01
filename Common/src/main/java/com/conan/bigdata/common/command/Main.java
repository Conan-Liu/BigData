package com.conan.bigdata.common.command;

/**
 * Created by Administrator on 2018/7/20.
 */
public class Main {
    public static void main(String[] args) {
        System.out.println("这是代码里面产生的日志");
        int exitCode = 0;
        try {
            for (int i = 0; i < 10; i++) {
                System.out.println("代码里产生的日志： ============= " + i);
                Thread.sleep(2000);
            }
        } catch (Throwable e) {
            exitCode = 1;
            System.exit(exitCode);
        }
        System.exit(exitCode);
    }
}