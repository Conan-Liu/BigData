package com.conan.bigdata.spark.example;

import org.apache.log4j.Logger;

/**
 * Created by Conan on 2019/5/2.
 */
public class LoggerGenerator {

    private static Logger LOG=Logger.getLogger(LoggerGenerator.class);

    public static void main(String[] args) throws InterruptedException {
        int index=0;

        while(true){
            Thread.sleep(1000);
            // 太长了， flume控制台不好显示完整， 不便于测试
            LOG.info("Value is : "+index);
            index++;
        }
    }
}