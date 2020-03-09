package com.conan.bigdata.flume.log4j;

import org.apache.log4j.Logger;

import java.util.Date;

public class Log4JToFlume {
    private static Logger LOG = Logger.getLogger(Log4JToFlume.class);

    public static void main(String[] args) throws InterruptedException {
        LOG.debug("This is debug message.");
        LOG.info("This is info message.");
        LOG.error("This is error message.");

        while (true) {
            LOG.info("测试数据 : " + new Date().getTime());
            Thread.sleep(500);
        }
    }
}
